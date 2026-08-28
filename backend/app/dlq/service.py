"""DLQService — operator-facing dead-letter queue over the dead_letter_message table.

Replaces the previous Redis XQ implementation. That version could not work: the
orchestrator swallows every step exception, so dramatiq acks each message as a
success and `dramatiq:default.XQ` is never even created. See DESIGN.md section 1.

See DESIGN.md section 4 for the replay guarantees enforced below.
"""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.models import (
    DeadLetterMessage,
    FailureClass,
    Pipeline,
    PipelineStatus,
    StepStatus,
    StepTag,
)
from app.observability.logging import logger

# POISON and NEEDS_HUMAN never replay: one reproduces the identical failure, the
# other needs a person to act first. UNKNOWN means we do not know whether the
# side effect landed, so it replays only on an explicit operator override.
NEVER_REPLAY = frozenset({FailureClass.POISON, FailureClass.NEEDS_HUMAN})
FORCE_REQUIRED = frozenset({FailureClass.UNKNOWN})


class ReplayRefused(Exception):
    """Replay is not permitted for this row in its current state."""


class DLQService:
    def list(
        self,
        session: Session,
        *,
        f_class: str | None = None,
        s_tag: str | None = None,
        p_id: uuid.UUID | None = None,
        resolved: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> list[DeadLetterMessage]:
        """Newest first. resolved=False returns only rows still needing action.

        f_class: failure class filter
        s_tag: step tag filter
        p_id: pipeline id filter
        """
        stmt = select(DeadLetterMessage)
        if not resolved:
            stmt = stmt.where(
                DeadLetterMessage.replayed_at.is_(None),
                DeadLetterMessage.discarded_at.is_(None),
            )
        if f_class is not None:
            stmt = stmt.where(DeadLetterMessage.failure_class == f_class)
        if s_tag is not None:
            stmt = stmt.where(DeadLetterMessage.step_tag == s_tag)
        if p_id is not None:
            stmt = stmt.where(DeadLetterMessage.pipeline_id == p_id)
        stmt = stmt.order_by(DeadLetterMessage.created_at.desc()).limit(limit).offset(offset)
        return list(session.execute(stmt).scalars())

    def get(self, session: Session, dlq_id: uuid.UUID) -> DeadLetterMessage | None:
        return session.get(DeadLetterMessage, dlq_id)

    def gauges(self, session: Session) -> dict[str, int]:
        """Unresolved row counts per failure class.

        Per class, not a single total: 40 POISON is a bug shipped an hour ago,
        40 TRANSIENT is a vendor outage. Same number, opposite response.
        """
        stmt = (
            select(DeadLetterMessage.failure_class, func.count())
            .where(
                DeadLetterMessage.replayed_at.is_(None),
                DeadLetterMessage.discarded_at.is_(None),
            )
            .group_by(DeadLetterMessage.failure_class)
        )
        counts = {c.value: 0 for c in FailureClass}
        for failure_class, count in session.execute(stmt):
            counts[failure_class] = count
        return counts

    def replay(self, session: Session, dlq_id: uuid.UUID, *, force: bool = False) -> dict[str, Any]:
        """Re-dispatch the failed step, enforcing the DESIGN.md section 4 guarantees."""
        from app.pipeline.orchestrator import set_step_state
        from app.pipeline.runner import dispatch_step

        row = session.get(DeadLetterMessage, dlq_id)
        if row is None:
            raise LookupError(f'dlq entry {dlq_id} not found')

        # Guarantee 1 — class gate.
        failure_class = FailureClass(row.failure_class)
        if failure_class in NEVER_REPLAY:
            raise ReplayRefused(
                f'{failure_class.value} is not replayable: '
                f'{"a replay reproduces the identical failure" if failure_class is FailureClass.POISON else "a person must act first"}. '
                f'Discard it instead.'
            )
        if failure_class in FORCE_REQUIRED and not force:
            raise ReplayRefused(
                f'{failure_class.value} may have already applied its side effect '
                f'({row.exception_type}). Replaying could duplicate it. '
                f'Re-send with force=true to override.'
            )

        # Guarantee 3 — the pipeline is still in the state this row describes.
        pipeline = session.get(Pipeline, row.pipeline_id)
        if pipeline is None:
            raise ReplayRefused(f'pipeline {row.pipeline_id} no longer exists')
        if pipeline.status != PipelineStatus.CRASHED.value:
            raise ReplayRefused(
                f'pipeline is {pipeline.status}, not CRASHED — it has already moved on. '
                f'Replaying now would re-run a step out of order.'
            )
        if pipeline.current_step != row.step_tag:
            raise ReplayRefused(
                f'pipeline is at {pipeline.current_step}, but this row is for '
                f'{row.step_tag}. Replaying a stale step risks duplicate side effects.'
            )

        # Guarantee 2 — claim the row atomically. Two operators hitting replay at
        # the same time: exactly one UPDATE matches, the other gets zero rows.
        claimed = session.execute(
            update(DeadLetterMessage)
            .where(
                DeadLetterMessage.id == dlq_id,
                DeadLetterMessage.replayed_at.is_(None),
                DeadLetterMessage.discarded_at.is_(None),
            )
            .values(replayed_at=func.now())
            .execution_options(synchronize_session=False)
        ).rowcount
        if claimed != 1:
            session.rollback()
            raise ReplayRefused('already replayed or discarded by another operator')

        # Guarantee 4 — reset pipeline state in the same transaction as the claim.
        # Otherwise the pipeline reads CRASHED while a worker is actively running
        # it, and every "is it stuck?" query gets the wrong answer.
        step_tag = StepTag(row.step_tag)
        set_step_state(pipeline, step_tag, status=StepStatus.ENQUEUED.value, exception=None)
        pipeline.status = PipelineStatus.RUNNING.value
        pipeline.exception = None
        pipeline.is_pipeline_level_crash = False
        session.commit()

        # Guarantee 5 — dispatch only after the commit. If this throws we have an
        # ENQUEUED pipeline with a replayed row, which the reconciler picks up.
        # Enqueuing before the commit would not be recoverable.
        dispatch_step(str(pipeline.id), step_tag, rc=row.attempts)

        logger.info(
            'dlq_replayed',
            dlq_id=str(dlq_id),
            pipeline_id=str(pipeline.id),
            step_tag=step_tag.value,
            failure_class=failure_class.value,
            forced=force,
        )
        return {
            'id': str(dlq_id),
            'pipeline_id': str(pipeline.id),
            'step_tag': step_tag.value,
            'failure_class': failure_class.value,
            'retry_count': row.attempts,
            'forced': force,
        }

    def discard(self, session: Session, dlq_id: uuid.UUID, *, reason: str) -> DeadLetterMessage:
        """Soft delete. The row stays queryable — operator actions leave a record."""
        row = session.get(DeadLetterMessage, dlq_id)
        if row is None:
            raise LookupError(f'dlq entry {dlq_id} not found')
        if row.discarded_at is not None:
            raise ReplayRefused('already discarded')
        if row.replayed_at is not None:
            raise ReplayRefused('already replayed; discard the row its replay produced')
        row.discarded_at = func.now()
        row.discard_reason = reason
        session.commit()
        logger.info('dlq_discarded', dlq_id=str(dlq_id), reason=reason)
        return row
