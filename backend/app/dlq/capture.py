"""Capture — record a failed pipeline step as a dead-letter row.

Called from the orchestrator's failure branches and deliberately does NOT commit:
the caller's commit makes the dead-letter row and the pipeline's CRASHED status
land in one transaction. This is the atomicity architecture.md claims for the
`after_nack` middleware but cannot deliver, because that path spans Redis and
Postgres. See DESIGN.md section 2.
"""
from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app.dlq.classify import SOFT_FAILURE_CLASS, classify
from app.models import DeadLetterMessage, Pipeline, StepTag
from app.observability.logging import logger

SOFT_FAILURE = 'StepSoftFailure'


def record_failure(
    session: Session,
    pipeline: Pipeline,
    step_tag: StepTag,
    *,
    exc: BaseException | None = None,
    traceback_text: str | None = None,
) -> DeadLetterMessage:
    """Add a dead-letter row to the session without committing.

    exc: the raised exception, or None for a step that returned success=False
         rather than raising. Drives the failure class.
    traceback_text: formatted traceback, or the step's error message on a soft
         failure.
    """
    failure_class = classify(exc) if exc is not None else SOFT_FAILURE_CLASS
    exception_type = type(exc).__name__ if exc is not None else SOFT_FAILURE

    row = DeadLetterMessage(
        id=uuid.uuid4(),
        pipeline_id=pipeline.id,
        step_tag=step_tag.value,
        failure_class=failure_class.value,
        exception_type=exception_type,
        traceback=traceback_text,
        payload={'actor': 'run_step', 'args': [str(pipeline.id), step_tag.value]},
    )
    session.add(row)
    logger.info(
        'dlq_captured',
        pipeline_id=str(pipeline.id),
        step_tag=step_tag.value,
        failure_class=failure_class.value,
        exception_type=exception_type,
    )
    return row
