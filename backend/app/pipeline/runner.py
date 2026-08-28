from __future__ import annotations

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from app.config import settings
from app.db import SessionLocal
from app.models import StepTag
from app.observability.logging import logger

# The dramatiq CLI imports this module directly without first loading app.main,
# which means a default RedisBroker (pointing at localhost:6379) may already be
# installed before this module runs. Always install the broker against
# settings.redis_url so the worker connects to the configured Redis in compose.
dramatiq.set_broker(RedisBroker(url=settings.redis_url))


@dramatiq.actor(max_retries=0, time_limit=10 * 60 * 1000)
def run_step(pipeline_id: str, step_tag_value: str) -> None:
    """Dramatiq actor that runs a single pipeline step.

    Step-level failures are captured by the orchestrator into the
    `dead_letter_message` table, in the same transaction as the pipeline's
    CRASHED status. Exceptions do not escape to dramatiq — `run_step_inline`
    catches them — so broker retries and Redis XQ dead-lettering never occur,
    and `dramatiq:default.XQ` is never written.

    max_retries=0 is a pre-existing deviation from AGENTS.md §Retry Policy,
    left in place deliberately: restoring automatic retries before the
    side-effecting steps are idempotent would duplicate vendor jobs, invite
    emails and customer deliveries. See DESIGN.md section 6b.
    """
    from app.pipeline.orchestrator import run_step_inline

    session = SessionLocal()
    try:
        run_step_inline(session, pipeline_id, StepTag(step_tag_value))
    finally:
        session.close()


def dispatch_step(p_id: str, s_tag: StepTag, rc: int = 0) -> None:
    """
    p_id: pipeline id
    s_tag: step tag
    rc: retry count, carried as a message header on replay
    """
    logger.info('dispatch_step', pipeline_id=p_id, step_tag=s_tag.value, retry_count=rc)
    run_step.send_with_options(args=(p_id, s_tag.value), retry_count=rc)
