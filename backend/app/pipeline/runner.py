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

    max_retries=0 — failures land in Redis XQ immediately. There is no DLQ persistence layer:
    failed messages are visible via DLQGauge metrics but the message itself expires from XQ
    after Dramatiq's default TTL.
    """
    from app.pipeline.orchestrator import run_step_inline

    session = SessionLocal()
    try:
        run_step_inline(session, pipeline_id, StepTag(step_tag_value))
    finally:
        session.close()


def dispatch_step(pipeline_id: str, step_tag: StepTag) -> None:
    logger.info('dispatch_step', pipeline_id=pipeline_id, step_tag=step_tag.value)
    run_step.send(pipeline_id, step_tag.value)
