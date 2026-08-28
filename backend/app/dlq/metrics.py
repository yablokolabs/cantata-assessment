"""DLQ gauge metrics — unresolved dead-letter rows, per failure class.

Previously read `dramatiq:default.XQ` in Redis, a key that is never written
because the orchestrator swallows step exceptions before dramatiq can nack.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.dlq.service import DLQService

_service = DLQService()


def current_gauges(session: Session) -> dict[str, int]:
    return _service.gauges(session)
