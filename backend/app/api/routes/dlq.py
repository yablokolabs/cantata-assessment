"""DLQ HTTP surface — the operator's 3am entry point. See DESIGN.md section 5."""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db import get_session
from app.dlq.service import DLQService, ReplayRefused
from app.models import DeadLetterMessage

router = APIRouter(prefix='/dlq', tags=['dlq'])

_service = DLQService()


def _serialise(row: DeadLetterMessage) -> dict[str, Any]:
    return {
        'id': str(row.id),
        'pipelineId': str(row.pipeline_id),
        'stepTag': row.step_tag,
        'failureClass': row.failure_class,
        'exceptionType': row.exception_type,
        'attempts': row.attempts,
        'createdAt': row.created_at,
        'replayedAt': row.replayed_at,
        'replayOfId': str(row.replay_of_id) if row.replay_of_id else None,
        'discardedAt': row.discarded_at,
        'discardReason': row.discard_reason,
    }


@router.get('')
def list_dlq(
    failure_class: str | None = Query(default=None, alias='failureClass'),
    step_tag: str | None = Query(default=None, alias='stepTag'),
    pipeline_id: uuid.UUID | None = Query(default=None, alias='pipelineId'),
    resolved: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    rows = _service.list(
        session,
        failure_class=failure_class,
        step_tag=step_tag,
        pipeline_id=pipeline_id,
        resolved=resolved,
        limit=limit,
        offset=offset,
    )
    return {'items': [_serialise(r) for r in rows], 'gauges': _service.gauges(session)}


@router.get('/{dlq_id}')
def read_dlq(dlq_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    row = _service.get(session, dlq_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f'dlq entry {dlq_id} not found')
    return {**_serialise(row), 'traceback': row.traceback, 'payload': row.payload}


@router.post('/{dlq_id}/replay')
def replay_dlq(
    dlq_id: uuid.UUID,
    force: bool = Query(default=False),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return _service.replay(session, dlq_id, force=force)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReplayRefused as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.delete('/{dlq_id}')
def discard_dlq(
    dlq_id: uuid.UUID,
    reason: str = Body(embed=True, min_length=1),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        row = _service.discard(session, dlq_id, reason=reason)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReplayRefused as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {'status': 'discarded', **_serialise(row)}
