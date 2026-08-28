"""DLQ HTTP surface — operator-facing endpoints over Dramatiq's Redis XQ."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.dlq.metrics import current_gauges
from app.dlq.service import DLQService

router = APIRouter(prefix='/dlq', tags=['dlq'])

_service = DLQService()


@router.get('')
def list_dlq(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return {
        'items': _service.list_all(limit=limit, offset=offset),
        'gauges': current_gauges(),
    }


@router.get('/{message_id}')
def read_dlq(message_id: str) -> dict[str, Any]:
    entry = _service.get(message_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f'dlq entry {message_id} not found')
    return entry


@router.post('/{message_id}/replay')
def replay_dlq(message_id: str) -> dict[str, Any]:
    try:
        return _service.replay(message_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete('/{message_id}')
def discard_dlq(message_id: str) -> dict[str, Any]:
    removed = _service.discard(message_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f'dlq entry {message_id} not found')
    return {'status': 'discarded', 'message_id': message_id}
