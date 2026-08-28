"""DLQService — operator-facing dead-letter queue.

Reads dead-lettered messages from Dramatiq's Redis XQ and supports replay by
re-enqueueing onto the same queue with an incremented `retry_count` header.

See AGENTS.md § Dead-Letter Queue and ADR-002 for the design rationale.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import dramatiq
import redis

from app.config import settings
from app.observability.logging import logger

_DLQ_KEY_TEMPLATE = 'dramatiq:{queue}.XQ'


class DLQService:
    def __init__(self, queue: str = 'default') -> None:
        self.queue = queue
        self._redis: redis.Redis | None = None

    def _client(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    def list_all(self, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """Return dead-lettered messages from the XQ sorted set.

        Each entry includes the original dramatiq message body plus the
        archived-at timestamp (the XQ sorted-set score is the nack timestamp).
        """
        key = _DLQ_KEY_TEMPLATE.format(queue=self.queue)
        client = self._client()
        raw = client.zrange(key, offset, offset + limit - 1, withscores=True)
        out: list[dict[str, Any]] = []
        for raw_msg, score in raw:
            try:
                body = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue
            out.append(
                {
                    'message_id': body.get('message_id'),
                    'actor_name': body.get('actor_name'),
                    'args': body.get('args', []),
                    'kwargs': body.get('kwargs', {}),
                    'options': body.get('options', {}),
                    'archived_at': float(score),
                }
            )
        return out

    def get(self, message_id: str) -> dict[str, Any] | None:
        for entry in self.list_all(limit=1000):
            if entry.get('message_id') == message_id:
                return entry
        return None

    def replay(self, message_id: str) -> dict[str, Any]:
        """Re-enqueue a dead-lettered message.

        Looks up the message in XQ by message_id, increments the `retry_count`
        header in the message options, removes the XQ entry, and re-enqueues
        to the same actor on the same queue.
        """
        key = _DLQ_KEY_TEMPLATE.format(queue=self.queue)
        client = self._client()
        raw_entries = client.zrange(key, 0, -1)
        for raw_msg in raw_entries:
            try:
                body = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue
            if body.get('message_id') != message_id:
                continue

            options = dict(body.get('options', {}))
            retry_count = int(options.get('retry_count', 0)) + 1
            options['retry_count'] = retry_count

            broker = dramatiq.get_broker()
            actor = broker.get_actor(body['actor_name'])
            new_message = actor.message_with_options(
                args=tuple(body.get('args', [])),
                kwargs=dict(body.get('kwargs', {})),
                **options,
            )
            broker.enqueue(new_message)

            client.zrem(key, raw_msg)
            logger.info(
                'dlq_replayed',
                message_id=message_id,
                actor=body['actor_name'],
                retry_count=retry_count,
            )
            return {
                'message_id': new_message.message_id,
                'original_message_id': message_id,
                'retry_count': retry_count,
            }

        raise LookupError(f'dlq entry not found for message_id={message_id}')

    def discard(self, message_id: str) -> bool:
        key = _DLQ_KEY_TEMPLATE.format(queue=self.queue)
        client = self._client()
        for raw_msg in client.zrange(key, 0, -1):
            try:
                body = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue
            if body.get('message_id') == message_id:
                client.zrem(key, raw_msg)
                logger.info('dlq_discarded', message_id=message_id)
                return True
        return False


def _new_id() -> str:
    return str(uuid.uuid4())
