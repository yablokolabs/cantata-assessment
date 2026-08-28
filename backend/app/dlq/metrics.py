"""DLQ gauge metrics.

Reads the size of dramatiq's Redis XQ sorted sets per queue. This is the only
piece of the "DLQ" that actually works today.
"""
from __future__ import annotations

import redis

from app.config import settings

_redis: redis.Redis | None = None


def _client() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


def current_gauges() -> dict[str, int]:
    """Return {queue_name: xq_size} for every dramatiq queue we know about."""
    try:
        client = _client()
        return {
            'default': int(client.zcard('dramatiq:default.XQ') or 0),
        }
    except redis.RedisError:
        return {'default': 0}
