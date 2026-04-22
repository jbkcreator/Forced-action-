"""
Redis client — lazy singleton with graceful degradation.

Not available locally. All callers must guard with redis_available() or
accept None / fallback behavior when Redis is not configured.

Usage:
    from src.core.redis_client import rget, rset, rincr, rdelete, redis_available

    if redis_available():
        rset("key", "value", ttl_seconds=300)
"""

import logging
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)

_redis = None
_init_attempted = False


def _get_client():
    global _redis, _init_attempted
    if _init_attempted:
        return _redis
    _init_attempted = True
    url = settings.redis_url
    if not url:
        return None
    try:
        import redis as _redis_lib
        client = _redis_lib.Redis.from_url(url, decode_responses=True, socket_connect_timeout=2)
        client.ping()
        _redis = client
        logger.info("Redis connected: %s", url)
    except Exception as exc:
        logger.warning("Redis unavailable (%s) — falling back to Postgres counters", exc)
        _redis = None
    return _redis


def redis_available() -> bool:
    return _get_client() is not None


def rset(key: str, value: str, ttl_seconds: int = 300) -> bool:
    client = _get_client()
    if client is None:
        return False
    try:
        client.setex(key, ttl_seconds, value)
        return True
    except Exception as exc:
        logger.warning("Redis rset failed for %s: %s", key, exc)
        return False


def rget(key: str) -> Optional[str]:
    client = _get_client()
    if client is None:
        return None
    try:
        return client.get(key)
    except Exception as exc:
        logger.warning("Redis rget failed for %s: %s", key, exc)
        return None


def rincr(key: str, ttl_seconds: Optional[int] = None) -> int:
    client = _get_client()
    if client is None:
        return 0
    try:
        val = client.incr(key)
        if val == 1 and ttl_seconds:
            client.expire(key, ttl_seconds)
        return val
    except Exception as exc:
        logger.warning("Redis rincr failed for %s: %s", key, exc)
        return 0


def rdelete(key: str) -> None:
    client = _get_client()
    if client is None:
        return
    try:
        client.delete(key)
    except Exception as exc:
        logger.warning("Redis rdelete failed for %s: %s", key, exc)


def get_redis():
    """Return the raw Redis client (or None if unavailable). Use redis_available() guard first."""
    return _get_client()
