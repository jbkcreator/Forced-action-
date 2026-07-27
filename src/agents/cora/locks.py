"""
Per-opportunity_thread_id lock — "one active execution per opportunity" via
a plain Redis SET NX EX, chosen over Postgres locking (src.core.redis_client
is already a dependency; adding Postgres advisory-lock usage would touch
nothing forbidden either, but Redis keeps this consistent with the rest of
Cora's reliability mechanics, which are all Redis-side).
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator, Optional

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

_LOCK_PREFIX = "lock:cora:"
_DEFAULT_TTL_SECONDS = 300


def acquire(opportunity_thread_id: str, owner: str, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> bool:
    if not redis_available():
        # No Redis == no other worker to race against in this process's
        # test/dev context; fail open rather than blocking all progress.
        return True
    key = f"{_LOCK_PREFIX}{opportunity_thread_id}"
    return bool(get_redis().set(key, owner, nx=True, ex=ttl_seconds))


def release(opportunity_thread_id: str, owner: str) -> None:
    if not redis_available():
        return
    key = f"{_LOCK_PREFIX}{opportunity_thread_id}"
    r = get_redis()
    current = r.get(key)
    if current == owner:
        r.delete(key)


@contextmanager
def opportunity_lock(opportunity_thread_id: str, owner: str, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> Iterator[bool]:
    """Yields True if the lock was acquired, False otherwise. Always releases on exit if held."""
    acquired = acquire(opportunity_thread_id, owner, ttl_seconds=ttl_seconds)
    try:
        yield acquired
    finally:
        if acquired:
            release(opportunity_thread_id, owner)
