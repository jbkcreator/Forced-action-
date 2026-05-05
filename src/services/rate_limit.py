"""
Lightweight per-IP rate limiter for public API endpoints.

Backed by Redis when available (multi-worker-safe), with an in-memory
fallback for dev/test. Single-worker dev uses the fallback transparently;
production with multiple uvicorn workers should run with Redis configured.

Usage:
    from src.services.rate_limit import enforce_or_429
    enforce_or_429(request, scope="leaderboard", limit=60, window_seconds=60)

The helper raises HTTPException(429) when the IP exceeds the limit. Failure
modes (Redis down, header parsing error) fail-open — rate limiting is a
soft DoS protection, not a security boundary.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Deque

from fastapi import HTTPException, Request

from src.core.redis_client import redis_available, rincr

logger = logging.getLogger(__name__)


# In-memory fallback — per (scope, ip) deque of recent timestamps.
# Bounded size per key prevents memory growth from a single attacker.
_LOCAL_BUCKET_MAX = 1000
_local: dict[str, Deque[float]] = defaultdict(deque)
_local_lock = Lock()


def _client_ip(request: Request) -> str:
    """Pick the most-trustworthy IP we can. Fail-soft to 'unknown'.

    Behind a CDN / load balancer, X-Forwarded-For is usually a comma-list
    'client, proxy1, proxy2'; we take the leftmost non-empty token. If
    no header is present, fall back to request.client.host. Any failure
    returns 'unknown' (which collapses everyone into one bucket — strict).
    """
    try:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            first = xff.split(",")[0].strip()
            if first:
                return first
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
        if request.client and request.client.host:
            return request.client.host
    except Exception:
        pass
    return "unknown"


def _redis_check(key: str, limit: int, window_seconds: int) -> bool:
    """Returns True if the request should be allowed, False if rate-limited."""
    count = rincr(key, ttl_seconds=window_seconds)
    if count <= 0:
        return True   # Redis hiccup — fail-open
    return count <= limit


def _local_check(key: str, limit: int, window_seconds: int) -> bool:
    """In-memory sliding window. Single-process only."""
    now = time.monotonic()
    cutoff = now - window_seconds
    with _local_lock:
        bucket = _local[key]
        # Drop expired hits
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        # Cap unbounded growth from a single attacker
        if len(bucket) > _LOCAL_BUCKET_MAX:
            for _ in range(len(bucket) - _LOCAL_BUCKET_MAX):
                bucket.popleft()
        return True


def is_allowed(request: Request, scope: str, limit: int, window_seconds: int) -> bool:
    """Return True if this request is within the limit. False if rate-limited.

    `scope` namespaces the limiter (e.g. 'leaderboard') so different endpoints
    have independent buckets.
    """
    ip = _client_ip(request)
    key = f"rl:{scope}:{ip}"
    try:
        if redis_available():
            return _redis_check(key, limit, window_seconds)
        return _local_check(key, limit, window_seconds)
    except Exception as exc:
        logger.warning("rate_limit check failed (fail-open): %s", exc)
        return True


def enforce_or_429(request: Request, scope: str, limit: int, window_seconds: int) -> None:
    """Raise HTTPException(429) if the IP has exceeded the limit; else return."""
    if not is_allowed(request, scope, limit, window_seconds):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limited",
                "scope": scope,
                "retry_after_seconds": window_seconds,
                "message": "Too many requests — please slow down.",
            },
            headers={"Retry-After": str(window_seconds)},
        )


def reset_local_buckets() -> None:
    """Test-only: clear the in-memory state between tests."""
    with _local_lock:
        _local.clear()
