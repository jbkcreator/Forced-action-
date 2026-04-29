"""
Urgency window engine — Redis-backed lead viewing windows for FOMO display.

All operations are no-ops when Redis is unavailable (server-only feature).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from config.cora_guardrails import get_guardrail
from src.core.redis_client import redis_available, rdelete, rget, rset

logger = logging.getLogger(__name__)


def create_window(lead_id: int, zip_code: str, vertical: str) -> dict:
    guardrail = get_guardrail("urgency_window")
    window_minutes = guardrail.get("min_minutes", 30)
    ttl_seconds = window_minutes * 60
    expires_at = datetime.now(timezone.utc).timestamp() + ttl_seconds

    data = {
        "lead_id": lead_id,
        "zip_code": zip_code,
        "vertical": vertical,
        "expires_at": expires_at,
        "window_minutes": window_minutes,
    }

    if redis_available():
        key = f"urgency:{lead_id}"
        rset(key, json.dumps(data), ttl_seconds=ttl_seconds)
        _increment_zip_counter(zip_code, ttl_seconds)

    return data


def get_window(lead_id: int) -> Optional[dict]:
    if not redis_available():
        return None
    val = rget(f"urgency:{lead_id}")
    if not val:
        return None
    try:
        return json.loads(val)
    except Exception:
        return None


def get_active_count(zip_code: str) -> int:
    if not redis_available():
        return 0
    from src.core.redis_client import _get_client
    client = _get_client()
    if client is None:
        return 0
    try:
        count = client.zcard(f"urgency_zips:{zip_code}")
        return count or 0
    except Exception:
        return 0


def is_within_window(lead_id: int) -> bool:
    window = get_window(lead_id)
    if not window:
        return False
    return datetime.now(timezone.utc).timestamp() < window.get("expires_at", 0)


def expire_window(lead_id: int) -> None:
    if not redis_available():
        return
    rdelete(f"urgency:{lead_id}")


def _increment_zip_counter(zip_code: str, ttl_seconds: int) -> None:
    from src.core.redis_client import _get_client
    client = _get_client()
    if client is None:
        return
    try:
        now_ts = datetime.now(timezone.utc).timestamp()
        key = f"urgency_zips:{zip_code}"
        client.zadd(key, {str(now_ts): now_ts})
        client.expire(key, ttl_seconds)
        # Prune entries older than the window
        client.zremrangebyscore(key, "-inf", now_ts - ttl_seconds)
    except Exception as exc:
        logger.debug("urgency_zips increment failed: %s", exc)
