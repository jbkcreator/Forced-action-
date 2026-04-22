"""
Lead Hold service — Item 35.

20-minute TTL reservation per lead. Prevents two subscribers from working
the same lead simultaneously. Uses Redis for TTL; no-ops gracefully when
Redis is unavailable (hold tracking simply skipped).
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

_HOLD_TTL = 20 * 60   # 20 minutes in seconds
_KEY = "lead_hold:{lead_id}"


def hold(lead_id: int, subscriber_id: int) -> dict:
    """
    Reserve lead_id for subscriber_id for 20 minutes.

    Returns dict with held=True on success, held=False if already held by someone else.
    When Redis is unavailable, returns held=True (optimistic — no enforcement).
    """
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=_HOLD_TTL)
    out = {
        "lead_id": lead_id,
        "subscriber_id": subscriber_id,
        "expires_at": expires_at.isoformat(),
        "hold_minutes": 20,
    }

    if not redis_available():
        logger.debug("Redis unavailable — lead hold skipped for lead=%d", lead_id)
        return {"held": True, **out}

    r = get_redis()
    key = f"lead_hold:{lead_id}"
    existing = r.get(key)
    if existing is not None:
        current_holder = int(existing)
        if current_holder != subscriber_id:
            logger.info("Lead %d already held by subscriber %d", lead_id, current_holder)
            return {"held": False, "held_by": current_holder, "lead_id": lead_id}

    r.setex(key, _HOLD_TTL, str(subscriber_id))
    logger.info("Lead %d held by subscriber %d (20 min TTL)", lead_id, subscriber_id)
    return {"held": True, **out}


def get_holder(lead_id: int) -> Optional[int]:
    """Return the subscriber_id currently holding this lead, or None."""
    if not redis_available():
        return None
    val = get_redis().get(f"lead_hold:{lead_id}")
    return int(val) if val else None


def is_held(lead_id: int) -> bool:
    """True if any subscriber currently holds this lead."""
    return get_holder(lead_id) is not None


def is_held_by(lead_id: int, subscriber_id: int) -> bool:
    """True if this specific subscriber holds the lead."""
    return get_holder(lead_id) == subscriber_id


def release(lead_id: int, subscriber_id: int) -> bool:
    """
    Release the hold. Only the holder can release.
    Returns True if released, False if not held by this subscriber.
    """
    if not redis_available():
        return False
    r = get_redis()
    key = f"lead_hold:{lead_id}"
    current = r.get(key)
    if current is not None and int(current) == subscriber_id:
        r.delete(key)
        logger.info("Lead %d hold released by subscriber %d", lead_id, subscriber_id)
        return True
    return False


def get_active_holds(subscriber_id: int) -> list[int]:
    """Return list of lead_ids currently held by subscriber_id (Redis SCAN-based)."""
    if not redis_available():
        return []
    r = get_redis()
    held = []
    try:
        for key in r.scan_iter("lead_hold:*", count=100):
            val = r.get(key)
            if val and int(val) == subscriber_id:
                lead_id = int(key.split(":")[-1])
                held.append(lead_id)
    except Exception as exc:
        logger.warning("get_active_holds scan failed: %s", exc)
    return held
