"""
Opt-in sentinel — Redis-backed guard for TCPA double opt-in.

send_opt_in_prompt sets opt_in_pending:{phone} with a 15-minute TTL.
handle_opt_in_reply calls consume_pending, which atomically reads and deletes
the key — returning True only if the key existed.

Without the sentinel a YES reply returns None and falls through to sms_commands,
preserving the PAUSE-confirm YES flow and preventing arbitrary inbound texts
from polluting the consent log.

Redis degradation: mark_pending is a no-op and consume_pending returns False
when Redis is unavailable. YES replies silently do nothing until Redis recovers.
"""

import logging

from src.core import redis_client

logger = logging.getLogger(__name__)

_TTL_SECONDS = 900  # 15-minute window for YES reply after prompt
_KEY_PREFIX = "opt_in_pending"


def _key(phone: str) -> str:
    return f"{_KEY_PREFIX}:{phone}"


def mark_pending(phone: str) -> None:
    """Set the opt-in sentinel for this phone. No-op when Redis is unavailable."""
    if not phone:
        return
    if not redis_client.redis_available():
        logger.warning("opt_in_sentinel.mark_pending: Redis unavailable — skipping for %s", phone)
        return
    redis_client.rset(_key(phone), "1", ttl_seconds=_TTL_SECONDS)


def consume_pending(phone: str) -> bool:
    """
    Atomically check-and-delete the opt-in sentinel.

    Returns True if the sentinel existed (YES reply is valid consent).
    Returns False if absent (no prompt was sent — caller should fall through).
    """
    if not phone:
        return False
    client = redis_client.get_redis()
    if client is None:
        logger.warning(
            "opt_in_sentinel.consume_pending: Redis unavailable — returning False for %s", phone
        )
        return False
    try:
        result = client.getdel(_key(phone))  # atomic; Redis 6.2+ / fakeredis
        return result is not None
    except Exception:
        # Fallback for Redis < 6.2: non-atomic but safe enough here
        val = redis_client.rget(_key(phone))
        if val is not None:
            redis_client.rdelete(_key(phone))
            return True
        return False
