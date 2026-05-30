"""
Cora human-review switch — runtime on/off for intercepting outbound SMS.

When ON, outbound Cora marketing SMS are held as `pending_review` for manual
approve/cancel in the admin queue. When OFF (the default), Cora's messages send
immediately and nothing is held.

State precedence (mirrors the kill_switch_override pattern in gating_tools):
    1. Redis key 'cora_human_review:enabled' ('1' / '0') — runtime operator toggle
    2. settings.cora_human_review_enabled                — env baseline default

Redis is the runtime source of truth so an operator can flip review on or off
from the admin UI without a redeploy. The key is persistent (no TTL) — the
switch stays exactly where the operator left it. If Redis is unavailable the
settings baseline applies, which defaults to OFF — the safe direction
(send the message, never silently hold it waiting on a human who isn't there).
"""

import logging

from config.settings import settings
from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

_KEY = "cora_human_review:enabled"


def is_review_enabled() -> bool:
    """
    Return True if outbound Cora messages should be held for human review.

    Reads the Redis runtime override first; falls back to the env baseline
    (settings.cora_human_review_enabled, default False) when Redis has no
    value set or is unavailable.
    """
    if redis_available():
        try:
            raw = get_redis().get(_KEY)
            if raw is not None:
                # decode_responses=True → str; guard bytes just in case.
                val = raw.decode() if isinstance(raw, bytes) else raw
                return val == "1"
        except Exception as exc:
            logger.warning("cora_review_switch read failed, using baseline: %s", exc)
    return bool(settings.cora_human_review_enabled)


def set_review_enabled(enabled: bool, actor: str | None = None) -> bool:
    """
    Turn the human-review switch on or off at runtime.

    Writes a persistent (no-TTL) Redis key so the switch survives until an
    operator changes it. Returns the value that is now in effect.

    Raises RuntimeError if Redis is unavailable — the toggle is meaningless
    without a runtime store, and we must not silently no-op an operator action.
    """
    if not redis_available():
        raise RuntimeError(
            "Redis unavailable — cannot persist the Cora human-review switch."
        )
    try:
        # Persistent SET (no expiry) — rset() always attaches a TTL, so write
        # through the raw client here.
        get_redis().set(_KEY, "1" if enabled else "0")
    except Exception as exc:
        logger.error("cora_review_switch write failed: %s", exc)
        raise RuntimeError("Failed to persist the Cora human-review switch.") from exc

    logger.info(
        "Cora human-review switch set to %s by %s",
        "ON" if enabled else "OFF",
        actor or "unknown",
    )
    return enabled
