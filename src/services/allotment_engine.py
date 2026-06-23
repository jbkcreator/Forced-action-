"""
Free-tier allotment engine — weekly action limits with Redis counters.

Redis is the primary store; falls back to Postgres WalletTransaction counts
when Redis is unavailable (server-only deployment).
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.revenue_ladder import FREE_ALLOTMENT
from src.core.models import WalletBalance, WalletTransaction

logger = logging.getLogger(__name__)

ALLOTMENT_KEYS = {
    "skip_trace": "skips_per_week",
    "outbound_text": "texts_per_week",
    "voicemail": "voicemails_per_week",
}


def _week_key() -> str:
    iso = datetime.now(timezone.utc).isocalendar()
    return f"{iso[0]}-{iso[1]}"


def _redis_key(subscriber_id: int, action: str) -> str:
    return f"allotment:{subscriber_id}:{action}:{_week_key()}"


def get_remaining(subscriber_id: int, action: str, db: Session) -> int:
    allotment_key = ALLOTMENT_KEYS.get(action)
    if not allotment_key:
        return 999  # unlimited for unknown actions

    limit = FREE_ALLOTMENT.get(allotment_key, 0)

    from src.core.redis_client import redis_available, rget
    if redis_available():
        val = rget(_redis_key(subscriber_id, action))
        used = int(val) if val else 0
    else:
        # Postgres fallback: count debit transactions this week
        week_start = _week_start()
        used = db.execute(
            select(func.count()).select_from(WalletTransaction).where(
                WalletTransaction.subscriber_id == subscriber_id,
                WalletTransaction.txn_type == "debit",
                WalletTransaction.description == action,
                WalletTransaction.created_at >= week_start,
            )
        ).scalar() or 0

    return max(0, limit - used)


def can_perform(subscriber_id: int, action: str, db: Session) -> bool:
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if wallet and wallet.credits_remaining > 0:
        return True
    return get_remaining(subscriber_id, action, db) > 0


def consume(subscriber_id: int, action: str, db: Session) -> bool:
    """
    Attempt to consume one unit of the given action for this subscriber.

    Wallet holders debit from their wallet balance. Free-tier subscribers
    are gated by the weekly allotment. Any paid subscriber without wallet
    credits (e.g. a future non-free tier) falls through to the allotment
    check — callers must NOT add an outer `tier == "free"` guard, as that
    would bypass this function for paid tiers entirely.

    Returns True if the action is permitted and the usage has been recorded,
    False if the weekly cap is exhausted.
    """
    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if wallet and wallet.credits_remaining > 0:
        from src.services.wallet_engine import debit
        return debit(subscriber_id, action, db)

    if get_remaining(subscriber_id, action, db) <= 0:
        return False

    # Free-tier: increment Redis counter
    from src.core.redis_client import redis_available, rincr
    if redis_available():
        rincr(_redis_key(subscriber_id, action), ttl_seconds=7 * 86400)
    return True


def _week_start() -> datetime:
    now = datetime.now(timezone.utc)
    # Monday of current ISO week
    day_of_week = now.weekday()  # 0=Monday
    return (now - timedelta(days=day_of_week)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
