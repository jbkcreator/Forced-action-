"""
Revenue Signal Score — Balanced RFM (0–100).

Weights:
    spend_velocity       25%
    engagement_recency   25%
    wallet_lock_status   20%
    lead_interaction_rate 20%
    zip_competition      10%
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import Subscriber, UserSegment, WalletBalance, WalletTransaction

logger = logging.getLogger(__name__)

WEIGHTS = {
    "spend_velocity": 0.25,
    "engagement_recency": 0.25,
    "wallet_lock_status": 0.20,
    "lead_interaction_rate": 0.20,
    "zip_competition": 0.10,
}


def compute_score(subscriber_id: int, db: Session) -> int:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return 0

    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    raw = (
        _spend_velocity(subscriber_id, db) * WEIGHTS["spend_velocity"]
        + _engagement_recency(subscriber_id, db) * WEIGHTS["engagement_recency"]
        + _wallet_lock_status(sub, wallet) * WEIGHTS["wallet_lock_status"]
        + _lead_interaction_rate(subscriber_id, db) * WEIGHTS["lead_interaction_rate"]
        + _zip_competition(sub, db) * WEIGHTS["zip_competition"]
    )

    score = min(100, max(0, int(round(raw * 100))))

    seg = db.execute(
        select(UserSegment).where(UserSegment.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    if seg:
        seg.revenue_signal_score = score
        db.flush()

    return score


def _spend_velocity(subscriber_id: int, db: Session) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    total_debits = db.execute(
        select(func.sum(WalletTransaction.amount)).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= cutoff,
        )
    ).scalar() or 0
    spent = abs(total_debits)
    # Normalize: 20+ credits/month = 1.0
    return min(1.0, spent / 20.0)


def _engagement_recency(subscriber_id: int, db: Session) -> float:
    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.updated_at:
        return 0.0
    days_since = (datetime.now(timezone.utc) - sub.updated_at).days
    if days_since <= 1:
        return 1.0
    if days_since <= 7:
        return 0.7
    if days_since <= 14:
        return 0.4
    if days_since <= 30:
        return 0.2
    return 0.0


def _wallet_lock_status(sub: Subscriber, wallet: Optional[WalletBalance]) -> float:
    if wallet is None:
        return 0.0
    if wallet.wallet_tier == "power":
        return 1.0
    if wallet.wallet_tier == "growth":
        return 0.7
    if wallet.wallet_tier == "starter_wallet":
        return 0.4
    return 0.0


def _lead_interaction_rate(subscriber_id: int, db: Session) -> float:
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    interactions = db.execute(
        select(func.count()).select_from(WalletTransaction).where(
            WalletTransaction.subscriber_id == subscriber_id,
            WalletTransaction.txn_type == "debit",
            WalletTransaction.created_at >= cutoff,
        )
    ).scalar() or 0
    # Normalize: 10+ interactions/month = 1.0
    return min(1.0, interactions / 10.0)


def _zip_competition(sub: Subscriber, db: Session) -> float:
    from src.core.redis_client import redis_available, rget
    if not redis_available():
        return 0.5  # neutral default without Redis
    # Placeholder: would query ZIP activity counters from Redis sorted set
    return 0.5
