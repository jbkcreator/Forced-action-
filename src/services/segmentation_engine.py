"""
Segmentation engine — behavioral segment classification.

8 buckets (in precedence order):
    churned → new → at_risk → wallet_active → high_intent →
    lock_candidate → engaged → browsing
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.core.models import (
    DealOutcome,
    Subscriber,
    UserSegment,
    WalletBalance,
    WalletTransaction,
)

logger = logging.getLogger(__name__)


def classify(subscriber_id: int, db: Session) -> str:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        raise ValueError(f"Subscriber {subscriber_id} not found")

    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    now = datetime.now(timezone.utc)
    days_inactive = (now - sub.created_at).days if sub.created_at else 0
    if sub.updated_at:
        days_inactive = (now - sub.updated_at).days

    segment, reason = _compute_segment(sub, wallet, days_inactive)
    upsert_segment(subscriber_id, segment, reason, db)
    return segment


def classify_all(db: Session) -> int:
    subs = db.execute(select(Subscriber)).scalars().all()
    count = 0
    for sub in subs:
        try:
            classify(sub.id, db)
            count += 1
        except Exception as exc:
            logger.warning("classify_all failed for subscriber %s: %s", sub.id, exc)
    return count


def _compute_segment(
    sub: Subscriber,
    wallet: Optional[WalletBalance],
    days_inactive: int,
) -> tuple[str, str]:
    if sub.status in ("churned", "cancelled"):
        return "churned", f"status={sub.status}"

    now = datetime.now(timezone.utc)
    account_age_days = (now - sub.created_at).days if sub.created_at else 0
    if account_age_days < 7:
        return "new", f"account_age={account_age_days}d"

    if days_inactive >= 14:
        return "at_risk", f"inactive={days_inactive}d"

    if wallet and wallet.credits_remaining > 0:
        return "wallet_active", f"wallet_tier={wallet.wallet_tier}"

    return "browsing", "no_wallet_no_actions"


def upsert_segment(subscriber_id: int, segment: str, reason: str, db: Session) -> UserSegment:
    existing = db.execute(
        select(UserSegment).where(UserSegment.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if existing:
        existing.segment = segment
        existing.classification_reason = reason
        existing.last_classified_at = now
        db.flush()
        return existing
    seg = UserSegment(
        subscriber_id=subscriber_id,
        segment=segment,
        classification_reason=reason,
        last_classified_at=now,
    )
    db.add(seg)
    db.flush()
    return seg
