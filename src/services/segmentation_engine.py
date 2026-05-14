"""
Segmentation engine — behavioral segment classification.

8 buckets (precedence order; first match wins):
    churned → at_risk → lock_candidate → high_intent →
    wallet_active → new → engaged → browsing

Rules:
    - churned       : subscriber.status in ('churned', 'cancelled')
    - at_risk       : last activity older than 14 days
    - lock_candidate: subscriber.lock_candidate_zip is not null, tier < lock
    - high_intent   : user_segments.revenue_signal_score >= 70
    - wallet_active : wallet credits remaining > 0
    - new           : account < 7 days, no wallet, no engagement
    - engaged       : reply/click in message_outcomes within last 7 days
    - browsing      : fallback
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.core.models import (
    Subscriber,
    UserSegment,
    WalletBalance,
)

logger = logging.getLogger(__name__)

HIGH_INTENT_RSS_THRESHOLD = 70
AT_RISK_INACTIVE_DAYS = 14
NEW_ACCOUNT_AGE_DAYS = 7
ENGAGEMENT_WINDOW_DAYS = 7


def classify(subscriber_id: int, db: Session) -> str:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        raise ValueError(f"Subscriber {subscriber_id} not found")

    wallet = db.execute(
        select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
    ).scalar_one_or_none()

    user_seg = db.execute(
        select(UserSegment).where(UserSegment.subscriber_id == subscriber_id)
    ).scalar_one_or_none()
    rss = user_seg.revenue_signal_score if user_seg and user_seg.revenue_signal_score else 0

    last_activity_at = _last_activity_at(subscriber_id, db)
    has_recent_engagement = _has_recent_engagement(subscriber_id, db)

    segment, reason = _compute_segment(sub, wallet, rss, last_activity_at, has_recent_engagement)
    upsert_segment(subscriber_id, segment, reason, db)
    return segment


def classify_all(db: Session, batch_size: int = 500) -> int:
    subs = db.execute(select(Subscriber.id)).scalars().all()
    count = 0
    for i, sub_id in enumerate(subs, start=1):
        try:
            classify(sub_id, db)
            count += 1
        except Exception as exc:
            logger.warning("classify_all failed for subscriber %s: %s", sub_id, exc)
        if i % batch_size == 0:
            db.commit()
            logger.info("classify_all: %d/%d done", i, len(subs))
    db.commit()
    return count


def reclassify_safe(subscriber_id: int, db: Session) -> None:
    """Recompute revenue_signal_score then classify. Never raises."""
    try:
        from src.services import revenue_signal
        revenue_signal.recompute(subscriber_id, db)
        classify(subscriber_id, db)
    except Exception as exc:
        logger.warning("reclassify_safe failed for sub=%s: %s", subscriber_id, exc)


def _compute_segment(
    sub: Subscriber,
    wallet: Optional[WalletBalance],
    revenue_signal_score: int,
    last_activity_at: Optional[datetime],
    has_recent_engagement: bool,
) -> tuple[str, str]:
    from config.wallet_to_lock import LOCK_OR_ABOVE_TIERS

    now = datetime.now(timezone.utc)

    # 1. churned
    if sub.status in ("churned", "cancelled"):
        return "churned", f"churned:status={sub.status}"

    # 2. at_risk
    if last_activity_at:
        days_inactive = (now - last_activity_at).days
        if days_inactive >= AT_RISK_INACTIVE_DAYS:
            return "at_risk", f"at_risk:inactive={days_inactive}d"

    # 3. lock_candidate
    if sub.lock_candidate_zip and sub.tier not in LOCK_OR_ABOVE_TIERS:
        return "lock_candidate", f"lock_candidate:zip={sub.lock_candidate_zip}"

    # 4. high_intent
    if revenue_signal_score >= HIGH_INTENT_RSS_THRESHOLD:
        return "high_intent", f"high_intent:rss={revenue_signal_score}"

    # 5. wallet_active
    if wallet and wallet.credits_remaining > 0:
        return "wallet_active", f"wallet_active:tier={wallet.wallet_tier}"

    # 6. new
    account_age_days = (now - sub.created_at).days if sub.created_at else 0
    if (
        account_age_days < NEW_ACCOUNT_AGE_DAYS
        and (wallet is None or wallet.credits_remaining == 0)
        and not has_recent_engagement
    ):
        return "new", f"new:account_age={account_age_days}d"

    # 7. engaged
    if has_recent_engagement:
        return "engaged", "engaged:recent_msg_activity"

    # 8. browsing
    return "browsing", "browsing:no_signals"


def _last_activity_at(subscriber_id: int, db: Session) -> Optional[datetime]:
    """Max of message sent, wallet txn, account creation."""
    row = db.execute(text("""
        SELECT GREATEST(
            COALESCE((SELECT MAX(sent_at) FROM message_outcomes WHERE subscriber_id = :sid), 'epoch'::timestamptz),
            COALESCE((SELECT MAX(created_at) FROM wallet_transactions WHERE subscriber_id = :sid), 'epoch'::timestamptz),
            COALESCE((SELECT created_at FROM subscribers WHERE id = :sid), 'epoch'::timestamptz)
        ) AS last_activity
    """), {"sid": subscriber_id}).first()
    if not row or row.last_activity is None:
        return None
    last = row.last_activity
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return last


def _has_recent_engagement(subscriber_id: int, db: Session) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ENGAGEMENT_WINDOW_DAYS)
    return db.execute(text("""
        SELECT 1 FROM message_outcomes
        WHERE subscriber_id = :sid
          AND sent_at >= :cutoff
          AND (replied_at IS NOT NULL OR clicked_at IS NOT NULL)
        LIMIT 1
    """), {"sid": subscriber_id, "cutoff": cutoff}).first() is not None


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
