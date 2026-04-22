"""
Proactive Save — Item 7 (Data-Only Save Tier).

Identifies at-risk subscribers and offers the Data-Only plan ($97/mo) to
prevent churn.

Triggers (either fires the save offer):
  - inactivity:          5–7 days with no wallet activity
  - payment_failure_day5: subscriber has been in grace for 5+ days

Cron: 0 10 * * * (10 AM UTC daily, after scoring + annual push)
"""
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.revenue_ladder import DATA_ONLY_TIER
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import Subscriber, WalletTransaction

logger = logging.getLogger(__name__)

_INACTIVE_MIN = 5
_INACTIVE_MAX = 7


def run_proactive_save(dry_run: bool = False) -> dict:
    """Identify at-risk subscribers and send Data-Only save offers."""
    results = {"checked": 0, "at_risk": 0, "offers_sent": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.status.in_(["active", "grace"]))
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                trigger = _identify_risk(sub, db)
                if trigger:
                    results["at_risk"] += 1
                    if not dry_run:
                        if _send_save_offer(sub, trigger):
                            results["offers_sent"] += 1
            except Exception as exc:
                logger.error("Proactive save failed for subscriber %d: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[ProactiveSave] checked=%d at_risk=%d offers_sent=%d errors=%d dry_run=%s",
        results["checked"], results["at_risk"], results["offers_sent"], results["errors"], dry_run,
    )
    return results


def _identify_risk(sub: Subscriber, db: Session) -> Optional[str]:
    """Return trigger string if subscriber is at risk, else None."""
    if sub.tier in ("data_only", "free"):
        return None

    now = datetime.now(timezone.utc)

    # Trigger 1: 5–7 days with no wallet transactions (proxy for inactivity)
    last_txn_at = db.execute(
        select(WalletTransaction.created_at)
        .where(WalletTransaction.subscriber_id == sub.id)
        .order_by(WalletTransaction.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    ref = last_txn_at or sub.created_at
    if ref and ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    inactive_days = (now - ref).days if ref else 999

    if _INACTIVE_MIN <= inactive_days <= _INACTIVE_MAX:
        return "inactivity"

    # Trigger 2: Day 5+ of grace period (payment failure)
    if sub.status == "grace" and sub.grace_expires_at:
        expires = sub.grace_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        grace_entered = expires - timedelta(hours=settings.grace_period_hours)
        days_in_grace = (now - grace_entered).days
        if days_in_grace >= 5:
            return "payment_failure_day5"

    return None


def _send_save_offer(sub: Subscriber, trigger: str) -> bool:
    """Send Data-Only save offer email. Returns True if sent."""
    if not sub.email:
        return False

    price = DATA_ONLY_TIER["price_cents"] // 100
    trigger_line = (
        "We noticed you haven't been active recently — life gets busy."
        if trigger == "inactivity"
        else "We noticed your payment hasn't gone through yet."
    )

    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid
        else settings.app_base_url
    )

    try:
        from src.services.email import send_email
        send_email(
            to=sub.email,
            subject=f"Keep your leads for ${price}/mo — Data-Only access",
            body_text=(
                f"Hi {sub.name or 'there'},\n\n"
                f"{trigger_line}\n\n"
                f"We don't want you to lose your territory. Switch to our Data-Only plan at "
                f"just ${price}/mo — full property data feed, no enrichment fees, cancel anytime.\n\n"
                f"Visit your dashboard to make the switch:\n{feed_url}\n\n"
                f"Questions? Reply to this email.\n\n"
                f"— Forced Action Team"
            ),
        )
        logger.info("[ProactiveSave] Offer sent: subscriber=%d trigger=%s", sub.id, trigger)
        return True
    except Exception as exc:
        logger.error("Save offer email failed for subscriber %d: %s", sub.id, exc)
        return False


def downgrade_to_data_only(subscriber_id: int, db: Session) -> bool:
    """
    Downgrade subscriber to Data-Only plan via Stripe.
    Called when subscriber accepts the save offer.
    Returns True on success.
    """
    from src.services.stripe_service import switch_subscription_plan

    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.stripe_subscription_id:
        logger.error(
            "downgrade_to_data_only: subscriber %d has no active subscription", subscriber_id
        )
        return False

    price_id = settings.active_stripe_price("data_only")
    if not price_id:
        logger.error("downgrade_to_data_only: STRIPE_PRICE_DATA_ONLY not configured")
        return False

    try:
        switch_subscription_plan(sub.stripe_subscription_id, price_id)
        sub.tier = "data_only"
        logger.info("Subscriber %d downgraded to data_only", subscriber_id)
        return True
    except Exception as exc:
        logger.error("downgrade_to_data_only failed for subscriber %d: %s", subscriber_id, exc)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_proactive_save(dry_run=dry))
