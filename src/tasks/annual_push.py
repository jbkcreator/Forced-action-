"""
Annual Push — Item 6.

Checks ANNUAL_PUSH_TRIGGERS daily for every active subscriber and sends
the annual lock offer via email (SMS pending Subscriber.phone in 2B-2).

Triggers (ANY fires the push):
  - charter_day_7:      Day 7 for founding members (first 50 charter users)
  - day_10_14:          Day 10–14 for all users
  - two_deals:          2+ confirmed deal outcomes
  - spend_250:          $250+ cumulative wallet spend (debit transactions)
  - deal_win_10k:       Single deal reported at $10K+
  - auto_switch_day_60: Day 60 — automated annual offer

Cron: 0 8 * * * (8 AM UTC, after scoring)
"""
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.revenue_ladder import ANNUAL_PLAN
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import DealOutcome, Subscriber, WalletTransaction

logger = logging.getLogger(__name__)


def run_annual_push(dry_run: bool = False) -> dict:
    """Check all active subscribers and push annual offer to qualifiers."""
    results = {"checked": 0, "triggered": 0, "pushed": 0, "errors": 0}

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(Subscriber.status == "active")
        ).scalars().all()

        for sub in subs:
            results["checked"] += 1
            try:
                triggers = _check_triggers(sub, db)
                if triggers:
                    results["triggered"] += 1
                    if not dry_run:
                        if _push_annual_offer(sub, triggers[0], db):
                            results["pushed"] += 1
            except Exception as exc:
                logger.error("Annual push failed for subscriber %d: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[AnnualPush] checked=%d triggered=%d pushed=%d errors=%d dry_run=%s",
        results["checked"], results["triggered"], results["pushed"], results["errors"], dry_run,
    )
    return results


def _check_triggers(sub: Subscriber, db: Session) -> list[str]:
    """Return list of trigger names that apply to this subscriber (may be empty)."""
    if sub.tier == "annual_lock":
        return []   # already annual

    now = datetime.now(timezone.utc)
    created = sub.created_at
    if created and created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    account_age = (now - created).days if created else 0

    triggered: list[str] = []

    if sub.founding_member and account_age == 7:
        triggered.append("charter_day_7")

    if 10 <= account_age <= 14:
        triggered.append("day_10_14")

    deal_count = db.execute(
        select(func.count(DealOutcome.id)).where(
            DealOutcome.subscriber_id == sub.id,
            DealOutcome.deal_size_bucket != "skip",
        )
    ).scalar_one_or_none() or 0
    if deal_count >= 2:
        triggered.append("two_deals")

    total_debits = db.execute(
        select(func.sum(func.abs(WalletTransaction.amount))).where(
            WalletTransaction.subscriber_id == sub.id,
            WalletTransaction.txn_type == "debit",
        )
    ).scalar_one_or_none() or 0
    if float(total_debits) * 2.5 >= 250:
        triggered.append("spend_250")

    big_deal = db.execute(
        select(DealOutcome.id).where(
            DealOutcome.subscriber_id == sub.id,
            DealOutcome.deal_amount >= 10000,
        ).limit(1)
    ).scalar_one_or_none()
    if big_deal:
        triggered.append("deal_win_10k")

    if account_age == 60:
        triggered.append("auto_switch_day_60")

    return triggered


def _push_annual_offer(sub: Subscriber, trigger: str, db: Session) -> bool:
    """Send annual offer. Returns True if dispatched successfully."""
    if not sub.email:
        logger.debug("No email for subscriber %d — annual push skipped", sub.id)
        return False

    annual_cents = ANNUAL_PLAN["price_cents"]
    monthly_cents = ANNUAL_PLAN["effective_monthly_cents"]
    annual_str = f"${annual_cents // 100:,}"
    monthly_str = f"${monthly_cents // 100}"

    feed_url = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        if sub.event_feed_uuid
        else settings.app_base_url
    )

    try:
        from src.services.email import send_email
        send_email(
            to=sub.email,
            subject="Save 2 months — lock your territory for a full year",
            body_text=(
                f"Hi {sub.name or 'there'},\n\n"
                f"Lock in your Forced Action territory for a full year at just "
                f"{annual_str}/yr ({monthly_str}/mo effective — 2 months free).\n\n"
                f"This rate is available now. Visit your dashboard to upgrade:\n{feed_url}\n\n"
                f"Or reply to this email and we'll take care of it.\n\n"
                f"— Forced Action Team"
            ),
        )
        logger.info(
            "[AnnualPush] Offer sent: subscriber=%d trigger=%s", sub.id, trigger
        )
        return True
    except Exception as exc:
        logger.error("Annual push email failed for subscriber %d: %s", sub.id, exc)
        return False


def switch_to_annual(subscriber_id: int, db: Session) -> bool:
    """
    Switch a subscriber's Stripe subscription from monthly to annual.
    Called after subscriber accepts the annual offer.
    Returns True on success.
    """
    from src.services.stripe_service import switch_subscription_plan

    sub = db.get(Subscriber, subscriber_id)
    if not sub or not sub.stripe_subscription_id:
        logger.error(
            "switch_to_annual: subscriber %d has no active subscription", subscriber_id
        )
        return False

    price_id = settings.active_stripe_price("annual_lock")
    if not price_id:
        logger.error("switch_to_annual: STRIPE_PRICE_ANNUAL_LOCK not configured")
        return False

    try:
        switch_subscription_plan(sub.stripe_subscription_id, price_id)
        sub.tier = "annual_lock"
        logger.info("Subscriber %d switched to annual_lock", subscriber_id)
        return True
    except Exception as exc:
        logger.error("switch_to_annual failed for subscriber %d: %s", subscriber_id, exc)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_annual_push(dry_run=dry))
