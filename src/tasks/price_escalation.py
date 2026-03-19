"""
6-month founding rate escalation task.

Finds founding members whose rate lock has been in place for ≥ 6 months
and whose subscription has NOT yet been escalated, then:
  1. Updates their Stripe subscription to the current regular price.
  2. Sets subscriber.escalated_at = now().
  3. Sends a notification email.

Run weekly via cron (Monday at 8 AM so it never runs on a billing weekend):
    0 8 * * 1 $PROJECT/scripts/cron/run.sh src.tasks.price_escalation

Supports --dry-run to preview eligible subscribers without making changes.
"""

import logging
from datetime import datetime, timezone, timedelta

import stripe
from sqlalchemy import select

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import Subscriber
from src.services.email import send_email

logger = logging.getLogger(__name__)

# Regular price map: (tier, vertical) → settings attribute name
# The vertical dimension is currently the same price per tier, but kept
# extensible in case verticals diverge later.
_REGULAR_PRICE_ATTR = {
    "starter":   "stripe_price_starter_regular",
    "pro":       "stripe_price_pro_regular",
    "dominator": "stripe_price_dominator_regular",
}

_SIX_MONTHS = timedelta(days=183)


def run_price_escalation(dry_run: bool = False) -> dict:
    """
    Escalate founding members who have passed the 6-month rate lock window.

    Returns:
        dict with keys: checked, eligible, escalated, failed, dry_run
    """
    settings = get_settings()
    stats = {"checked": 0, "eligible": 0, "escalated": 0, "failed": 0, "dry_run": dry_run}

    if not settings.stripe_secret_key:
        logger.warning("[PriceEscalation] STRIPE_SECRET_KEY not set — cannot escalate")
        return stats

    stripe.api_key = settings.stripe_secret_key.get_secret_value()
    cutoff = datetime.now(timezone.utc) - _SIX_MONTHS

    with get_db_context() as db:
        eligible = db.execute(
            select(Subscriber).where(
                Subscriber.founding_member == True,  # noqa: E712
                Subscriber.status == "active",
                Subscriber.rate_locked_at <= cutoff,
                Subscriber.escalated_at == None,  # noqa: E711
                Subscriber.stripe_subscription_id != None,  # noqa: E711
            )
        ).scalars().all()

        stats["checked"] = len(eligible)
        logger.info(
            "[PriceEscalation] Found %d founding member(s) eligible for rate escalation%s",
            len(eligible), " (DRY RUN)" if dry_run else "",
        )

        for subscriber in eligible:
            stats["eligible"] += 1
            regular_price_id = getattr(settings, _REGULAR_PRICE_ATTR.get(subscriber.tier, ""), None)

            if not regular_price_id:
                logger.error(
                    "[PriceEscalation] No regular price configured for tier '%s' — skipping %s",
                    subscriber.tier, subscriber.id,
                )
                stats["failed"] += 1
                continue

            if dry_run:
                logger.info(
                    "[PriceEscalation] DRY RUN — would escalate subscriber %s (%s) "
                    "from founding price to %s",
                    subscriber.id, subscriber.email, regular_price_id,
                )
                continue

            try:
                # Retrieve current subscription items to get the item ID
                sub = stripe.Subscription.retrieve(subscriber.stripe_subscription_id)
                item_id = sub["items"]["data"][0]["id"]

                # Update to regular price (prorated at next billing cycle)
                stripe.Subscription.modify(
                    subscriber.stripe_subscription_id,
                    items=[{"id": item_id, "price": regular_price_id}],
                    proration_behavior="none",  # switch at next renewal, no charge now
                )

                # Mark escalated in DB
                subscriber.escalated_at = datetime.now(timezone.utc)
                db.flush()

                stats["escalated"] += 1
                logger.info(
                    "[PriceEscalation] Escalated subscriber %s (%s) to regular price %s",
                    subscriber.id, subscriber.email, regular_price_id,
                )

                # Send notification email
                if subscriber.email:
                    dashboard_url = (
                        f"{settings.app_base_url}/dashboard/{subscriber.event_feed_uuid}"
                        if subscriber.event_feed_uuid else settings.app_base_url
                    )
                    send_email(
                        to=subscriber.email,
                        subject="Update to your Forced Action subscription",
                        body_text=(
                            f"Hi {subscriber.name or 'there'},\n\n"
                            f"Your 6-month founding rate lock has now expired. "
                            f"Your subscription will automatically renew at the current "
                            f"{subscriber.tier.title()} plan rate starting with your next "
                            f"billing cycle.\n\n"
                            f"Your ZIP territories and lead access remain unchanged — "
                            f"only the price updates.\n\n"
                            f"Access your lead feed:\n{dashboard_url}\n\n"
                            f"Questions? support@forcedaction.io\n\n"
                            f"— Forced Action Team"
                        ),
                    )

            except stripe.StripeError as exc:
                logger.error(
                    "[PriceEscalation] Stripe error escalating subscriber %s: %s",
                    subscriber.id, exc,
                )
                stats["failed"] += 1
            except Exception as exc:
                logger.error(
                    "[PriceEscalation] Unexpected error for subscriber %s: %s",
                    subscriber.id, exc, exc_info=True,
                )
                stats["failed"] += 1

    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Escalate founding member prices after 6 months")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()

    result = run_price_escalation(dry_run=args.dry_run)
    print(result)
