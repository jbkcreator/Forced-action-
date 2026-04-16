"""
Stripe Subscription Reconciliation Task
========================================
Runs daily to catch any subscribers who paid via Stripe but were never
activated in our DB — e.g. if all 87 Stripe webhook retries failed.

For each active Stripe subscription with no matching subscriber record,
this task re-fires the checkout.session.completed logic by fetching the
original checkout session and replaying it.

Usage:
    python -m src.tasks.stripe_reconcile
    python -m src.tasks.stripe_reconcile --dry-run
"""

import argparse
import logging
import sys

import stripe

from config.settings import settings
from src.core.database import get_db_context
from src.services.stripe_webhooks import _on_checkout_completed

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def _init_stripe() -> bool:
    try:
        secret = settings.active_stripe_secret_key
        if not secret:
            return False
        stripe.api_key = secret.get_secret_value()
        return True
    except Exception:
        return False


def reconcile_subscriptions(dry_run: bool = False) -> dict:
    """
    Compare active Stripe subscriptions against our subscribers table.
    Activate any that are missing.

    Returns:
        dict with keys: checked, already_active, activated, failed
    """
    if not _init_stripe():
        logger.error("Stripe not configured — cannot reconcile")
        return {"checked": 0, "already_active": 0, "activated": 0, "failed": 0}

    stats = {"checked": 0, "already_active": 0, "activated": 0, "failed": 0}

    # Fetch all active Stripe subscriptions (paginated)
    logger.info("Fetching active Stripe subscriptions...")
    subscriptions = []
    params = {"status": "active", "limit": 100, "expand": ["data.customer"]}
    while True:
        page = stripe.Subscription.list(**params)
        subscriptions.extend(page.data)
        if not page.has_more:
            break
        params["starting_after"] = page.data[-1].id

    logger.info("Found %d active Stripe subscriptions", len(subscriptions))

    with get_db_context() as db:
        from sqlalchemy import select
        from src.core.models import Subscriber

        for sub in subscriptions:
            stats["checked"] += 1
            customer_id = sub.customer.id if hasattr(sub.customer, "id") else sub.customer

            # Check if subscriber exists in our DB
            existing = db.execute(
                select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
            ).scalar_one_or_none()

            if existing and existing.status == "active":
                stats["already_active"] += 1
                logger.debug("OK: customer %s already active (subscriber_id=%s)", customer_id, existing.id)
                continue

            logger.warning(
                "GAP DETECTED: Stripe customer %s has active subscription %s but %s in our DB",
                customer_id, sub.id,
                f"status={existing.status}" if existing else "no record"
            )

            if dry_run:
                logger.info("[DRY RUN] Would activate customer %s", customer_id)
                stats["activated"] += 1
                continue

            # Find the original checkout session to replay metadata
            try:
                sessions = stripe.checkout.Session.list(
                    customer=customer_id,
                    limit=10,
                )
                checkout_session = None
                for s in sessions.data:
                    if s.subscription == sub.id and s.status == "complete":
                        checkout_session = s
                        break

                if not checkout_session:
                    logger.error(
                        "Could not find completed checkout session for customer %s / subscription %s",
                        customer_id, sub.id
                    )
                    stats["failed"] += 1
                    continue

                # Replay the checkout handler
                session_dict = checkout_session.to_dict_recursive() if hasattr(checkout_session, "to_dict_recursive") else dict(checkout_session)
                _on_checkout_completed(session_dict, db)
                db.commit()

                logger.info(
                    "RECONCILED: customer %s activated via checkout session %s",
                    customer_id, checkout_session.id
                )
                stats["activated"] += 1

            except stripe.error.StripeError as e:
                logger.error("Stripe error reconciling customer %s: %s", customer_id, e)
                stats["failed"] += 1
            except Exception as e:
                logger.error("Error reconciling customer %s: %s", customer_id, e, exc_info=True)
                db.rollback()
                stats["failed"] += 1

    logger.info(
        "Reconciliation complete — checked=%d already_active=%d activated=%d failed=%d",
        stats["checked"], stats["already_active"], stats["activated"], stats["failed"]
    )
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile Stripe subscriptions with local DB")
    parser.add_argument("--dry-run", action="store_true", help="Check only, do not activate")
    args = parser.parse_args()

    result = reconcile_subscriptions(dry_run=args.dry_run)
    if result["failed"] > 0:
        sys.exit(1)
