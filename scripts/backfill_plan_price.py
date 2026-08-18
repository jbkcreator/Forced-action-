"""
Backfill plan_price for active non-free subscribers where it is NULL.

Looks up each subscriber's current Stripe subscription, reads the price
unit_amount, normalises to a monthly run-rate, and writes plan_price.

Usage:
    PYTHONPATH=. python scripts/backfill_plan_price.py          # dry-run
    PYTHONPATH=. python scripts/backfill_plan_price.py --apply  # write to DB
"""
from __future__ import annotations

import argparse
import logging
import sys

import stripe
from sqlalchemy import select, text

from config.settings import get_settings
from src.core.database import Database
from src.core.models import Subscriber
from src.services.stripe_webhooks import normalized_monthly_price

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MONTHS_PER_INTERVAL = {"monthly": 1, "month": 1, "annual": 12, "year": 12, "yearly": 12}


def main(apply: bool) -> None:
    settings = get_settings()
    stripe.api_key = settings.stripe_secret_key

    with Database().session_scope() as db:
        rows = db.execute(
            select(Subscriber).where(
                Subscriber.plan_price.is_(None),
                Subscriber.tier.notin_(["free"]),
                Subscriber.status.in_(["active", "grace"]),
                Subscriber.is_test.is_(False),
            )
        ).scalars().all()

        if not rows:
            logger.info("No NULL plan_price subscribers found — nothing to do.")
            return

        logger.info("Found %d subscriber(s) with NULL plan_price:", len(rows))
        for sub in rows:
            logger.info("  id=%s email=%s tier=%s stripe_sub=%s", sub.id, sub.email, sub.tier, sub.stripe_subscription_id)

        fixed = 0
        for sub in rows:
            if not sub.stripe_subscription_id:
                logger.warning("  SKIP sub=%s — no stripe_subscription_id", sub.id)
                continue
            try:
                stripe_sub = stripe.Subscription.retrieve(
                    sub.stripe_subscription_id, expand=["items.data.price"]
                )
            except stripe.error.StripeError as exc:
                logger.error("  SKIP sub=%s — Stripe error: %s", sub.id, exc)
                continue

            items = (stripe_sub.get("items") or {}).get("data") or []
            if not items:
                logger.warning("  SKIP sub=%s — no line items on subscription", sub.id)
                continue

            price = items[0].get("price") or {}
            unit_amount = price.get("unit_amount") or 0
            interval = (price.get("recurring") or {}).get("interval") or "month"
            interval_count = (price.get("recurring") or {}).get("interval_count") or 1

            if unit_amount <= 0:
                logger.warning("  SKIP sub=%s — unit_amount=%s", sub.id, unit_amount)
                continue

            # Normalise: year-interval OR 12-month interval → "annual"
            norm_interval = "annual" if (interval == "year" or interval_count == 12) else "monthly"
            new_price = normalized_monthly_price(unit_amount, norm_interval)

            logger.info(
                "  sub=%s tier=%s unit_amount=%s interval=%s/%s → plan_price=%.2f  [%s]",
                sub.id, sub.tier, unit_amount, interval, interval_count, new_price,
                "APPLY" if apply else "DRY-RUN",
            )

            if apply:
                sub.plan_price = new_price
                fixed += 1

        if apply:
            db.commit()
            logger.info("Committed. %d subscriber(s) updated.", fixed)
        else:
            logger.info("Dry-run complete — rerun with --apply to write.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes to DB")
    args = parser.parse_args()
    main(apply=args.apply)
