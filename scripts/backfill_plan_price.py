"""
Backfill / reconcile plan_price for active non-free subscribers against Stripe.

Looks up each subscriber's current Stripe subscription, reads the recurring price
unit_amount, normalises to a monthly run-rate, and writes plan_price when it is
NULL *or* disagrees with Stripe. The drift case matters: a discounted or prorated
first charge can persist a wrong run-rate (e.g. a $10 founding first month stored
against a $299/mo plan), which never shows up in a NULL-only backfill.

Usage:
    PYTHONPATH=. python scripts/backfill_plan_price.py               # dry-run, NULL + drift
    PYTHONPATH=. python scripts/backfill_plan_price.py --apply       # write to DB
    PYTHONPATH=. python scripts/backfill_plan_price.py --nulls-only  # legacy: NULL-fill only
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


def main(apply: bool, nulls_only: bool) -> None:
    settings = get_settings()
    stripe.api_key = settings.stripe_secret_key.get_secret_value()

    with Database().session_scope() as db:
        conditions = [
            Subscriber.tier.notin_(["free"]),
            Subscriber.status.in_(["active", "grace"]),
            Subscriber.is_test.is_(False),
        ]
        if nulls_only:
            conditions.append(Subscriber.plan_price.is_(None))

        rows = db.execute(select(Subscriber).where(*conditions)).scalars().all()

        if not rows:
            logger.info("No candidate subscribers found — nothing to do.")
            return

        logger.info(
            "Checking %d active non-free non-test subscriber(s) (%s):",
            len(rows), "NULL only" if nulls_only else "NULL + drift",
        )

        filled = 0
        corrected = 0
        for sub in rows:
            if not sub.stripe_subscription_id:
                if sub.plan_price is None:
                    logger.warning("  SKIP sub=%s — NULL plan_price and no stripe_subscription_id", sub.id)
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

            current = float(sub.plan_price) if sub.plan_price is not None else None
            is_null = current is None
            is_drift = current is not None and abs(current - new_price) >= 0.01
            if not (is_null or is_drift):
                continue  # already correct

            kind = "NULL-FILL" if is_null else "DRIFT-FIX"
            logger.info(
                "  %s sub=%s email=%s tier=%s current=%s stripe=%s/%s → plan_price=%.2f  [%s]",
                kind, sub.id, sub.email, sub.tier,
                "NULL" if is_null else f"{current:.2f}",
                interval, interval_count, new_price,
                "APPLY" if apply else "DRY-RUN",
            )

            if apply:
                sub.plan_price = new_price
                if is_null:
                    filled += 1
                else:
                    corrected += 1

        if apply:
            db.commit()
            logger.info("Committed. %d NULL-filled, %d drift-corrected.", filled, corrected)
        else:
            logger.info("Dry-run complete — rerun with --apply to write.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes to DB")
    parser.add_argument(
        "--nulls-only", action="store_true",
        help="Legacy behavior: only fill NULL plan_price, skip drift correction",
    )
    args = parser.parse_args()
    main(apply=args.apply, nulls_only=args.nulls_only)
