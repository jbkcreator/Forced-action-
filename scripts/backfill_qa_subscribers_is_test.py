"""Backfill is_test=TRUE on QA subscriber rows and cancel their Stripe subscriptions.

Identifies QA rows by email/customer-id patterns known from pre-launch testing:
  - email contains lesly.vj+, @example.com
  - stripe_customer_id starts with cus_test_ or sub_test_

Run with --dry-run first to preview. Add --apply to execute.

Usage:
    PYTHONPATH=. python scripts/backfill_qa_subscribers_is_test.py --dry-run
    PYTHONPATH=. python scripts/backfill_qa_subscribers_is_test.py --apply
"""
from __future__ import annotations

import argparse
import logging
import sys

import stripe
from sqlalchemy import text

from config.settings import get_settings
from src.core.database import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

QA_EMAIL_PATTERNS = ["lesly.vj+", "@example.com"]
QA_CUSTOMER_PREFIXES = ["cus_test_", "sub_test_"]


def find_qa_rows(db):
    email_conditions = " OR ".join(
        f"email ILIKE :pat{i}" for i in range(len(QA_EMAIL_PATTERNS))
    )
    cid_conditions = " OR ".join(
        f"stripe_customer_id ILIKE :cid{i}" for i in range(len(QA_CUSTOMER_PREFIXES))
    )
    params = {f"pat{i}": f"%{p}%" for i, p in enumerate(QA_EMAIL_PATTERNS)}
    params.update({f"cid{i}": f"{p}%" for i, p in enumerate(QA_CUSTOMER_PREFIXES)})

    rows = db.execute(text(f"""
        SELECT id, email, stripe_customer_id, stripe_subscription_id, status, is_test
        FROM subscribers
        WHERE ({email_conditions} OR {cid_conditions})
          AND is_test = FALSE
    """), params).fetchall()
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--apply", action="store_true", default=False)
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Pass --dry-run or --apply", file=sys.stderr)
        sys.exit(1)

    settings = get_settings()
    if settings.active_stripe_secret_key:
        stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

    db = SessionLocal()
    try:
        rows = find_qa_rows(db)
        logger.info("Found %d QA subscriber rows with is_test=FALSE", len(rows))

        for r in rows:
            logger.info(
                "  id=%s email=%s customer=%s sub=%s status=%s",
                r.id, r.email, r.stripe_customer_id, r.stripe_subscription_id, r.status,
            )

        if args.dry_run:
            logger.info("Dry run — no changes made")
            return

        for r in rows:
            # Cancel Stripe subscription if active
            if r.stripe_subscription_id and r.status not in ("cancelled", "churned"):
                try:
                    stripe.Subscription.cancel(r.stripe_subscription_id)
                    logger.info("Cancelled Stripe subscription %s", r.stripe_subscription_id)
                except stripe.error.InvalidRequestError as exc:
                    logger.warning("Stripe cancel failed for %s: %s", r.stripe_subscription_id, exc)

            # Mark is_test + cancelled in DB
            db.execute(text("""
                UPDATE subscribers
                SET is_test = TRUE, status = 'cancelled'
                WHERE id = :id
            """), {"id": r.id})

        db.commit()
        logger.info("Backfill complete — %d rows updated", len(rows))
    finally:
        db.close()


if __name__ == "__main__":
    main()
