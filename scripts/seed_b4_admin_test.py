"""Seed demo data for manually testing the B4 admin Lead-Delivery screen.

Creates 3 contractors (with company names) and a spread of deliveries across
grades and statuses so every filter on /admin/deliveries can be exercised.
Namespaced (cus_B4DEMO_* / B4DEMO-*) and removable.

    python -m scripts.seed_b4_admin_test            # cleanup + seed
    python -m scripts.seed_b4_admin_test --cleanup  # remove demo data
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import CustomerAccount, Delivery, Property

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("seed_b4_admin")

CUST_PREFIX = "cus_B4DEMO_"
PARCEL_PREFIX = "B4DEMO-"
_NOW = datetime.now(timezone.utc)

# (company, stripe_suffix, vertical, [(grade, status, reason, days_ago), ...])
_CONTRACTORS = [
    ("Acme Roofing", "acme", "roofing", [
        ("Ultra", "delivered", None, 1),
        ("Gold", "delivered", None, 2),
        ("Bronze", "rejected", "disconnected", 4),
    ]),
    ("BuildRight Restoration", "buildright", "restoration", [
        ("Platinum", "delivered", None, 1),
        ("Gold", "delivered", None, 3),
        ("Silver", "delivered", None, 6),
    ]),
    ("TopTier Exteriors", "toptier", "roofing", [
        ("Gold", "delivered", None, 2),
        ("Gold", "rejected", "wrong_party", 5),
    ]),
]


def seed(db) -> None:
    n_acct = n_deliv = 0
    for company, suffix, vertical, deliveries in _CONTRACTORS:
        acct = CustomerAccount(
            stripe_customer_id=f"{CUST_PREFIX}{suffix}",
            company_name=company, status="active",
            lead_entitlement={"ultra": 2, "platinum": 5, "gold": 20, "silver": 50, "bronze": 10},
            current_period_end=_NOW + timedelta(days=20),
        )
        db.add(acct)
        db.flush()
        n_acct += 1
        for i, (grade, status, reason, days_ago) in enumerate(deliveries):
            prop = Property(parcel_id=f"{PARCEL_PREFIX}{suffix}-{i}", zip="33601", county_id="hillsborough")
            db.add(prop)
            db.flush()
            db.add(Delivery(
                property_id=prop.id, account_id=acct.account_id, grade=grade, vertical=vertical,
                status=status, rejection_reason=reason,
                rejected_at=(_NOW - timedelta(days=days_ago)) if status == "rejected" else None,
                billing_period_end=_NOW + timedelta(days=20),
                delivered_at=_NOW - timedelta(days=days_ago), source="seed",
            ))
            n_deliv += 1
    db.commit()
    logger.info("seeded %d contractors, %d deliveries", n_acct, n_deliv)


def cleanup(db) -> None:
    db.execute(text(f"""DELETE FROM deliveries WHERE property_id IN
        (SELECT id FROM properties WHERE parcel_id LIKE '{PARCEL_PREFIX}%')"""))
    db.execute(text(f"DELETE FROM customer_accounts WHERE stripe_customer_id LIKE '{CUST_PREFIX}%'"))
    db.execute(text(f"DELETE FROM properties WHERE parcel_id LIKE '{PARCEL_PREFIX}%'"))
    db.commit()
    logger.info("removed B4 demo data")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()
    with get_db_context() as db:
        cleanup(db)
        if not args.cleanup:
            seed(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
