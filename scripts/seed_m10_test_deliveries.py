"""Seed M10 / B2 Lead Delivery test data and drive the full flow against the real
DB so it can be inspected by hand.

Namespaced for safety + trivial removal:
  * properties parcel_id -> M10SEED-*
  * Stripe customer ids  -> cus_M10SEED_*
  * test plan            -> m10seed_starter  (real free_trial/starter catalog untouched)

Drives the genuine M10 service (claim, reject_delivery) and the B1 conversion hook
(record_subscription_active -> free_to_paid attribution), committing each stage so
you can watch deliveries, credits, and attribution evolve.

    python -m scripts.seed_m10_test_deliveries            # cleanup, seed, drive, dump
    python -m scripts.seed_m10_test_deliveries --dump      # print current state
    python -m scripts.seed_m10_test_deliveries --cleanup   # remove all test data
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import CustomerAccount, Property, Subscriber, ZipTerritory
from src.services.lead_delivery import Lead, claim, reject_delivery
from src.services.revenue_engine import record_subscription_active

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("seed_m10")

PARCEL_PREFIX = "M10SEED-"
CUST = "cus_M10SEED_acme"
PLAN_ID = "m10seed_starter"
ZIP, COUNTY, VERT = "33601", "hillsborough", "roofing"
_PERIOD_END = datetime(2026, 7, 23, tzinfo=timezone.utc)


def ensure_plan(db) -> None:
    db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements)
        VALUES (:pid,'Starter (M10 TEST)','starter',29900,'monthly', CAST(:ent AS jsonb))
        ON CONFLICT (plan_id) DO UPDATE SET entitlements = EXCLUDED.entitlements
    """), {"pid": PLAN_ID, "ent": '{"gold": 2, "bronze": 5}'})
    db.commit()


def _property(db, suffix: str) -> Property:
    p = Property(parcel_id=f"{PARCEL_PREFIX}{suffix}", zip=ZIP, county_id=COUNTY)
    db.add(p)
    db.flush()
    return p


def _lead(prop, grade: str) -> Lead:
    return Lead(property_id=prop.id, zip_code=ZIP, county_id=COUNTY, grade=grade, verticals=[VERT])


def run_flow(db) -> None:
    # trial account that owns the ZIP for roofing
    sub = Subscriber(stripe_customer_id=CUST, tier="starter", vertical=VERT,
                     county_id=COUNTY, status="active", email="acme@m10seed.local")
    db.add(sub); db.flush()
    acct = CustomerAccount(stripe_customer_id=CUST, subscriber_id=sub.id, status="free_trial",
                           lead_entitlement={"bronze": 5}, current_period_end=None)
    db.add(acct); db.flush()
    db.add(ZipTerritory(zip_code=ZIP, vertical=VERT, county_id=COUNTY,
                        subscriber_id=sub.id, status="locked"))
    db.commit()
    logger.info("seeded trial account %s owning ZIP %s/%s", CUST, ZIP, VERT)

    # 1. free Bronze leads during trial
    for i in (1, 2):
        d = claim(db, _lead(_property(db, f"bronze{i}"), "Bronze"))
        db.commit()
        logger.info("  free Bronze lead -> delivery id=%s", d.id if d else None)

    # 2. convert to paid -> B1 hook writes first-touch attribution
    record_subscription_active(db, acct, plan_id=PLAN_ID, stripe_subscription_id="sub_M10SEED",
                               current_period_end=_PERIOD_END, stripe_event_id="evt_M10SEED")
    db.commit()
    logger.info("  converted to paid (%s) -> attribution recorded", PLAN_ID)

    # 3. paid Gold leads up to the gold:2 bucket, then exhausted
    g1 = claim(db, _lead(_property(db, "gold1"), "Gold")); db.commit()
    g2 = claim(db, _lead(_property(db, "gold2"), "Gold")); db.commit()
    g3 = claim(db, _lead(_property(db, "gold3"), "Gold")); db.commit()
    logger.info("  Gold deliveries: %s, %s; 3rd (exhausted) -> %s",
                g1.id if g1 else None, g2.id if g2 else None, g3.id if g3 else None)

    # 4. reject a Gold -> +1 credit -> one replacement gets through
    reject_delivery(db, g1.id, "disconnected"); db.commit()
    g4 = claim(db, _lead(_property(db, "gold4"), "Gold")); db.commit()
    logger.info("  rejected gold1 (disconnected) -> credit; replacement -> %s",
                g4.id if g4 else None)


def dump(db) -> None:
    rows = db.execute(text(f"""
        SELECT p.parcel_id, d.grade, d.status, d.rejection_reason, d.billing_period_end IS NOT NULL AS paid_cycle
        FROM deliveries d JOIN properties p ON p.id = d.property_id
        WHERE p.parcel_id LIKE '{PARCEL_PREFIX}%' ORDER BY d.id
    """)).fetchall()
    logger.info("-" * 70)
    logger.info("%-16s %-7s %-10s %-13s %s", "parcel", "grade", "status", "reason", "paid_cycle")
    for r in rows:
        logger.info("%-16s %-7s %-10s %-13s %s", r.parcel_id, r.grade, r.status,
                    r.rejection_reason or "-", r.paid_cycle)
    acct = db.execute(text("SELECT status, lead_entitlement, lead_credits FROM customer_accounts WHERE stripe_customer_id=:c"),
                      {"c": CUST}).fetchone()
    attr = db.execute(text("""
        SELECT free_leads_count, first_free_delivery_id, first_paid_plan
        FROM free_to_paid_attribution a JOIN customer_accounts ca ON ca.account_id=a.account_id
        WHERE ca.stripe_customer_id=:c
    """), {"c": CUST}).fetchone()
    logger.info("-" * 70)
    if acct:
        logger.info("account: status=%s entitlement=%s credits=%s", acct.status, acct.lead_entitlement, acct.lead_credits)
    if attr:
        logger.info("attribution: free_leads=%s first_touch_delivery=%s first_paid_plan=%s",
                    attr.free_leads_count, attr.first_free_delivery_id, attr.first_paid_plan)
    logger.info("-" * 70)


def cleanup(db) -> None:
    db.execute(text(f"""DELETE FROM free_to_paid_attribution WHERE account_id IN
        (SELECT account_id FROM customer_accounts WHERE stripe_customer_id = :c)"""), {"c": CUST})
    db.execute(text(f"""DELETE FROM deliveries WHERE property_id IN
        (SELECT id FROM properties WHERE parcel_id LIKE '{PARCEL_PREFIX}%')"""))
    db.execute(text(f"""DELETE FROM zip_territories WHERE subscriber_id IN
        (SELECT id FROM subscribers WHERE stripe_customer_id = :c)"""), {"c": CUST})
    db.execute(text(f"""DELETE FROM mrr_movements WHERE account_id IN
        (SELECT account_id FROM customer_accounts WHERE stripe_customer_id = :c)"""), {"c": CUST})
    db.execute(text("DELETE FROM customer_accounts WHERE stripe_customer_id = :c"), {"c": CUST})
    db.execute(text("DELETE FROM subscribers WHERE stripe_customer_id = :c"), {"c": CUST})
    db.execute(text(f"DELETE FROM properties WHERE parcel_id LIKE '{PARCEL_PREFIX}%'"))
    db.execute(text("DELETE FROM plans WHERE plan_id = :p"), {"p": PLAN_ID})
    db.commit()
    logger.info("cleaned up all M10 TEST data")


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed + exercise M10 Lead Delivery test data")
    ap.add_argument("--cleanup", action="store_true", help="remove all TEST data and exit")
    ap.add_argument("--dump", action="store_true", help="print current TEST state and exit")
    args = ap.parse_args()

    with get_db_context() as db:
        if args.cleanup:
            cleanup(db)
            return 0
        if args.dump:
            dump(db)
            return 0
        cleanup(db)
        ensure_plan(db)
        run_flow(db)
        dump(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
