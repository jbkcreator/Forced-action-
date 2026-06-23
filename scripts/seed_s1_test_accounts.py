"""Seed S1 Revenue Engine test subscribers and drive the full lifecycle against
the real DB so it can be inspected by hand.

Everything is namespaced so it is safe to run against the shared DB and trivial
to remove:
  * Stripe customer ids  ->  cus_S1TEST_*
  * test plans           ->  s1test_*  (the real free_trial/starter catalog is untouched)

It drives the SAME revenue_engine entrypoints the Stripe webhook handlers call
(plan_id_for_tier -> get_or_create_account -> record_subscription_active for
checkout; record_past_due / record_recovery; plan_id_for_price for upgrade;
record_churn for cancel), committing after each stage so you can watch the
customer_accounts row and the mrr_movements ledger evolve.

    python -m scripts.seed_s1_test_accounts            # cleanup, seed, lifecycle, dump
    python -m scripts.seed_s1_test_accounts --dump      # just print current TEST state
    python -m scripts.seed_s1_test_accounts --cleanup   # remove all TEST data and exit
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import CustomerAccount, Subscriber
from src.services.revenue_engine import (
    get_or_create_account,
    plan_id_for_price,
    plan_id_for_tier,
    record_churn,
    record_past_due,
    record_recovery,
    record_subscription_active,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("seed_s1_test_accounts")

CUST_PREFIX = "cus_S1TEST_"
PLAN_PREFIX = "s1test_"
_PERIOD_END = datetime(2026, 7, 23, tzinfo=timezone.utc)


# ── plan catalog (namespaced, removed on cleanup) ────────────────────────────

def ensure_test_plans(db) -> None:
    db.execute(text("""
        INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements, stripe_price_id)
        VALUES
          ('s1test_starter','Starter (S1 TEST)','s1test_starter',29900,'monthly', CAST(:s AS jsonb),'price_s1test_starter'),
          ('s1test_pro',    'Pro (S1 TEST)',    's1test_pro',    49900,'monthly', CAST(:p AS jsonb),'price_s1test_pro')
        ON CONFLICT (plan_id) DO UPDATE SET
          price_cents = EXCLUDED.price_cents,
          stripe_price_id = EXCLUDED.stripe_price_id
    """), {"s": '{"gold": 20}', "p": '{"gold": 50}'})
    db.commit()
    logger.info("ensured test plans: s1test_starter ($299), s1test_pro ($499)")


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_subscriber(db, suffix: str, *, status: str = "active") -> Subscriber:
    cust = f"{CUST_PREFIX}{suffix}"
    sub = Subscriber(
        stripe_customer_id=cust,
        stripe_subscription_id=f"sub_S1TEST_{suffix}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        status=status, email=f"{suffix}@s1test.local", name=f"S1 Test {suffix}",
    )
    db.add(sub)
    db.flush()
    return sub


def _checkout(db, suffix: str, plan_tier: str = "s1test_starter"):
    """Mirror the B1 block in _on_checkout_completed: create the paid Subscriber,
    bridge a CustomerAccount, record the 'new' MRR movement."""
    sub = _make_subscriber(db, suffix, status="active")
    plan_id = plan_id_for_tier(db, plan_tier)
    account = get_or_create_account(
        db, stripe_customer_id=sub.stripe_customer_id, subscriber_id=sub.id,
    )
    record_subscription_active(
        db, account,
        plan_id=plan_id,
        stripe_subscription_id=sub.stripe_subscription_id,
        current_period_end=_PERIOD_END,
        stripe_event_id=f"checkout:{sub.stripe_subscription_id}",
    )
    return sub, account


# ── standing accounts: one per end-state, for side-by-side inspection ────────

def seed_standing_accounts(db) -> None:
    # trial — account exists, never converted
    sub = _make_subscriber(db, "trial", status="active")
    get_or_create_account(db, stripe_customer_id=sub.stripe_customer_id, subscriber_id=sub.id)
    db.commit()
    logger.info("seeded standing: %strial (free_trial, $0)", CUST_PREFIX)

    # active — paid starter
    _checkout(db, "active")
    db.commit()
    logger.info("seeded standing: %sactive (active, $299)", CUST_PREFIX)

    # past_due — paid then failed invoice
    _, acct = _checkout(db, "pastdue")
    record_past_due(db, acct)
    db.commit()
    logger.info("seeded standing: %spastdue (past_due, mrr retained)", CUST_PREFIX)

    # churned — paid then cancelled
    _, acct = _checkout(db, "churned")
    record_churn(db, acct, stripe_event_id="subdel:sub_S1TEST_churned", effective_at=datetime.now(timezone.utc))
    db.commit()
    logger.info("seeded standing: %schurned (churned, mrr=0)", CUST_PREFIX)


# ── full lifecycle on a single customer, stage by stage ──────────────────────

def run_lifecycle(db) -> None:
    suffix = "life"
    logger.info("-" * 60)
    logger.info("LIFECYCLE: %s%s", CUST_PREFIX, suffix)

    sub, acct = _checkout(db, suffix)
    db.commit()
    _print_account(db, acct.account_id, "1. checkout (starter)")

    sub.payment_failed_at = datetime.now(timezone.utc)
    record_past_due(db, acct)
    db.commit()
    _print_account(db, acct.account_id, "2. invoice.payment_failed")

    record_recovery(db, acct)
    db.commit()
    _print_account(db, acct.account_id, "3. invoice.payment_succeeded (recovery)")

    pro_plan = plan_id_for_price(db, "price_s1test_pro")
    record_subscription_active(
        db, acct, plan_id=pro_plan,
        stripe_subscription_id=sub.stripe_subscription_id,
        current_period_end=_PERIOD_END,
        stripe_event_id=f"subupd:{sub.stripe_subscription_id}:{pro_plan}",
    )
    db.commit()
    _print_account(db, acct.account_id, "4. subscription.updated (upgrade -> pro)")

    record_churn(
        db, acct,
        stripe_event_id=f"subdel:{sub.stripe_subscription_id}",
        effective_at=datetime.now(timezone.utc),
    )
    db.commit()
    _print_account(db, acct.account_id, "5. subscription.deleted (cancel)")

    total = db.execute(
        text("SELECT COALESCE(SUM(delta_cents),0) FROM mrr_movements WHERE account_id = :a"),
        {"a": str(acct.account_id)},
    ).scalar()
    logger.info("LIFECYCLE net MRR delta = %d cents (expect 0 - reconciles)", total)


def _print_account(db, account_id, label: str) -> None:
    acct = db.execute(
        text("SELECT status, plan_tier, mrr_cents FROM customer_accounts WHERE account_id = :a"),
        {"a": str(account_id)},
    ).fetchone()
    moves = db.execute(
        text("""SELECT movement_type, delta_cents, mrr_after_cents, is_involuntary
                FROM mrr_movements WHERE account_id = :a ORDER BY id"""),
        {"a": str(account_id)},
    ).fetchall()
    logger.info(
        "  %-42s status=%-9s plan=%-14s mrr=%d", label, acct.status, acct.plan_tier, acct.mrr_cents
    )
    for m in moves:
        flag = " involuntary" if m.is_involuntary else ""
        logger.info("       movement: %-12s delta=%+7d after=%6d%s",
                    m.movement_type, m.delta_cents, m.mrr_after_cents, flag)


# ── dump + cleanup ────────────────────────────────────────────────────────────

def dump(db) -> None:
    rows = db.execute(text(f"""
        SELECT ca.stripe_customer_id, ca.status, ca.plan_tier, ca.mrr_cents,
               ca.subscriber_id, s.status AS sub_status,
               (SELECT count(*) FROM mrr_movements m WHERE m.account_id = ca.account_id) AS movements
        FROM customer_accounts ca
        LEFT JOIN subscribers s ON s.id = ca.subscriber_id
        WHERE ca.stripe_customer_id LIKE '{CUST_PREFIX}%'
        ORDER BY ca.stripe_customer_id
    """)).fetchall()
    logger.info("-" * 78)
    logger.info("%-22s %-9s %-15s %7s %10s %8s", "customer", "status", "plan", "mrr", "sub.status", "moves")
    logger.info("-" * 78)
    for r in rows:
        logger.info("%-22s %-9s %-15s %7d %10s %8d",
                    r.stripe_customer_id, r.status, r.plan_tier or "-", r.mrr_cents,
                    r.sub_status or "-", r.movements)
    logger.info("-" * 78)
    logger.info("%d TEST account(s). Inspect with:", len(rows))
    logger.info("  SELECT * FROM customer_accounts WHERE stripe_customer_id LIKE '%s%%';", CUST_PREFIX)
    logger.info("  SELECT * FROM mrr_movements WHERE stripe_event_id LIKE '%%S1TEST%%' OR stripe_event_id LIKE 'checkout:sub_S1TEST%%';")


def cleanup(db) -> None:
    db.execute(text(f"""
        DELETE FROM mrr_movements WHERE account_id IN (
            SELECT account_id FROM customer_accounts WHERE stripe_customer_id LIKE '{CUST_PREFIX}%'
        )
    """))
    db.execute(text(f"DELETE FROM customer_accounts WHERE stripe_customer_id LIKE '{CUST_PREFIX}%'"))
    db.execute(text(f"DELETE FROM subscribers WHERE stripe_customer_id LIKE '{CUST_PREFIX}%'"))
    db.execute(text(f"DELETE FROM plans WHERE plan_id LIKE '{PLAN_PREFIX}%'"))
    db.commit()
    logger.info("cleaned up all S1 TEST data (%s* customers, %s* plans)", CUST_PREFIX, PLAN_PREFIX)


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed + exercise S1 Revenue Engine test data")
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

        cleanup(db)            # idempotent re-run from a clean slate
        ensure_test_plans(db)
        seed_standing_accounts(db)
        run_lifecycle(db)
        dump(db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
