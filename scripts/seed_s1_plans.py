"""Seed the S1 plan catalog (B1 / M9): free_trial + starter.

Idempotent UPSERT so price/entitlement edits re-seed cleanly (config-over-code).
starter reuses the existing Stripe price (STRIPE_PRICE_STARTER_REGULAR).

    python -m scripts.seed_s1_plans
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("seed_s1_plans")

_UPSERT = text("""
    INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements, stripe_price_id)
    VALUES (:plan_id, :name, :tier, :price_cents, :interval, CAST(:entitlements AS jsonb), :stripe_price_id)
    ON CONFLICT (plan_id) DO UPDATE SET
        name = EXCLUDED.name,
        tier = EXCLUDED.tier,
        price_cents = EXCLUDED.price_cents,
        interval = EXCLUDED.interval,
        entitlements = EXCLUDED.entitlements,
        stripe_price_id = EXCLUDED.stripe_price_id,
        updated_at = now()
""")


def main() -> int:
    settings = get_settings()
    starter_price_id = settings.stripe_price_starter_regular

    # Entitlement buckets are keyed by M6 verdict grade, lowercased (grade_key):
    # ultra | platinum | gold | silver | bronze. 'sub_grade' has no bucket — M6 has
    # already marked it dead, so M10 never delivers it. Counts below are PLACEHOLDERS
    # to open the full grade pipe end-to-end (every grade has a jar to deliver into);
    # product sets the real per-tier allowances later — config-over-code, no code change.
    plans = [
        {
            # Free trial = the free Bronze hand-off only (M6 free_hand_delivered → Bronze).
            "plan_id": "free_trial", "name": "Free Trial", "tier": "free_trial",
            "price_cents": 0, "interval": "trial",
            "entitlements": '{"bronze": 5}', "stripe_price_id": None,
        },
        {
            "plan_id": "starter", "name": "Starter", "tier": "starter",
            "price_cents": 29900, "interval": "monthly",
            "entitlements": '{"ultra": 2, "platinum": 5, "gold": 20, "silver": 50, "bronze": 10}',
            "stripe_price_id": starter_price_id,
        },
    ]

    with get_db_context() as db:
        for p in plans:
            db.execute(_UPSERT, p)
        db.commit()

    if not starter_price_id:
        logger.warning("STRIPE_PRICE_STARTER_REGULAR not set — starter.stripe_price_id is NULL")
    logger.info("seeded plans: free_trial, starter")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
