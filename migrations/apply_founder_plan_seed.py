"""Seed the `founder` entitlement tier (founder-cohort plan).

B1-02's `entitlement_service.TIER_RANK` reserves `founder` at rank 30 but no
live `plans` row reaches it, so the gate fails closed. This seeds two rows —
`founder_monthly` and `founder_annual` — both `tier='founder'` so the gate lifts.

Design (grill 2026-07-17, ADR 0033):
- Entitlements are copied VERBATIM from the live `pro` plan so a founder is
  never *less* entitled than pro. Founder-exclusive perks are a later task.
- Annual price_cents = monthly x 10 (Annual Prepay = 2 months free).
- Stripe price IDs come from settings (STRIPE_PRICE_FOUNDER_MONTHLY /
  STRIPE_PRICE_FOUNDER_ANNUAL). If a price ID is missing, the row is still
  seeded with a NULL stripe_price_id — the tier gate works (rank reached), but
  purchase/switch on that interval stays blocked until the founder delivers it.
- `Plan.tier` is free-text (no CheckConstraint), so no schema change is needed.
- Idempotent UPSERT on plan_id — safe to re-run once real price IDs arrive.

Usage:
    PYTHONPATH=. python migrations/apply_founder_plan_seed.py

BLOCKED on: real founder Stripe price IDs (monthly + annual) from the client.
Until then run seeds NULL price IDs (gate works, checkout does not).
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Monthly founder price in cents ($1,100/mo, client-confirmed 2026-07-20).
# Annual = x10 ($11,000/yr, two months free).
FOUNDER_MONTHLY_CENTS = 110000

_SELECT_PRO_ENTITLEMENTS = text(
    "SELECT entitlements FROM plans WHERE plan_id = 'pro' AND is_active = true LIMIT 1"
)

_UPSERT = text("""
    INSERT INTO plans (plan_id, name, tier, price_cents, interval, entitlements, stripe_price_id)
    VALUES (:plan_id, :name, :tier, :price_cents, :interval, CAST(:entitlements AS jsonb), :stripe_price_id)
    ON CONFLICT (plan_id) DO UPDATE SET
        name = EXCLUDED.name,
        tier = EXCLUDED.tier,
        price_cents = EXCLUDED.price_cents,
        interval = EXCLUDED.interval,
        entitlements = EXCLUDED.entitlements,
        -- Never null out a live price mapping: a rerun in an environment that
        -- is missing the env var passes NULL, and overwriting would break
        -- checkout/webhook price->plan resolution. Keep the existing id.
        stripe_price_id = COALESCE(EXCLUDED.stripe_price_id, plans.stripe_price_id),
        updated_at = now()
""")


def seed_founder_plans(conn, monthly_price, annual_price) -> None:
    """Seed founder_monthly + founder_annual on the given connection.

    Reads `pro`'s entitlements and copies them verbatim. Raises if no active
    `pro` plan exists. Connection-scoped so tests can drive it in a rolled-back
    transaction.
    """
    import json

    row = conn.execute(_SELECT_PRO_ENTITLEMENTS).fetchone()
    if row is None:
        raise RuntimeError("no active `pro` plan found — cannot copy entitlements")
    pro_entitlements = json.dumps(row[0])

    plans = [
        {
            "plan_id": "founder_monthly", "name": "Founder (Monthly)", "tier": "founder",
            "price_cents": FOUNDER_MONTHLY_CENTS, "interval": "monthly",
            "entitlements": pro_entitlements, "stripe_price_id": monthly_price,
        },
        {
            # Annual Prepay = monthly x 10 (two months free).
            "plan_id": "founder_annual", "name": "Founder (Annual)", "tier": "founder",
            "price_cents": FOUNDER_MONTHLY_CENTS * 10, "interval": "annual",
            "entitlements": pro_entitlements, "stripe_price_id": annual_price,
        },
    ]
    for p in plans:
        conn.execute(_UPSERT, p)


def main() -> int:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    monthly_price = settings.stripe_price_founder_monthly
    annual_price = settings.stripe_price_founder_annual

    with engine.begin() as conn:
        try:
            seed_founder_plans(conn, monthly_price, annual_price)
        except RuntimeError as exc:
            logger.error("%s; aborting", exc)
            return 1

    if not monthly_price:
        logger.warning("STRIPE_PRICE_FOUNDER_MONTHLY not set — founder_monthly.stripe_price_id is NULL (checkout blocked)")
    if not annual_price:
        logger.warning("STRIPE_PRICE_FOUNDER_ANNUAL not set — founder_annual.stripe_price_id is NULL (#148 annual switch blocked)")
    logger.info("seeded plans: founder_monthly, founder_annual (tier=founder)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
