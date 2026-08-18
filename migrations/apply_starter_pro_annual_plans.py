"""Seed `starter_annual` and `pro_annual` plans (§3.2 annual SKUs).

Checkout now accepts the new annual Stripe price IDs for starter/pro, but the
webhook resolves a plan by Stripe price id (`plan_id_for_price`) and falls back
to the tier when unmapped. Without these rows an annual starter/pro subscription
falls back to the *monthly* `starter`/`pro` plan, recording monthly MRR, a
monthly interval, and monthly plan metadata for a yearly subscription.

Design (mirrors migrations/apply_founder_plan_seed.py):
- Entitlements copied VERBATIM from the matching monthly plan — an annual buyer
  gets the same features as monthly, never fewer.
- Annual price_cents = monthly x 10 (Annual Prepay = two months free); this
  matches the live Stripe prices ($2,990/yr starter, $4,990/yr pro). MRR is
  normalized to price_cents//12 by revenue_engine.normalize_mrr_cents.
- Stripe price IDs come from settings (STRIPE_PRICE_STARTER_ANNUAL /
  STRIPE_PRICE_PRO_ANNUAL via active_stripe_price). A missing id seeds NULL so
  the plan exists (price->plan resolution ready) without clobbering a live id.
- Idempotent UPSERT on plan_id — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_starter_pro_annual_plans.py
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ANNUAL_MULTIPLIER = 10  # two months free

_SELECT_MONTHLY = text(
    "SELECT price_cents, entitlements FROM plans "
    "WHERE plan_id = :tier AND is_active = true LIMIT 1"
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
        -- Never null out a live price mapping on a rerun in an env missing the var.
        stripe_price_id = COALESCE(EXCLUDED.stripe_price_id, plans.stripe_price_id),
        updated_at = now()
""")

_TIERS = [
    ("starter", "Starter (Annual)"),
    ("pro", "Pro (Annual)"),
]


def seed_annual_plans(conn, price_ids: dict[str, str | None]) -> None:
    """Seed starter_annual + pro_annual on the given connection.

    Copies each tier's monthly entitlements and price verbatim. Raises if a
    monthly source plan is missing. Connection-scoped for rolled-back tests.
    """
    for tier, name in _TIERS:
        row = conn.execute(_SELECT_MONTHLY, {"tier": tier}).fetchone()
        if row is None:
            raise RuntimeError(f"no active `{tier}` plan found — cannot copy entitlements")
        monthly_cents, entitlements = int(row[0]), json.dumps(row[1])
        conn.execute(_UPSERT, {
            "plan_id": f"{tier}_annual",
            "name": name,
            "tier": tier,
            "price_cents": monthly_cents * _ANNUAL_MULTIPLIER,
            "interval": "annual",
            "entitlements": entitlements,
            "stripe_price_id": price_ids.get(tier),
        })


def main() -> int:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    price_ids = {t: settings.active_stripe_price(f"{t}_annual") for t, _ in _TIERS}

    with engine.begin() as conn:
        try:
            seed_annual_plans(conn, price_ids)
        except RuntimeError as exc:
            logger.error("%s; aborting", exc)
            return 1

    for tier, _ in _TIERS:
        if not price_ids.get(tier):
            logger.warning(
                "STRIPE_PRICE_%s_ANNUAL not set — %s_annual.stripe_price_id is NULL "
                "(webhook price->plan resolution blocked for this SKU)",
                tier.upper(), tier,
            )
    logger.info("seeded plans: starter_annual, pro_annual")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
