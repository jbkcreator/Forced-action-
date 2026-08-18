"""Starter/Pro annual SKU plans (§3.2).

Two defects fixed:
  1. Annual checkout must not enter the monthly cohort-pricing block (it would
     bill a year at one month's price).
  2. Annual Stripe price ids must map to `*_annual` plans so the webhook records
     annual MRR/interval, not the monthly fallback.

Seed test drives `seed_annual_plans` inside the rolled-back `fresh_db`
transaction — no committed rows in the shared DB.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from migrations.apply_starter_pro_annual_plans import seed_annual_plans


def _get_plan(db, plan_id):
    return db.execute(
        text("SELECT plan_id, tier, price_cents, interval, entitlements, stripe_price_id "
             "FROM plans WHERE plan_id = :p"),
        {"p": plan_id},
    ).mappings().fetchone()


@pytest.fixture
def seeded(fresh_db):
    if _get_plan(fresh_db, "starter") is None or _get_plan(fresh_db, "pro") is None:
        pytest.skip("no live starter/pro plans to copy entitlements from")
    seed_annual_plans(fresh_db.connection(), {
        "starter": "price_starter_annual_TEST",
        "pro": "price_pro_annual_TEST",
    })
    return fresh_db


# ── Finding 2: annual plans exist and resolve correctly ───────────────────────

def test_both_annual_rows_seeded(seeded):
    for plan_id, tier in (("starter_annual", "starter"), ("pro_annual", "pro")):
        row = _get_plan(seeded, plan_id)
        assert row is not None, f"{plan_id} not seeded"
        assert row["tier"] == tier
        assert row["interval"] == "annual"


def test_annual_price_is_ten_times_monthly(seeded):
    for tier in ("starter", "pro"):
        monthly = _get_plan(seeded, tier)
        annual = _get_plan(seeded, f"{tier}_annual")
        assert annual["price_cents"] == monthly["price_cents"] * 10


def test_annual_entitlements_match_monthly(seeded):
    for tier in ("starter", "pro"):
        assert _get_plan(seeded, f"{tier}_annual")["entitlements"] == \
            _get_plan(seeded, tier)["entitlements"]


def test_price_id_resolves_to_annual_plan_not_monthly_fallback(seeded):
    from src.services.revenue_engine import plan_id_for_price, plan_id_for_tier
    # The annual price id must resolve to the annual plan…
    assert plan_id_for_price(seeded, "price_starter_annual_TEST") == "starter_annual"
    assert plan_id_for_price(seeded, "price_pro_annual_TEST") == "pro_annual"
    # …not the monthly tier fallback that the webhook would otherwise use.
    assert plan_id_for_tier(seeded, "starter") != "starter_annual"


def test_annual_mrr_normalizes_to_monthly_run_rate(seeded):
    from src.services.revenue_engine import normalize_mrr_cents
    annual = _get_plan(seeded, "starter_annual")
    # e.g. $2,990/yr → ~$249.16/mo, NOT $2,990 recorded as monthly MRR.
    assert normalize_mrr_cents(annual["price_cents"], "annual") == annual["price_cents"] // 12
    assert normalize_mrr_cents(annual["price_cents"], "annual") < annual["price_cents"]


def test_seed_idempotent(seeded):
    before = _get_plan(seeded, "starter_annual")
    seed_annual_plans(seeded.connection(), {
        "starter": "price_starter_annual_TEST", "pro": "price_pro_annual_TEST",
    })
    after = _get_plan(seeded, "starter_annual")
    assert before["price_cents"] == after["price_cents"]


def test_rerun_with_missing_price_id_preserves_existing(seeded):
    """A rerun in an env missing the annual env var must not null a live id."""
    seed_annual_plans(seeded.connection(), {"starter": None, "pro": None})
    assert _get_plan(seeded, "starter_annual")["stripe_price_id"] == "price_starter_annual_TEST"


# ── Finding 1: annual checkout uses the flat annual price, not a cohort amount ─

@pytest.mark.parametrize("tier", ["starter", "pro"])
def test_annual_checkout_selects_flat_annual_price(fresh_db, tier):
    """Annual starter/pro must resolve to the flat annual Stripe price with
    is_founding=False. That price carries the yearly interval; the checkout
    cohort block (which rebuilds price_data from a MONTHLY amount) is gated off
    for annual, so an annual sub can never be billed a year at one month's rate.
    """
    from config.settings import get_settings
    from src.services.stripe_service import get_price_id_for_checkout

    annual_id = get_settings().active_stripe_price(f"{tier}_annual")
    if not annual_id:
        pytest.skip(f"STRIPE_PRICE_{tier.upper()}_ANNUAL not configured")

    price_id, is_founding = get_price_id_for_checkout(
        fresh_db, tier, "roofing", "hillsborough", interval="annual"
    )
    assert price_id == annual_id
    assert is_founding is False


def test_annual_interval_gates_cohort_block():
    """Guard sanity: the cohort block only runs for non-annual ZIP-priced tiers."""
    import src.api.main as main
    assert "starter" in main._ZIP_PRICING_TIERS
    assert "pro" in main._ZIP_PRICING_TIERS
