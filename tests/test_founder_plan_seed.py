"""Founder-plan seed (founder-cohort task).

Drives `seed_founder_plans` against real Postgres inside the rolled-back
`fresh_db` transaction — reads the live `pro` row, seeds the two founder rows,
asserts shape, and leaves NO committed rows in the shared DB.

The B1-02 gate (200 for founder / 403 for lower) is NOT asserted here:
`entitlement_service.py` ships on PR #144, not yet on dev. That check is added
once #144 merges (see the skipped placeholder below).
"""
from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from migrations.apply_founder_plan_seed import (
    FOUNDER_MONTHLY_CENTS,
    seed_founder_plans,
)

MONTHLY_FAKE = "price_founder_monthly_TEST"
ANNUAL_FAKE = "price_founder_annual_TEST"


def _get_plan(db, plan_id):
    return db.execute(
        text("SELECT plan_id, tier, price_cents, interval, entitlements, stripe_price_id "
             "FROM plans WHERE plan_id = :p"),
        {"p": plan_id},
    ).mappings().fetchone()


@pytest.fixture
def seeded(fresh_db):
    # fresh_db skips if no live `pro` row exists (nothing to copy).
    if _get_plan(fresh_db, "pro") is None:
        pytest.skip("no live `pro` plan to copy entitlements from")
    conn = fresh_db.connection()
    seed_founder_plans(conn, MONTHLY_FAKE, ANNUAL_FAKE)
    return fresh_db


def test_both_founder_rows_seeded_with_founder_tier(seeded):
    for plan_id in ("founder_monthly", "founder_annual"):
        row = _get_plan(seeded, plan_id)
        assert row is not None, f"{plan_id} not seeded"
        assert row["tier"] == "founder"


def test_annual_is_ten_times_monthly(seeded):
    monthly = _get_plan(seeded, "founder_monthly")
    annual = _get_plan(seeded, "founder_annual")
    assert monthly["price_cents"] == FOUNDER_MONTHLY_CENTS
    assert annual["price_cents"] == FOUNDER_MONTHLY_CENTS * 10
    assert monthly["interval"] == "monthly"
    assert annual["interval"] == "annual"


def test_entitlements_copied_verbatim_from_pro(seeded):
    pro = _get_plan(seeded, "pro")
    monthly = _get_plan(seeded, "founder_monthly")
    assert monthly["entitlements"] == pro["entitlements"]


def test_stripe_price_ids_wired(seeded):
    assert _get_plan(seeded, "founder_monthly")["stripe_price_id"] == MONTHLY_FAKE
    assert _get_plan(seeded, "founder_annual")["stripe_price_id"] == ANNUAL_FAKE


def test_seed_is_idempotent(seeded):
    # Re-run on the same connection; UPSERT must not error or duplicate.
    seed_founder_plans(seeded.connection(), MONTHLY_FAKE, ANNUAL_FAKE)
    count = seeded.execute(
        text("SELECT count(*) FROM plans WHERE plan_id IN ('founder_monthly','founder_annual')")
    ).scalar()
    assert count == 2


def test_rerun_without_price_ids_preserves_existing(seeded):
    # Review fix #3: a rerun in an env missing the price vars (None) must NOT
    # null out the mapping already stored — COALESCE keeps it.
    seed_founder_plans(seeded.connection(), None, None)
    assert _get_plan(seeded, "founder_monthly")["stripe_price_id"] == MONTHLY_FAKE
    assert _get_plan(seeded, "founder_annual")["stripe_price_id"] == ANNUAL_FAKE


def test_price_resolves_the_correct_founder_interval(seeded):
    # Review fix #2: the two founder rows must be distinguishable by price id,
    # so checkout resolves the right interval (not an arbitrary tier LIMIT 1).
    from src.services.revenue_engine import plan_id_for_price
    assert plan_id_for_price(seeded, MONTHLY_FAKE) == "founder_monthly"
    assert plan_id_for_price(seeded, ANNUAL_FAKE) == "founder_annual"


@pytest.mark.skip(reason="entitlement_service ships on PR #144, not yet on dev")
def test_gate_200_for_founder_403_for_lower():
    """Once #144 merges: founder account -> 200 on founder-only surface,
    starter/pro account -> 403. Assert against TIER_RANK via the live gate."""
