"""Founder checkout wiring (PR-2) — test-mode verification, no live charge.

Verifies the founder tier is purchasable through the checkout price-resolution
path without ever calling live Stripe:
  - get_price_id_for_checkout resolves the correct founder price per interval,
    straight from the seeded `plans` catalog.
  - the preview resolver returns the monthly founder price.
  - CheckoutRequest accepts tier='founder' and validates the interval field.
  - the subscribers.tier constraint now admits 'founder' (migration applied).

All DB work runs in the rolled-back `fresh_db` transaction.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from migrations.apply_founder_plan_seed import seed_founder_plans
from src.core.models import County, FoundingSubscriberCount

M_PRICE = "price_founder_monthly_TESTMODE"
A_PRICE = "price_founder_annual_TESTMODE"


@pytest.fixture
def founder_seeded(fresh_db):
    if fresh_db.execute(text("SELECT 1 FROM plans WHERE plan_id='pro'")).fetchone() is None:
        pytest.skip("no live `pro` plan to copy entitlements from")
    seed_founder_plans(fresh_db.connection(), M_PRICE, A_PRICE)
    return fresh_db


def test_checkout_resolves_monthly_founder_price(founder_seeded):
    from src.services.stripe_service import get_price_id_for_checkout
    price_id, is_founding = get_price_id_for_checkout(
        founder_seeded, "founder", "roofing", "hillsborough", "monthly"
    )
    assert price_id == M_PRICE
    assert is_founding is False


def test_checkout_resolves_annual_founder_price(founder_seeded):
    from src.services.stripe_service import get_price_id_for_checkout
    price_id, _ = get_price_id_for_checkout(
        founder_seeded, "founder", "roofing", "hillsborough", "annual"
    )
    assert price_id == A_PRICE


def test_checkout_defaults_to_monthly_when_interval_omitted(founder_seeded):
    from src.services.stripe_service import get_price_id_for_checkout
    price_id, _ = get_price_id_for_checkout(
        founder_seeded, "founder", "roofing", "hillsborough"
    )
    assert price_id == M_PRICE


def test_preview_resolves_founder_monthly(founder_seeded):
    from src.services.stripe_service import get_price_id_for_preview
    price_id, _ = get_price_id_for_preview(
        founder_seeded, "founder", "roofing", "hillsborough"
    )
    assert price_id == M_PRICE


def test_missing_founder_plan_raises(fresh_db):
    # No founder rows seeded -> resolution must raise a clear config error,
    # not silently return None.
    from src.services.stripe_service import get_price_id_for_checkout
    fresh_db.execute(text("DELETE FROM plans WHERE plan_id IN ('founder_monthly','founder_annual')"))
    with pytest.raises(ValueError):
        get_price_id_for_checkout(fresh_db, "founder", "roofing", "hillsborough", "monthly")


def test_checkout_uses_regular_price_when_founding_deadline_passed(fresh_db, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from src.services.stripe_service import get_price_id_for_checkout

    county_id = "deadline_test_county"
    fresh_db.add(
        County(
            county_id=county_id,
            display_name="Deadline Test County",
            founding_price_deadline_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
    )
    fresh_db.add(
        FoundingSubscriberCount(
            tier="starter",
            vertical="roofing",
            county_id=county_id,
            count=0,
        )
    )
    fresh_db.flush()

    monkeypatch.setattr("src.services.stripe_service._founding_limit", lambda: 10)
    monkeypatch.setattr(
        "src.services.stripe_service._price_ids",
        lambda: {
            "starter": {"founding": "price_founding_deadline", "regular": "price_regular_deadline"},
            "pro": {"founding": "price_pro_founding", "regular": "price_pro_regular"},
            "dominator": {"founding": "price_dom_founding", "regular": "price_dom_regular"},
            "partner": {"founding": "price_partner", "regular": "price_partner"},
            "annual_lock": {"founding": "price_annual_lock", "regular": "price_annual_lock"},
        },
    )

    price_id, is_founding = get_price_id_for_checkout(
        fresh_db, "starter", "roofing", county_id, "monthly"
    )

    assert price_id == "price_regular_deadline"
    assert is_founding is False


def test_checkout_request_accepts_founder_and_interval():
    from src.api.main import CheckoutRequest
    ten = [f"{33600 + i}" for i in range(10)]
    req = CheckoutRequest(tier="founder", vertical="roofing", county_id="hillsborough",
                          email="a@b.com", interval="annual", zip_codes=ten)
    assert req.tier == "founder"
    assert req.interval == "annual"


def test_checkout_request_rejects_bad_interval():
    from src.api.main import CheckoutRequest
    with pytest.raises(Exception):
        CheckoutRequest(tier="founder", vertical="roofing", county_id="hillsborough",
                        email="a@b.com", interval="weekly")


def test_annual_charge_normalized_to_monthly_mrr():
    # Review fix: an annual founder charge ($11,000 up front) must be stored as
    # a monthly run-rate in plan_price, not the raw 12-month charge.
    from src.services.stripe_webhooks import normalized_monthly_price
    # $11,000/yr -> ~$916.67/mo
    assert normalized_monthly_price(1100000, "annual") == round(1100000 / 100 / 12, 2)
    # monthly is unchanged
    assert normalized_monthly_price(110000, "monthly") == 1100.00
    # unknown/blank interval falls back to monthly (historical behavior)
    assert normalized_monthly_price(110000, "") == 1100.00


def test_founder_requires_exactly_ten_zips():
    from src.api.main import CheckoutRequest
    ten = [f"{33600 + i}" for i in range(10)]
    ok = CheckoutRequest(tier="founder", vertical="roofing", county_id="hillsborough",
                         email="a@b.com", zip_codes=ten)
    assert len(ok.zip_codes) == 10
    with pytest.raises(Exception):
        CheckoutRequest(tier="founder", vertical="roofing", county_id="hillsborough",
                        email="a@b.com", zip_codes=ten[:5])


def test_subscribers_tier_constraint_admits_founder(fresh_db):
    # Migration apply_founder_subscriber_tier added 'founder' to the CHECK.
    defn = fresh_db.execute(text(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conname = 'check_subscriber_tier'"
    )).scalar_one()
    assert "'founder'" in defn
