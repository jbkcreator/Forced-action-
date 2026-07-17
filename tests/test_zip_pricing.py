"""
B1-01 — ZIP → tier price.

Covers _cohort_adjusted_pricing() (src/api/main.py): falls back to the plain
Stripe base price when no cohort is active, and reflects an adjusted price
when pricing_cohort_engine.get_price_for_subscriber returns one.

Also covers /api/checkout: proves the Stripe checkout line-item amount matches
the cohort-adjusted amount /api/zip-check displayed, instead of silently
charging the fixed configured price_id (the bug this PR fixes).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_cohort_adjusted_pricing_falls_back_to_base_price():
    from src.api import main as api_main

    fake_base = {
        "starter": {"founding_amount": 600, "regular_amount": 800},
        "pro": {"founding_amount": 1100, "regular_amount": 1500},
        "dominator": {"founding_amount": 2000, "regular_amount": 2800},
        "annual_lock": {"founding_amount": None, "regular_amount": 1970},
    }

    with patch.object(api_main, "_cached_pricing_info", return_value=fake_base):
        with patch(
            "src.services.pricing_cohort_engine.get_price_for_subscriber",
            side_effect=lambda county, vertical, tier, cents, db: (cents, "base_price"),
        ):
            pricing = api_main._cohort_adjusted_pricing("hillsborough", "roofing", db=None)

    assert pricing["starter"]["founding_amount"] == 600
    assert pricing["starter"]["regular_amount"] == 800
    assert pricing["starter"]["price_source"] == "base_price"
    assert pricing["annual_lock"]["founding_amount"] is None
    assert pricing["annual_lock"]["regular_amount"] == 1970


def test_cohort_adjusted_pricing_reflects_active_cohort():
    """pricing_cohorts stores one FIXED price per (county, vertical, price_type) —
    not a percentage. get_price_for_subscriber ignores the base_price_cents it's
    given and returns that fixed cohort price whenever a cohort is active. This
    test's mock reflects that real behavior (constant return, independent of the
    cents passed in) — a mock that scales the input instead would mask the bug
    where founding_amount and regular_amount collapse to the same number.
    """
    from src.api import main as api_main

    fake_base = {
        "starter": {"founding_amount": 600, "regular_amount": 800},
        "pro": {"founding_amount": None, "regular_amount": None},
        "dominator": {"founding_amount": None, "regular_amount": None},
        "annual_lock": {"founding_amount": None, "regular_amount": None},
    }

    def fake_get_price(county, vertical, tier, cents, db):
        if tier == "starter":
            return 88000, "cohort_adjusted"  # fixed cohort price: $880, regardless of cents in
        return cents, "base_price"

    with patch.object(api_main, "_cached_pricing_info", return_value=fake_base):
        with patch(
            "src.services.pricing_cohort_engine.get_price_for_subscriber",
            side_effect=fake_get_price,
        ):
            pricing = api_main._cohort_adjusted_pricing("hillsborough", "roofing", db=None)

    # Regular anchors directly on the cohort's fixed price.
    assert pricing["starter"]["regular_amount"] == 880
    # Founding is scaled by the base 600/800 = 0.75 ratio, NOT equal to regular —
    # proves founding/regular don't collapse to the same number.
    assert pricing["starter"]["founding_amount"] == 660  # round(880 * 0.75)
    assert pricing["starter"]["founding_amount"] != pricing["starter"]["regular_amount"]
    assert pricing["starter"]["price_source"] == "cohort_adjusted"


def _checkout_client(db):
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db

    app.dependency_overrides[get_db] = lambda: db
    client = TestClient(app, raise_server_exceptions=False)
    return client


def _no_op_db():
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = None  # no existing subscriber
    db.execute.return_value.scalars.return_value.all.return_value = []  # no taken ZIPs
    return db


_CHECKOUT_PAYLOAD = {
    "tier": "starter",
    "vertical": "roofing",
    "county_id": "hillsborough",
    "zip_codes": ["33601"],
    "email": "buyer@example.com",
}


def test_checkout_charges_base_price_when_no_cohort_active():
    """No active cohort -> checkout must use the fixed Stripe price_id unchanged."""
    from src.api import main as api_main

    db = _no_op_db()
    client = _checkout_client(db)
    try:
        with patch.object(api_main, "get_price_id_for_checkout", return_value=("price_starter_regular", False)), \
             patch.object(api_main, "_cached_pricing_info", return_value={
                 "starter": {"founding_amount": 600, "regular_amount": 800},
             }), \
             patch(
                 "src.services.pricing_cohort_engine.get_price_for_subscriber",
                 return_value=(80000, "base_price"),
             ), \
             patch("src.services.checkout_recovery.start_recovery"), \
             patch.object(api_main.stripe.checkout.Session, "create") as mock_create:
            mock_create.return_value = MagicMock(client_secret="cs_test", id="sess_1", amount_total=80000)
            resp = client.post("/api/checkout", json=_CHECKOUT_PAYLOAD)

        assert resp.status_code == 200, resp.text
        line_items = mock_create.call_args.kwargs["line_items"]
        assert line_items == [{"price": "price_starter_regular", "quantity": 1}]
        assert mock_create.call_args.kwargs["metadata"]["price_source"] == "base_price"
    finally:
        api_main.app.dependency_overrides.clear()


def test_checkout_charges_cohort_adjusted_amount_matching_zip_check():
    """An active cohort must produce a Stripe line-item amount equal to the
    cohort-adjusted amount /api/zip-check would have displayed for this tier —
    proving checkout no longer silently charges the fixed configured price_id."""
    from src.api import main as api_main

    db = _no_op_db()
    client = _checkout_client(db)

    # Same cohort math as _cohort_adjusted_pricing: fixed cohort price anchored
    # on the regular amount, founding scaled by the base founding/regular ratio.
    fake_base = {"starter": {"founding_amount": 600, "regular_amount": 800}}
    displayed_regular_cents = 88000  # cohort's fixed $880 regular price

    try:
        with patch.object(api_main, "get_price_id_for_checkout", return_value=("price_starter_regular", False)), \
             patch.object(api_main, "_cached_pricing_info", return_value=fake_base), \
             patch(
                 "src.services.pricing_cohort_engine.get_price_for_subscriber",
                 return_value=(displayed_regular_cents, "cohort_adjusted"),
             ), \
             patch("src.services.checkout_recovery.start_recovery"), \
             patch.object(api_main.stripe.Price, "retrieve") as mock_retrieve, \
             patch.object(api_main.stripe.checkout.Session, "create") as mock_create:
            mock_retrieve.return_value = MagicMock(
                currency="usd", product="prod_starter",
                recurring=MagicMock(interval="month", interval_count=1),
            )
            mock_create.return_value = MagicMock(client_secret="cs_test", id="sess_2", amount_total=displayed_regular_cents)

            zip_check_pricing = api_main._cohort_adjusted_pricing("hillsborough", "roofing", db=db)
            resp = client.post("/api/checkout", json=_CHECKOUT_PAYLOAD)

        assert resp.status_code == 200, resp.text
        line_items = mock_create.call_args.kwargs["line_items"]
        charged_amount = line_items[0]["price_data"]["unit_amount"]

        # The amount actually charged must equal what /api/zip-check displayed.
        assert charged_amount == zip_check_pricing["starter"]["regular_amount"] * 100 == 88000
        assert line_items[0]["price_data"]["product"] == "prod_starter"
        assert mock_create.call_args.kwargs["metadata"]["price_source"] == "cohort_adjusted"
        assert mock_create.call_args.kwargs["metadata"]["resolved_amount_cents"] == "88000"
    finally:
        api_main.app.dependency_overrides.clear()


if __name__ == "__main__":
    test_cohort_adjusted_pricing_falls_back_to_base_price()
    test_cohort_adjusted_pricing_reflects_active_cohort()
    test_checkout_charges_base_price_when_no_cohort_active()
    test_checkout_charges_cohort_adjusted_amount_matching_zip_check()
    print("OK")
