"""
B1-01 — ZIP → tier price.

Covers _cohort_adjusted_pricing() (src/api/main.py): falls back to the plain
Stripe base price when no cohort is active, and reflects an adjusted price
when pricing_cohort_engine.get_price_for_subscriber returns one.
"""

from __future__ import annotations

from unittest.mock import patch


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
    from src.api import main as api_main

    fake_base = {
        "starter": {"founding_amount": 600, "regular_amount": 800},
        "pro": {"founding_amount": None, "regular_amount": None},
        "dominator": {"founding_amount": None, "regular_amount": None},
        "annual_lock": {"founding_amount": None, "regular_amount": None},
    }

    def fake_get_price(county, vertical, tier, cents, db):
        if tier == "starter":
            return int(cents * 1.10), "cohort_adjusted"
        return cents, "base_price"

    with patch.object(api_main, "_cached_pricing_info", return_value=fake_base):
        with patch(
            "src.services.pricing_cohort_engine.get_price_for_subscriber",
            side_effect=fake_get_price,
        ):
            pricing = api_main._cohort_adjusted_pricing("hillsborough", "roofing", db=None)

    assert pricing["starter"]["founding_amount"] == 660  # +10%
    assert pricing["starter"]["price_source"] == "cohort_adjusted"


if __name__ == "__main__":
    test_cohort_adjusted_pricing_falls_back_to_base_price()
    test_cohort_adjusted_pricing_reflects_active_cohort()
    print("OK")
