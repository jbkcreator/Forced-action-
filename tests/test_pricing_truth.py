"""
Unit tests for src/services/pricing_truth.py

All Stripe API calls and settings are mocked — no network, no DB required.

Run:
    python -m pytest tests/test_pricing_truth.py -q
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import stripe

from src.services.pricing_truth import check, _PRICE_TABLE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides):
    """Return a MagicMock mimicking AppSettings with all price IDs set."""
    s = MagicMock()
    # active_stripe_secret_key is a SecretStr mock
    sk = MagicMock()
    sk.get_secret_value.return_value = "sk_test_fake"
    s.active_stripe_secret_key = sk

    # Set every price attr in _PRICE_TABLE to a fake price_id by default.
    for attr, _surface, tier, _cents in _PRICE_TABLE:
        setattr(s, attr, f"price_{tier}")

    # Apply per-test overrides
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _stripe_price(unit_amount: int) -> dict:
    """Minimal Stripe Price object shape."""
    return {"object": "price", "unit_amount": unit_amount}


# ---------------------------------------------------------------------------
# All prices match → ok=True, mismatches=[]
# ---------------------------------------------------------------------------


def test_all_match():
    settings = _make_settings()

    # Build a retrieve side_effect that returns the correct unit_amount for
    # each price_id so every comparison passes.
    price_map = {f"price_{tier}": _stripe_price(cents) for _, _, tier, cents in _PRICE_TABLE}

    def fake_retrieve(price_id, api_key=None):
        return price_map[price_id]

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("src.services.pricing_truth.stripe.Price.retrieve", side_effect=fake_retrieve):
        result = check()

    assert result["ok"] is True
    assert result["mismatches"] == []


# ---------------------------------------------------------------------------
# One price disagrees → ok=False, mismatches contains correct detail
# ---------------------------------------------------------------------------


def test_single_mismatch():
    settings = _make_settings()

    # starter_founding is the first row in _PRICE_TABLE
    _attr, surface, tier, displayed_cents = _PRICE_TABLE[0]
    wrong_amount = displayed_cents + 500  # deliberately different

    price_map = {f"price_{t}": _stripe_price(c) for _, _, t, c in _PRICE_TABLE}
    price_map[f"price_{tier}"] = _stripe_price(wrong_amount)

    def fake_retrieve(price_id, api_key=None):
        return price_map[price_id]

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("src.services.pricing_truth.stripe.Price.retrieve", side_effect=fake_retrieve):
        result = check()

    assert result["ok"] is False
    assert len(result["mismatches"]) == 1
    mm = result["mismatches"][0]
    assert mm["surface"] == surface
    assert mm["tier"] == tier
    assert mm["displayed_cents"] == displayed_cents
    assert mm["stripe_cents"] == wrong_amount


# ---------------------------------------------------------------------------
# Stripe API raises an exception → ok=False, mismatches=[]  (fail closed)
# ---------------------------------------------------------------------------


def test_stripe_api_error_fails_closed():
    settings = _make_settings()

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch(
             "src.services.pricing_truth.stripe.Price.retrieve",
             side_effect=stripe.AuthenticationError("Invalid API key"),
         ):
        result = check()

    assert result["ok"] is False
    assert result["mismatches"] == []


# ---------------------------------------------------------------------------
# No Stripe key configured → ok=False, mismatches=[]
# ---------------------------------------------------------------------------


def test_no_api_key_fails_closed():
    settings = _make_settings()
    settings.active_stripe_secret_key = None

    with patch("src.services.pricing_truth.get_settings", return_value=settings):
        result = check()

    assert result["ok"] is False
    assert result["mismatches"] == []


# ---------------------------------------------------------------------------
# Price IDs that are None/empty in settings are silently skipped
# ---------------------------------------------------------------------------


def test_unconfigured_price_ids_are_skipped():
    settings = _make_settings()
    # Blank out all ICP prices
    for attr, surface, _tier, _cents in _PRICE_TABLE:
        if surface == "icp":
            setattr(settings, attr, None)

    # Only non-ICP prices are checked; they all match.
    price_map = {
        f"price_{tier}": _stripe_price(cents)
        for _, surface, tier, cents in _PRICE_TABLE
        if surface != "icp"
    }

    def fake_retrieve(price_id, api_key=None):
        return price_map[price_id]

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("src.services.pricing_truth.stripe.Price.retrieve", side_effect=fake_retrieve):
        result = check()

    assert result["ok"] is True
    assert result["mismatches"] == []


# ---------------------------------------------------------------------------
# Unexpected exception (non-Stripe) → fail closed
# ---------------------------------------------------------------------------


def test_unexpected_exception_fails_closed():
    with patch(
        "src.services.pricing_truth.get_settings",
        side_effect=RuntimeError("boom"),
    ):
        result = check()

    assert result["ok"] is False
    assert result["mismatches"] == []
