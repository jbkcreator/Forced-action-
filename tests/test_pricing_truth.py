"""
Unit tests for src/services/pricing_truth.py (advisory config diagnostic).

All Stripe API calls and settings are mocked — no network, no DB required.

Run:
    python -m pytest tests/test_pricing_truth.py -q
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import stripe

from src.services.pricing_truth import check, _PRICE_TABLE


def _make_settings(price_map=None, no_key=False):
    """Return a MagicMock mimicking AppSettings.

    price_map: optional {name -> price_id} override. By default every price
    name resolves to f'price_{name}'.
    """
    s = MagicMock()
    if no_key:
        s.active_stripe_secret_key = None
    else:
        sk = MagicMock()
        sk.get_secret_value.return_value = "sk_test_fake"
        s.active_stripe_secret_key = sk

    resolved = {name: f"price_{name}" for name, _, _ in _PRICE_TABLE}
    if price_map is not None:
        resolved.update(price_map)

    s.active_stripe_price = lambda name: resolved.get(name)
    s.active_hold_deposit_price_id = resolved.get("hold_deposit")
    return s


def _price(active=True, unit_amount=9900):
    return SimpleNamespace(object="price", active=active, unit_amount=unit_amount)


# ---------------------------------------------------------------------------
# All configured prices resolve, active, priced → ok=True
# ---------------------------------------------------------------------------

def test_all_prices_ok():
    settings = _make_settings()
    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", return_value=_price()):
        result = check()
    assert result["ok"] is True
    assert result["problems"] == []


# ---------------------------------------------------------------------------
# A price not configured in this env is skipped (not a problem)
# ---------------------------------------------------------------------------

def test_unconfigured_price_skipped():
    # annual_lock resolves to None → skipped silently.
    settings = _make_settings(price_map={"annual_lock": None})
    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", return_value=_price()):
        result = check()
    assert result["ok"] is True
    assert all(p["name"] != "annual_lock" for p in result["problems"])


# ---------------------------------------------------------------------------
# A Stripe 404 on a configured price → reason=not_found, ok=False
# ---------------------------------------------------------------------------

def test_not_found_reported():
    settings = _make_settings()

    def retrieve(price_id, api_key=None):
        if price_id == "price_annual_lock":
            raise stripe.InvalidRequestError("No such price", param="id")
        return _price()

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", side_effect=retrieve):
        result = check()
    assert result["ok"] is False
    probs = [p for p in result["problems"] if p["reason"] == "not_found"]
    assert probs and probs[0]["name"] == "annual_lock"


# ---------------------------------------------------------------------------
# Archived price → reason=inactive
# ---------------------------------------------------------------------------

def test_inactive_reported():
    settings = _make_settings()

    def retrieve(price_id, api_key=None):
        return _price(active=(price_id != "price_pro_regular"))

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", side_effect=retrieve):
        result = check()
    assert result["ok"] is False
    assert any(p["reason"] == "inactive" and p["name"] == "pro_regular"
               for p in result["problems"])


# ---------------------------------------------------------------------------
# Price with no fixed unit_amount → reason=no_amount
# ---------------------------------------------------------------------------

def test_no_amount_reported():
    settings = _make_settings()

    def retrieve(price_id, api_key=None):
        return _price(unit_amount=(None if price_id == "price_hold_deposit" else 9900))

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", side_effect=retrieve):
        result = check()
    assert result["ok"] is False
    assert any(p["reason"] == "no_amount" and p["name"] == "hold_deposit"
               for p in result["problems"])


# ---------------------------------------------------------------------------
# All problems collected (no short-circuit on the first bad price)
# ---------------------------------------------------------------------------

def test_collects_all_problems():
    settings = _make_settings()
    bad = {"price_annual_lock", "price_wallet_power"}

    def retrieve(price_id, api_key=None):
        if price_id in bad:
            raise stripe.InvalidRequestError("No such price", param="id")
        return _price()

    with patch("src.services.pricing_truth.get_settings", return_value=settings), \
         patch("stripe.Price.retrieve", side_effect=retrieve):
        result = check()
    names = {p["name"] for p in result["problems"]}
    assert {"annual_lock", "wallet_power"} <= names


# ---------------------------------------------------------------------------
# No Stripe key configured → ok=True (nothing to diagnose; advisory)
# ---------------------------------------------------------------------------

def test_no_key_returns_ok():
    settings = _make_settings(no_key=True)
    with patch("src.services.pricing_truth.get_settings", return_value=settings):
        result = check()
    assert result["ok"] is True
    assert result["problems"] == []
