"""Regression tests for plan_price derivation from the recurring Stripe price.

Root cause (fix B): checkout stored `plan_price` from `session.amount_total`, the
charge for the first billing period. A discounted founding first month (e.g. $10
on a $299/mo plan) therefore persisted $10 as the monthly run-rate and understated
MRR — the exact "$10 DB / $299 Stripe" drift observed in production.

`_recurring_monthly_price_from_subscription` reads the recurring price instead, so
it is immune to a discounted or prorated first charge.
"""

from __future__ import annotations

from src.services.stripe_webhooks import _recurring_monthly_price_from_subscription


def _sub(unit_amount, interval="month", interval_count=1):
    return {
        "items": {
            "data": [
                {"price": {"unit_amount": unit_amount, "recurring": {
                    "interval": interval, "interval_count": interval_count}}}
            ]
        }
    }


def test_discounted_first_charge_uses_recurring_price():
    # $299/mo recurring — must return 299.00 regardless of a $10 first charge.
    assert _recurring_monthly_price_from_subscription(_sub(29900)) == 299.00


def test_annual_price_normalized_to_monthly():
    # $3588/yr → $299/mo run-rate.
    assert _recurring_monthly_price_from_subscription(
        _sub(358800, interval="year")) == 299.00


def test_twelve_month_interval_treated_as_annual():
    assert _recurring_monthly_price_from_subscription(
        _sub(358800, interval="month", interval_count=12)) == 299.00


def test_no_subscription_returns_none():
    assert _recurring_monthly_price_from_subscription(None) is None


def test_no_items_returns_none():
    assert _recurring_monthly_price_from_subscription({"items": {"data": []}}) is None


def test_zero_unit_amount_returns_none():
    # Trials / $0 recurring have no run-rate to record here.
    assert _recurring_monthly_price_from_subscription(_sub(0)) is None
