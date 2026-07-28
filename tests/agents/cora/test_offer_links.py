from __future__ import annotations

from unittest.mock import MagicMock

from config.settings import get_settings
from src.agents.cora.offer_links import resolve_offer_link
from tests.agents.cora.fixtures.whales import WHALES


def test_hard_money_intro_resolves_calendar_link(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "demo_calendly_url", "https://calendly.com/test-rep", raising=False)
    resolved = resolve_offer_link("hard_money_intro")
    assert resolved.booking_link == "https://calendly.com/test-rep"
    assert resolved.payment_link is None


def test_hard_money_intro_never_carries_a_payment_link():
    # RESPA clearance not confirmed — this offer must never resolve a fee/checkout link.
    resolved = resolve_offer_link("hard_money_intro")
    assert resolved.payment_link is None


def test_checkout_kind_offers_resolve_payment_link_none():
    # Documented interim limitation — no stripe_service function fits a cold prospect yet.
    for offer in ("core_subscription", "lead_packs", "insurance_distress_pack", "bankruptcy_alert"):
        resolved = resolve_offer_link(offer)
        assert resolved.payment_link is None
        assert resolved.booking_link is None


def test_founder_tier_without_buyer_entity_resolves_none():
    # No buyer_entity supplied — falls through to the documented "unbuilt" path, not a crash.
    resolved = resolve_offer_link("founder_tier")
    assert resolved.payment_link is None


def test_founder_tier_with_buyer_entity_resolves_real_checkout_url():
    whale = WHALES[0]
    resolved = resolve_offer_link("founder_tier", buyer_entity=whale, db=MagicMock())
    assert resolved.payment_link == "https://checkout.stripe.com/test-fake"


def test_founder_tier_checkout_sources_price_from_settings_not_db(monkeypatch):
    # Direct instruction: founder_tier's price id must come from settings
    # (mode-aware active_stripe_price), never from plans.stripe_price_id.
    settings = get_settings()
    monkeypatch.setattr(settings, "stripe_test_mode", False, raising=False)
    monkeypatch.setattr(settings, "stripe_price_founder_monthly", "price_live_founder_monthly", raising=False)

    mock_create = MagicMock(return_value=MagicMock(url="https://checkout.stripe.com/x"))
    monkeypatch.setattr("stripe.checkout.Session.create", mock_create)

    whale = dict(WHALES[0], county_id="hillsborough")
    resolve_offer_link("founder_tier", buyer_entity=whale, db=MagicMock())

    _, kwargs = mock_create.call_args
    assert kwargs["line_items"][0]["price"] == "price_live_founder_monthly"
    assert kwargs["metadata"]["vertical"] == "investor"
    assert kwargs["metadata"]["county_id"] == "hillsborough"


def test_founder_tier_checkout_uses_test_price_in_test_mode(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "stripe_test_mode", True, raising=False)
    monkeypatch.setattr(settings, "stripe_test_price_founder_monthly", "price_test_founder_monthly", raising=False)

    mock_create = MagicMock(return_value=MagicMock(url="https://checkout.stripe.com/x"))
    monkeypatch.setattr("stripe.checkout.Session.create", mock_create)

    resolve_offer_link("founder_tier", buyer_entity=WHALES[0], db=MagicMock())

    _, kwargs = mock_create.call_args
    assert kwargs["line_items"][0]["price"] == "price_test_founder_monthly"


def test_founder_tier_no_price_configured_resolves_none(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "stripe_price_founder_monthly", None, raising=False)
    monkeypatch.setattr(settings, "stripe_test_mode", False, raising=False)

    resolved = resolve_offer_link("founder_tier", buyer_entity=WHALES[0], db=MagicMock())
    assert resolved.payment_link is None


def test_founder_tier_checkout_failure_resolves_none_not_a_crash(monkeypatch):
    monkeypatch.setattr(
        "stripe.checkout.Session.create",
        MagicMock(side_effect=RuntimeError("Stripe API error")),
    )
    resolved = resolve_offer_link("founder_tier", buyer_entity=WHALES[0], db=MagicMock())
    assert resolved.payment_link is None


def test_founder_tier_stripe_not_configured_resolves_none(monkeypatch):
    monkeypatch.setattr("src.services.stripe_service._init_stripe", lambda: False)
    resolved = resolve_offer_link("founder_tier", buyer_entity=WHALES[0], db=MagicMock())
    assert resolved.payment_link is None


def test_unknown_offer_resolves_empty():
    resolved = resolve_offer_link("not_a_real_offer")
    assert resolved.booking_link is None
    assert resolved.payment_link is None
