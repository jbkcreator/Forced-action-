from __future__ import annotations

from config.settings import get_settings
from src.agents.cora.offer_links import resolve_offer_link


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
    for offer in ("core_subscription", "lead_packs", "insurance_distress_pack", "bankruptcy_alert", "founder_tier"):
        resolved = resolve_offer_link(offer)
        assert resolved.payment_link is None
        assert resolved.booking_link is None


def test_unknown_offer_resolves_empty():
    resolved = resolve_offer_link("not_a_real_offer")
    assert resolved.booking_link is None
    assert resolved.payment_link is None
