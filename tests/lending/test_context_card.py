"""The caller's context card."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from src.lending.context_card import NO_PRIOR_CONTACT, build_context_card
from src.lending.dialer_display import NOT_AVAILABLE
from src.lending.lead_facts import LeadFacts
from src.lending.lead_scoring import LeadSignals, Signal, score_lead

TODAY = date(2026, 10, 5)
FACTS = LeadFacts(
    property_id=7,
    property_address="123 Main St, Tampa, FL 33602",
    lender_name="Test Lender",
    latest_permit="Residential alteration, 2026-08-14",
    signals=LeadSignals(
        maturity_date=Signal.estimated(date(2026, 11, 20)),
        equity_pct=Signal.estimated(Decimal("42.4")),
        loan_amount=Signal.known(Decimal("350000")),
    ),
)


def test_full_card():
    card = build_context_card(FACTS, score_lead(FACTS.signals, today=TODAY), prior_contact="Called 2026-10-01, voicemail")
    assert card.fields == {
        "property": "123 Main St, Tampa, FL 33602",
        "loan": "$350,000",
        "lender": "Test Lender",
        "maturity": "Around Nov 2026 (estimated, confirm with the borrower)",
        "permits": "Residential alteration, 2026-08-14",
        "equity": "About 42% (estimated)",
        "prior_contact": "Called 2026-10-01, voicemail",
        "score": "2/10",
        "ask": "Is the loan coming due around November 2026?",
    }
    assert card.lines[0] == "Property: 123 Main St, Tampa, FL 33602"


def test_known_maturity_is_shown_as_a_date_without_a_question():
    facts = LeadFacts(7, None, None, None, LeadSignals(maturity_date=Signal.known(date(2026, 11, 20))))
    card = build_context_card(facts, score_lead(facts.signals, today=TODAY))
    assert card.fields["maturity"] == "2026-11-20"
    assert "ask" not in card.fields


def test_missing_values_show_not_available_and_no_prior_contact():
    facts = LeadFacts(7, None, None, None, LeadSignals())
    card = build_context_card(facts, score_lead(facts.signals, today=TODAY))
    for name in ("property", "loan", "lender", "maturity", "permits", "equity"):
        assert card.fields[name] == NOT_AVAILABLE
    assert card.fields["prior_contact"] == NO_PRIOR_CONTACT
