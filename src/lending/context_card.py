"""The context card a caller sees before every dial.

Pure rendering over a lead's facts and score: property, loan, lender,
maturity, permits, equity, prior contact and rank. Estimated values are
marked as estimates, and an estimated maturity is shown as a question to ask,
never as a deadline. The ``fields`` mapping is what a dialer adapter writes
onto the contact; ``lines`` is the same content for a plain-text field.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from src.lending.dialer_display import NOT_AVAILABLE
from src.lending.lead_facts import LeadFacts
from src.lending.lead_scoring import LeadScore, Provenance, Signal

NO_PRIOR_CONTACT = "No prior contact"

CARD_FIELD_LABELS: dict[str, str] = {
    "property": "Property",
    "loan": "Loan",
    "lender": "Lender",
    "maturity": "Maturity",
    "permits": "Recent permit",
    "equity": "Equity",
    "prior_contact": "Prior contact",
    "score": "Score",
    "ask": "Ask",
}


@dataclass(frozen=True)
class ContextCard:
    fields: dict[str, str]

    @property
    def lines(self) -> list[str]:
        return [f"{CARD_FIELD_LABELS[name]}: {value}" for name, value in self.fields.items()]


def _money(signal: Signal[Decimal]) -> str:
    if signal.value is None:
        return NOT_AVAILABLE
    text = f"${signal.value:,.0f}"
    return f"{text} (estimated)" if signal.provenance is Provenance.ESTIMATED else text


def _maturity(signal) -> str:
    if signal.value is None:
        return NOT_AVAILABLE
    if signal.provenance is Provenance.ESTIMATED:
        return f"Around {signal.value:%b %Y} (estimated, confirm with the borrower)"
    return f"{signal.value:%Y-%m-%d}"


def _equity(signal: Signal[Decimal]) -> str:
    if signal.value is None:
        return NOT_AVAILABLE
    text = f"{signal.value:.0f}%"
    return f"About {text} (estimated)" if signal.provenance is Provenance.ESTIMATED else text


def build_context_card(facts: LeadFacts, score: LeadScore, *, prior_contact: Optional[str] = None) -> ContextCard:
    """Render the card for one lead. ``prior_contact`` is a one-line summary of earlier calls."""
    signals = facts.signals
    fields = {
        "property": facts.property_address or NOT_AVAILABLE,
        "loan": _money(signals.loan_amount),
        "lender": facts.lender_name or NOT_AVAILABLE,
        "maturity": _maturity(signals.maturity_date),
        "permits": facts.latest_permit or NOT_AVAILABLE,
        "equity": _equity(signals.equity_pct),
        "prior_contact": prior_contact or NO_PRIOR_CONTACT,
        "score": f"{score.rank}/10",
    }
    if score.caller_questions:
        fields["ask"] = " ".join(score.caller_questions)
    return ContextCard(fields=fields)
