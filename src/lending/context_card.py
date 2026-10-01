"""The context card a caller sees before every dial.

Pure rendering over a lead's facts and score: who and where (first name,
property, county), the campaign and its hook line, then loan, lender,
maturity, permits, equity, prior contact and rank. Estimated values are
marked as estimates, and an estimated maturity is shown as a question to ask,
never as a deadline.

``custom_fields`` is what the dialer adapter writes onto the BatchDialer
contact (the contact's ``customfields``), so the agent script can show each
value on its own; ``lines`` is the same content for a plain-text field.
The campaign's hook line is passed in by the caller, which owns the
campaign-to-hook mapping.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from src.lending.dialer_display import NOT_AVAILABLE
from src.lending.lead_facts import LeadFacts
from src.lending.lead_scoring import LeadScore, Provenance, Signal

NO_PRIOR_CONTACT = "No prior contact"
CUSTOM_FIELD_MAX_CHARS = 255

CARD_FIELD_LABELS: dict[str, str] = {
    "first_name": "First name",
    "property": "Property",
    "county": "County",
    "campaign": "Campaign",
    "hook": "Hook",
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

    @property
    def custom_fields(self) -> dict[str, str]:
        """BatchDialer ``customfields`` payload: one key per card field, values bounded."""
        return {
            name: value if len(value) <= CUSTOM_FIELD_MAX_CHARS else value[: CUSTOM_FIELD_MAX_CHARS - 1] + "…"
            for name, value in self.fields.items()
        }


def _text(value: Optional[str]) -> str:
    cleaned = " ".join(value.split()) if value else ""
    return cleaned or NOT_AVAILABLE


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


def build_context_card(
    facts: LeadFacts,
    score: LeadScore,
    *,
    first_name: Optional[str] = None,
    county: Optional[str] = None,
    campaign: Optional[str] = None,
    hook: Optional[str] = None,
    prior_contact: Optional[str] = None,
) -> ContextCard:
    """Render the card for one lead. ``prior_contact`` is a one-line summary of earlier calls."""
    signals = facts.signals
    fields = {
        "first_name": _text(first_name),
        "property": _text(facts.property_address),
        "county": _text(county),
        "campaign": _text(campaign),
        "hook": _text(hook),
        "loan": _money(signals.loan_amount),
        "lender": _text(facts.lender_name),
        "maturity": _maturity(signals.maturity_date),
        "permits": _text(facts.latest_permit),
        "equity": _equity(signals.equity_pct),
        "prior_contact": _text(prior_contact) if prior_contact else NO_PRIOR_CONTACT,
        "score": f"{score.rank}/10",
    }
    if score.caller_questions:
        fields["ask"] = " ".join(score.caller_questions)
    return ContextCard(fields=fields)
