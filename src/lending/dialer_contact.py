"""What a caller sees for a lending pool record, independent of the dialer.

``DialerDisplay`` holds the five spec display fields plus the campaign.
Provider adapters decide where each field goes on their contact record;
``display_lines`` is the shared labelled-line rendering for providers that
only offer a free-text field.

Blank values are shown as "Not available" so a caller can tell a missing
value from a field that was never mapped.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

NOT_AVAILABLE = "Not available"


@dataclass(frozen=True)
class DialerDisplay:
    """The five spec display fields plus the campaign, as stored and shown."""

    borrower_name: Optional[str]
    entity_name: Optional[str]
    property_address: Optional[str]
    estimated_loan_value: Optional[Decimal]
    recent_permit_details: Optional[str]
    campaign_tag: Optional[str]


def _clean(value: Any) -> Optional[str]:
    text = " ".join(str(value).split()) if value is not None else ""
    return text or None


def _money(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        amount = Decimal(str(value).replace("$", "").replace(",", "").strip())
    except InvalidOperation:
        return None
    return amount if amount >= 0 else None


def display_from_record(record: Mapping[str, Any], campaign_tag: Optional[str]) -> DialerDisplay:
    return DialerDisplay(
        borrower_name=_clean(record.get("borrower_name")),
        entity_name=_clean(record.get("entity_name")),
        property_address=_clean(record.get("property_address")),
        estimated_loan_value=_money(record.get("estimated_loan_value")),
        recent_permit_details=_clean(record.get("recent_permit_details")),
        campaign_tag=_clean(campaign_tag),
    )


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Last word is the last name; a single word is kept whole as the first name."""
    if not name:
        return None, None
    first, _, last = name.rpartition(" ")
    return (first, last) if first else (last, None)


def display_lines(display: DialerDisplay) -> list[str]:
    """Labelled lines for the fields that have no dedicated contact field."""
    loan = f"${display.estimated_loan_value:,.0f}" if display.estimated_loan_value is not None else None
    return [
        f"Property: {display.property_address or NOT_AVAILABLE}",
        f"Est. loan value: {loan or NOT_AVAILABLE}",
        f"Recent permit: {display.recent_permit_details or NOT_AVAILABLE}",
        f"Campaign: {display.campaign_tag or NOT_AVAILABLE}",
    ]
