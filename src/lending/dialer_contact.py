"""Map a lending pool record to the dialer contact a caller sees.

The contact carries fixed fields (first/last name, company, a free-text
information field), so the five dialer display fields are placed as:

- Borrower Name          -> first_name / last_name
- Entity Name            -> company_name
- Target Property Address, Estimated Loan Value, Recent Permit Details,
  campaign, county, hook -> labelled lines in ``information``

Blank values are shown as "Not available" so a caller can tell a missing
value from a field that was never mapped.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from config.lending_dialer import CAMPAIGN_HOOKS
from src.lending.dialer_port import DialerContactFields
from src.services.fa_max_backflip_feed import normalize_email

NOT_AVAILABLE = "Not available"
INFORMATION_MAX_CHARS = 1000


@dataclass(frozen=True)
class DialerDisplay:
    """The five spec display fields plus campaign, county and hook line, as stored and shown."""

    borrower_name: Optional[str]
    entity_name: Optional[str]
    property_address: Optional[str]
    estimated_loan_value: Optional[Decimal]
    recent_permit_details: Optional[str]
    campaign_tag: Optional[str]
    county: Optional[str] = None
    hook: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None


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
        county=_clean(record.get("county")),
        hook=CAMPAIGN_HOOKS.get(campaign_tag or ""),
        state=_clean(record.get("state")),
        postal_code=_clean(record.get("zip")),
    )


def _split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Last word is the last name; a single word is kept whole as the first name."""
    if not name:
        return None, None
    first, _, last = name.rpartition(" ")
    return (first, last) if first else (last, None)


def _information(display: DialerDisplay) -> str:
    loan = f"${display.estimated_loan_value:,.0f}" if display.estimated_loan_value is not None else None
    lines = [
        f"Property: {display.property_address or NOT_AVAILABLE}",
        f"Est. loan value: {loan or NOT_AVAILABLE}",
        f"Recent permit: {display.recent_permit_details or NOT_AVAILABLE}",
        f"Campaign: {display.campaign_tag or NOT_AVAILABLE}",
        f"County: {display.county or NOT_AVAILABLE}",
        f"Hook: {display.hook or NOT_AVAILABLE}",
    ]
    text = "\n".join(lines)
    return text if len(text) <= INFORMATION_MAX_CHARS else text[: INFORMATION_MAX_CHARS - 1] + "…"


def _street_and_city(address: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """"123 Main St, Tampa, FL 33602" -> ("123 Main St", "Tampa"); a bare street keeps no city."""
    if not address:
        return None, None
    parts = [p.strip() for p in address.split(",")]
    return parts[0] or None, (parts[1] or None) if len(parts) > 2 else None


def dialer_fields(display: DialerDisplay, email: Optional[str] = None) -> DialerContactFields:
    first_name, last_name = _split_name(display.borrower_name)
    street, city = _street_and_city(display.property_address)
    return DialerContactFields(
        first_name=first_name,
        last_name=last_name,
        company_name=display.entity_name,
        information=_information(display),
        email=normalize_email(email),
        address=street,
        city=city,
        state=display.state,
        postal_code=display.postal_code,
    )
