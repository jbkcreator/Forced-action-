"""Aircall behind the lending ``DialerClient`` contract.

Aircall contacts have fixed fields (first/last name, company, a free-text
information field) and no custom attributes, so the display is placed as:

- Borrower Name          -> first_name / last_name
- Entity Name            -> company_name
- Target Property Address, Estimated Loan Value, Recent Permit Details,
  campaign               -> labelled lines in ``information``
"""
from __future__ import annotations

from typing import Optional

from src.lending.dialer_client import (
    ContactUpsertResult,
    DialerAmbiguousContact,
    DialerRequestError,
)
from src.lending.dialer_contact import DialerDisplay, display_lines, split_name
from src.services import aircall_client
from src.services.aircall_client import AircallAmbiguousContact, AircallContactFields, AircallRequestError
from src.services.fa_max_backflip_feed import normalize_email

INFORMATION_MAX_CHARS = 1000


def _information(display: DialerDisplay) -> str:
    text = "\n".join(display_lines(display))
    return text if len(text) <= INFORMATION_MAX_CHARS else text[: INFORMATION_MAX_CHARS - 1] + "…"


def aircall_fields(display: DialerDisplay, email: Optional[str] = None) -> AircallContactFields:
    first_name, last_name = split_name(display.borrower_name)
    return AircallContactFields(
        first_name=first_name,
        last_name=last_name,
        company_name=display.entity_name,
        information=_information(display),
        email=normalize_email(email),
    )


class AircallDialer:
    """``DialerClient`` over the Aircall contact writes in ``aircall_client``."""

    def upsert_contact(self, phone: str, display: DialerDisplay, email: Optional[str]) -> ContactUpsertResult:
        try:
            result = aircall_client.upsert_contact(phone, aircall_fields(display, email))
        except AircallAmbiguousContact as exc:
            raise DialerAmbiguousContact(str(exc)) from exc
        except AircallRequestError as exc:
            raise DialerRequestError(exc.status) from exc
        return ContactUpsertResult(contact_id=result.contact_id, created=result.created)

    def update_contact(self, contact_id: int, display: DialerDisplay, email: Optional[str]) -> None:
        try:
            aircall_client.update_contact(contact_id, aircall_fields(display, email))
        except AircallRequestError as exc:
            raise DialerRequestError(exc.status) from exc
