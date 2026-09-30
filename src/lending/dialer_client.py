"""The dialer contract the lending load and removal code depend on.

The dialer is a provider behind this interface, so the compliance filter,
Backflip check and load bookkeeping never import a provider client. Each
provider adapter maps a ``DialerDisplay`` onto its own contact fields and
translates its errors into the two exceptions below.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from src.lending.dialer_contact import DialerDisplay


@dataclass(frozen=True)
class ContactUpsertResult:
    contact_id: int
    created: bool


class DialerRequestError(RuntimeError):
    """A dialer API call failed. ``status`` is the HTTP status, 0 for a network error."""

    def __init__(self, status: int, detail: str = "") -> None:
        super().__init__(f"dialer request failed ({status}){': ' + detail if detail else ''}")
        self.status = status


class DialerAmbiguousContact(RuntimeError):
    """More than one dialer contact already holds the phone, so the load will not pick one."""


class DialerClient(Protocol):
    def upsert_contact(self, phone: str, display: DialerDisplay, email: Optional[str]) -> ContactUpsertResult:
        """Update the contact holding ``phone``, or create one. Re-loads never duplicate."""
        ...

    def update_contact(self, contact_id: int, display: DialerDisplay, email: Optional[str]) -> None:
        """Update a known contact. Raises ``DialerRequestError`` with status 404 if it is gone."""
        ...
