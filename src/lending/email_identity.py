"""The sending identity for email to a LendingFlow lead: From ``LENDING_LENDINGFLOW_EMAIL_FROM`` (David), a blind copy
to ``LENDING_LENDINGFLOW_EMAIL_BCC`` (Josh), never a CC. Client request (Nov 17): every LendingFlow email, the first
touch included.

Only LendingFlow leads get it. The GHL sender (Cora's approved emails, document chase, booking reminders, gate-fail
message) asks ``is_lendingflow_recipient`` per email; every pre-qual letter goes to a LendingFlow lead (it is only ever
queued for one). Website, partner and cold (Instantly) email keep their own senders and no BCC. The client requires
both settings on every LendingFlow email, so a LendingFlow email is not sent while either is unset, and neither is an
email whose recipient cannot be checked. LendingFlow's first-touch and follow-up emails are GHL workflow steps; their
From and BCC are set in GHL by hand (docs/lending/email-sender.md).
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from config.settings import get_settings
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LendingEmailIdentity:
    from_address: str
    bcc: tuple[str, ...]


def _address(value: str) -> str | None:
    address = value.strip().lower()
    local, at, domain = address.partition("@")
    return address if at and local and "." in domain and " " not in address else None


def _parse_bcc(raw: str) -> tuple[str, ...] | None:
    """The BCC list, deduplicated in order, or None when empty or any entry is not an address."""
    entries = [part for part in raw.split(",") if part.strip()]
    parsed = [_address(part) for part in entries]
    if not parsed or any(address is None for address in parsed):
        return None
    return tuple(dict.fromkeys(address for address in parsed if address))


_warned: tuple[str, ...] | None = None


def lending_email_identity() -> LendingEmailIdentity | None:
    """David as sender and Josh in BCC for LendingFlow leads, or None (logged once per problem set) when either
    setting is unset or invalid."""
    global _warned
    settings = get_settings()
    from_address = _address(settings.lending_lendingflow_email_from or "")
    bcc = _parse_bcc(settings.lending_lendingflow_email_bcc or "")
    if from_address is not None and bcc is not None:
        return LendingEmailIdentity(from_address=from_address, bcc=bcc)
    problems = tuple(name for name, ok in (("LENDING_LENDINGFLOW_EMAIL_FROM", from_address),
                                           ("LENDING_LENDINGFLOW_EMAIL_BCC", bcc)) if not ok)
    if problems != _warned:
        logger.error("[lending-email] no email to a LendingFlow lead will be sent; missing or invalid: %s",
                     ", ".join(problems))
        _warned = problems
    return None


_IS_LENDINGFLOW = text(
    "SELECT EXISTS (SELECT 1 FROM lending.lendingflow_leads "
    "WHERE (CAST(:e AS text) IS NOT NULL AND lower(email) = :e) OR (CAST(:p AS text) IS NOT NULL AND phone = :p))"
)


def _default_session() -> Any:
    from src.lending.db import lending_session

    return lending_session()


_session_factory: Callable[[], Any] = _default_session


def is_lendingflow_recipient(email: str | None, phone: str | None) -> bool:
    """Whether the address or phone belongs to a LendingFlow lead. Raises when the check cannot be read: the caller
    refuses the send rather than guess the sender."""
    address = (email or "").strip().lower() or None
    number = normalize(phone) if phone else None
    if address is None and number is None:
        return False
    with _session_factory() as db:
        return bool(db.execute(_IS_LENDINGFLOW, {"e": address, "p": number}).scalar())
