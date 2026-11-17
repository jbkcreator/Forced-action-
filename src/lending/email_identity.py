"""The one sending identity for every Next Deal Lending email: From ``LENDING_EMAIL_FROM`` (David), a blind copy to
``LENDING_EMAIL_BCC`` (Josh), never a CC.

Every lending email path reads it here, so no sender can drift: the GHL sender (Cora's approved emails, document
chase, booking reminders, gate-fail message), the pre-qual and partner letters, and NDL's Instantly cold campaign and
thread replies. The client requires both on every email, so a sender that finds either unset sends nothing.
LendingFlow's first-touch and follow-up emails are GHL workflow actions; their From and BCC are set in GHL by hand
(docs/lending/email-sender.md).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from config.settings import get_settings

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
    """David as sender and Josh in BCC, or None (logged once per problem set) when either is unset or invalid."""
    global _warned
    settings = get_settings()
    from_address = _address(settings.lending_email_from or "")
    bcc = _parse_bcc(settings.lending_email_bcc or "")
    if from_address is not None and bcc is not None:
        return LendingEmailIdentity(from_address=from_address, bcc=bcc)
    problems = tuple(name for name, ok in (("LENDING_EMAIL_FROM", from_address), ("LENDING_EMAIL_BCC", bcc)) if not ok)
    if problems != _warned:
        logger.error("[lending-email] no lending email will be sent; missing or invalid: %s", ", ".join(problems))
        _warned = problems
    return None
