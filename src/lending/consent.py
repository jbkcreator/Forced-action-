"""Text-consent evidence (client Q27). A number is textable only with a live, unrevoked row and no opt-out.

Every LendingFlow lead counts as having agreed to texts (client decision) without a row, unless a consent row for
that number was revoked (STOP or a soft decline) or the number is opted out.

Universal rule for every lead source: a number that said "not interested" on a call is never messaged again
(``said_not_interested``; the GHL senders refuse texts and emails to it in ``cold_leads.cold_send_decision``)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_dispositions import NOT_INTERESTED_CODES
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CONSENT_SOURCES = ("inbound_call", "inbound_text", "web_form", "on_call_yes")


def record_consent(db, phone: str, source: str, *, call_id: Optional[str] = None,
                   captured_by: Optional[str] = None, at: Optional[datetime] = None) -> None:
    if source not in CONSENT_SOURCES:
        raise ValueError(f"unknown consent source {source!r}")
    norm = normalize(phone)
    if not norm:
        return
    db.execute(
        text("INSERT INTO lending.text_consents (phone, source, call_id, captured_by, captured_at) "
             "VALUES (:p, :s, :c, :by, :at) "
             "ON CONFLICT (phone, source) DO UPDATE SET revoked_at = NULL"),
        {"p": norm, "s": source, "c": call_id, "by": captured_by, "at": at or datetime.now(timezone.utc)},
    )
    logger.info("[lending] text consent recorded source=%s call=%s", source, call_id)


def revoke_consent(db, phone: str) -> None:
    norm = normalize(phone)
    if norm:
        db.execute(text("UPDATE lending.text_consents SET revoked_at = now() WHERE phone = :p AND revoked_at IS NULL"),
                   {"p": norm})


# SQL condition (bind :p = normalized phone): the number said "not interested" on a call. Never messaged again.
_NOT_INTERESTED_CODES_SQL = ", ".join(f"'{code}'" for code in sorted(NOT_INTERESTED_CODES))
SAID_NOT_INTERESTED_SQL = (f"EXISTS (SELECT 1 FROM lending.call_dispositions "
                           f"WHERE phone = :p AND disposition IN ({_NOT_INTERESTED_CODES_SQL}))")


def said_not_interested(db, phone: Optional[str]) -> bool:
    norm = normalize(phone) if phone else None
    return bool(norm) and bool(db.execute(text(f"SELECT {SAID_NOT_INTERESTED_SQL}"), {"p": norm}).scalar())


def has_text_consent(db, phone: str) -> bool:
    norm = normalize(phone)
    if not norm:
        return False
    return bool(db.execute(
        text("SELECT (EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :p AND revoked_at IS NULL) "
             "      OR (EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p) "
             "          AND NOT EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :p AND revoked_at IS NOT NULL))) "
             "AND NOT EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :p) "
             "AND NOT EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND do_not_contact) "
             f"AND NOT {SAID_NOT_INTERESTED_SQL}"),
        {"p": norm},
    ).scalar())
