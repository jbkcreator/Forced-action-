"""Text-consent evidence (client Q27). A number is textable only with a live, unrevoked row and no opt-out.

Every LendingFlow lead counts as having agreed to texts (client decision) without a row, unless a consent row for
that number was revoked (STOP or a soft decline) or the number is opted out."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import bindparam, text

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


def text_consented_phones(db, phones) -> set[str]:
    """The subset of ``phones`` (normalized) that ``has_text_consent`` would allow, in one query."""
    norms = sorted({n for n in (normalize(p) for p in phones) if n})
    if not norms:
        return set()
    rows = db.execute(
        text("SELECT p.phone FROM (SELECT phone FROM lending.text_consents WHERE phone IN :ps UNION "
             "      SELECT phone FROM lending.lendingflow_leads WHERE phone IN :ps) p "
             "WHERE (EXISTS (SELECT 1 FROM lending.text_consents c WHERE c.phone = p.phone AND c.revoked_at IS NULL) "
             "       OR (EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = p.phone) "
             "           AND NOT EXISTS (SELECT 1 FROM lending.text_consents c WHERE c.phone = p.phone "
             "                            AND c.revoked_at IS NOT NULL))) "
             "  AND NOT EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = p.phone) "
             "  AND NOT EXISTS (SELECT 1 FROM lending.contacts ct WHERE ct.phone = p.phone AND ct.do_not_contact)"
             ).bindparams(bindparam("ps", expanding=True)),
        {"ps": norms},
    ).scalars().all()
    return set(rows)


def has_text_consent(db, phone: str) -> bool:
    norm = normalize(phone)
    if not norm:
        return False
    return bool(db.execute(
        text("SELECT (EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :p AND revoked_at IS NULL) "
             "      OR (EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p) "
             "          AND NOT EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :p AND revoked_at IS NOT NULL))) "
             "AND NOT EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :p) "
             "AND NOT EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND do_not_contact)"),
        {"p": norm},
    ).scalar())
