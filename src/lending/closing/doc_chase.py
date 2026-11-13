"""P2.4 document chase: started when an approver tells Cora the borrower accepted the term sheet.

The chase holds the lender's checklist (config/lending_closing.py) plus any document a P2.2 quote condition adds.
Each message carries its own upload link (only the token's SHA-256 is stored); every link of a chase works until it
expires or the chase closes. Messages go out from the sweep (``run_chase_sweep``) as approval cards.
"""
from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from config.lending_closing import TOKEN_TTL, UPLOAD_LINK_PATH
from src.lending.closing.drafts import Recipient
from src.lending.closing.rules import build_checklist
from src.lending.partner_service import PartnerLinkNotConfigured, link_base_url
from src.lending.partners import hash_token

logger = logging.getLogger(__name__)

TOKEN_BYTES = 32
CENTRAL_LENDING = "central_lending"


@dataclass(frozen=True)
class StartedChase:
    chase_id: int
    missing: tuple[str, ...]
    reused: bool


_OPEN_FOR_PHONE = text("SELECT id FROM lending.doc_chases WHERE status = 'open' AND phone = :phone")
_MISSING = text("SELECT label FROM lending.doc_chase_items WHERE chase_id = :c AND received_at IS NULL ORDER BY id")
_OPPORTUNITY = text("SELECT ghl_opportunity_id FROM lending.ghl_opportunity_stage WHERE phone = :phone "
                    "ORDER BY updated_at DESC LIMIT 1")
_LOAN_FILE = text("SELECT id FROM lending.loan_files WHERE borrower_phone = :phone ORDER BY created_at DESC LIMIT 1")
# P2.2 stores each quote's request and result in lending.instant_quotes; conditions[] carries {"code": ...}.
_QUOTE_CONDITIONS = text("SELECT c ->> 'code' FROM lending.instant_quotes q, "
                         "jsonb_array_elements(COALESCE(q.result -> 'conditions', '[]'::jsonb)) c "
                         "WHERE q.quote_id = CAST(:q AS uuid)")
_INSERT_CHASE = text("""
    INSERT INTO lending.doc_chases (lead_ref, ghl_contact_id, ghl_opportunity_id, loan_file_id, first_name, phone, email,
                                    lender_key, quote_id, accepted_at, accepted_by, created_at, updated_at)
    VALUES (:lead_ref, :contact, :opp, :loan_file, :first_name, :phone, :email, :lender, CAST(:quote AS uuid), :now,
            :by, :now, :now)
    RETURNING id
""")
_INSERT_ITEM = text("INSERT INTO lending.doc_chase_items (chase_id, item_code, label) VALUES (:c, :code, :label)")
_INSERT_LINK = text("INSERT INTO lending.doc_chase_links (chase_id, token_hash, expires_at, created_at) "
                    "VALUES (:c, :hash, :expires, :now)")
_CHASE_FOR_TOKEN = text("""
    SELECT c.* FROM lending.doc_chase_links l JOIN lending.doc_chases c ON c.id = l.chase_id
     WHERE l.token_hash = :hash AND l.revoked_at IS NULL AND l.expires_at > :now AND c.status = 'open'
""")


def _optional(db, statement, params: dict[str, Any]) -> Any:
    """A lookup that may fail (a table not there yet): None, never an aborted caller transaction."""
    try:
        with db.begin_nested():
            return db.execute(statement, params).scalar()
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.warning("[doc-chase] optional lookup failed (%s)", type(exc).__name__)
        return None


def _quote_conditions(db, quote_id: str) -> list[str]:
    """Condition codes of a P2.2 quote; empty when the quote (or the P2.2 table) is not there."""
    try:
        with db.begin_nested():
            return [code for code in db.execute(_QUOTE_CONDITIONS, {"q": quote_id}).scalars() if code]
    except Exception as exc:  # class only
        logger.warning("[doc-chase] quote conditions unreadable (%s); checklist without them", type(exc).__name__)
        return []


def _missing(db, chase_id: int) -> tuple[str, ...]:
    return tuple(db.execute(_MISSING, {"c": chase_id}).scalars())


def start_chase(db, recipient: Recipient, *, lead_ref: str, accepted_by: str, lender_key: str = CENTRAL_LENDING,
                quote_id: Optional[str] = None, now: Optional[datetime] = None) -> StartedChase:
    """Open a chase for the borrower (or return the open one). Does not commit; the first message goes from the sweep."""
    if link_base_url() is None:
        raise PartnerLinkNotConfigured("LENDING_PARTNER_LINK_BASE_URL is not set: upload links cannot be made")
    if not recipient.phone and not recipient.email:
        raise ValueError("the borrower has no phone or email on record")
    now = now or datetime.now(timezone.utc)
    if recipient.phone:
        existing = db.execute(_OPEN_FOR_PHONE, {"phone": recipient.phone}).scalar()
        if existing is not None:
            return StartedChase(existing, _missing(db, existing), reused=True)
    conditions = _quote_conditions(db, quote_id) if quote_id else []
    checklist = build_checklist(lender_key, conditions)
    phone_params = {"phone": recipient.phone}
    chase_id = db.execute(_INSERT_CHASE, {
        "lead_ref": lead_ref, "contact": recipient.ghl_contact_id,
        "opp": _optional(db, _OPPORTUNITY, phone_params) if recipient.phone else None,
        "loan_file": _optional(db, _LOAN_FILE, phone_params) if recipient.phone else None,
        "first_name": recipient.first_name, "phone": recipient.phone, "email": recipient.email,
        "lender": lender_key, "quote": quote_id, "now": now, "by": accepted_by}).scalar_one()
    db.execute(_INSERT_ITEM, [{"c": chase_id, "code": item.code, "label": item.label} for item in checklist])
    logger.info("[doc-chase] chase %s started (%s items, lender %s)", chase_id, len(checklist), lender_key)
    return StartedChase(chase_id, tuple(item.label for item in checklist), reused=False)


def issue_link(db, chase_id: int, now: datetime) -> str:
    """A new upload link for the chase; the raw token exists only in the returned URL."""
    base = link_base_url()
    if base is None:
        raise PartnerLinkNotConfigured("LENDING_PARTNER_LINK_BASE_URL is not set: upload links cannot be made")
    token = secrets.token_urlsafe(TOKEN_BYTES)
    db.execute(_INSERT_LINK, {"c": chase_id, "hash": hash_token(token), "expires": now + TOKEN_TTL, "now": now})
    return base + UPLOAD_LINK_PATH.format(token=token)


def chase_for_token(db, token: str, now: datetime) -> Optional[dict[str, Any]]:
    """The open chase behind a live link; None for an unknown, expired or revoked link or a closed chase."""
    if not token or len(token) > 200:
        return None
    row = db.execute(_CHASE_FOR_TOKEN, {"hash": hash_token(token), "now": now}).mappings().first()
    return dict(row) if row else None
