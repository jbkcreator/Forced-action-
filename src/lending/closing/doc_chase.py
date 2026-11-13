"""P2.4 document chase: started when an approver tells Cora the borrower accepted the term sheet.

The chase holds the lender's checklist (config/lending_closing.py) plus any document a P2.2 quote condition adds.
Each message carries its own upload link (only the token's SHA-256 is stored); every link of a chase works until it
expires or the chase closes. Messages go out from the sweep (``run_chase_sweep``) as approval cards.
"""
from __future__ import annotations

import logging
import os
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text

from config.lending_closing import CLOSED_STAGE_KEYS, TOKEN_TTL, UPLOAD_LINK_PATH
from packages.agent_core.send_gate import SendGate
from src.lending.closing.drafts import Recipient, chase_draft, submit_draft
from src.lending.closing.rules import build_checklist, due_chase_day
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.partner_service import PartnerLinkNotConfigured, link_base_url
from src.lending.partners import InvalidSubmission, Upload, check_uploads, hash_token

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


# ---- the sweep -----------------------------------------------------------------------------------------------------

DRAFT_TTL = timedelta(hours=24)
_OPEN_CHASES = text("""
    SELECT c.id, c.first_name, c.phone, c.email, c.ghl_contact_id, c.accepted_at, s.stage_key,
           COALESCE((SELECT array_agg(t.chase_day) FROM lending.doc_chase_touches t WHERE t.chase_id = c.id),
                    '{}') AS sent_days
      FROM lending.doc_chases c
      LEFT JOIN lending.ghl_opportunity_stage s ON s.ghl_opportunity_id = c.ghl_opportunity_id
     WHERE c.status = 'open'
     ORDER BY c.id
       FOR UPDATE OF c SKIP LOCKED
""")
_CLOSE = text("UPDATE lending.doc_chases SET status = :status, status_reason = :reason, updated_at = :now WHERE id = :c")
_REVOKE_LINKS = text("UPDATE lending.doc_chase_links SET revoked_at = :now WHERE chase_id = :c AND revoked_at IS NULL")
_INSERT_TOUCH = text("INSERT INTO lending.doc_chase_touches (chase_id, chase_day, action_id, created_at) "
                     "VALUES (:c, :day, :action, :now) ON CONFLICT DO NOTHING")


def _who(chase: dict[str, Any]) -> str:
    last4 = f" (…{chase['phone'][-4:]})" if chase["phone"] else ""
    return f"{chase['first_name'] or 'Borrower'}{last4}"


def _close(db, chase: dict[str, Any], status: str, reason: str, now: datetime, summary: str) -> None:
    db.execute(_CLOSE, {"status": status, "reason": reason, "now": now, "c": chase["id"]})
    db.execute(_REVOKE_LINKS, {"now": now, "c": chase["id"]})
    add_to_inbox_safely(db, event_type=f"doc_chase_{status}", dedup_key=str(chase["id"]), summary=summary,
                        payload={"chase_id": chase["id"], "reason": reason})
    logger.info("[doc-chase] chase %s %s (%s)", chase["id"], status, reason)


def _chase_step(db, gate: SendGate, chase: dict[str, Any], day: int, missing: tuple[str, ...], *,
                approvals_channel: str, now: datetime, text_consent: Callable[[str], bool]) -> bool:
    """Queue the day's message; True when a draft went to the gate. No usable channel raises one inbox item."""
    text_ok = bool(chase["phone"]) and text_consent(chase["phone"])
    if not text_ok and not chase["email"]:
        db.execute(_INSERT_TOUCH, {"c": chase["id"], "day": day, "action": None, "now": now})
        add_to_inbox_safely(db, event_type="doc_chase_no_channel", dedup_key=str(chase["id"]),
                            summary=f"{_who(chase)}: documents still missing and no text consent or email on file. "
                                    "Call them.", payload={"chase_id": chase["id"]})
        return False
    recipient = Recipient(chase["first_name"], chase["phone"], chase["email"], chase["ghl_contact_id"])
    draft = chase_draft("request" if day == 0 else f"d{day}", recipient, link=issue_link(db, chase["id"], now),
                        missing=missing, text_ok=text_ok,
                        contact_ref=f"ghl:{chase['ghl_contact_id']}" if chase["ghl_contact_id"] else f"doc_chase:{chase['id']}",
                        deal_ref=f"doc_chase:{chase['id']}")
    action_id = submit_draft(gate, draft, key=f"doc_chase:{chase['id']}:d{day}", channel=approvals_channel,
                             ttl=DRAFT_TTL)
    db.execute(_INSERT_TOUCH, {"c": chase["id"], "day": day, "action": action_id, "now": now})
    return True


def run_chase_sweep(db, gate: SendGate, *, approvals_channel: str, now: Optional[datetime] = None,
                    text_consent: Callable[[str], bool]) -> int:
    """One pass over the open chases: close finished or lost ones, queue the due step. Returns drafts queued.
    Each chase runs in its own savepoint, so one failure never stops the others; the caller commits."""
    now = now or datetime.now(timezone.utc)
    queued = 0
    for chase in [dict(row) for row in db.execute(_OPEN_CHASES).mappings()]:
        try:
            with db.begin_nested():
                missing = _missing(db, chase["id"])
                if chase["stage_key"] in CLOSED_STAGE_KEYS:
                    _close(db, chase, "cancelled", f"stage:{chase['stage_key']}", now,
                           f"{_who(chase)}: document chase stopped, the deal moved to {chase['stage_key']}.")
                elif not missing:
                    _close(db, chase, "complete", "all received", now,
                           f"{_who(chase)}: all documents received.")
                else:
                    day = due_chase_day(chase["accepted_at"], now, frozenset(chase["sent_days"] or ()))
                    if day is not None and _chase_step(db, gate, chase, day, missing,
                                                       approvals_channel=approvals_channel, now=now,
                                                       text_consent=text_consent):
                        queued += 1
        except Exception as exc:  # class only: SQL errors embed bound parameters
            logger.error("[doc-chase] chase %s skipped this sweep (%s)", chase["id"], type(exc).__name__)
    return queued


# ---- uploads -------------------------------------------------------------------------------------------------------

_ITEMS = text("SELECT item_code, label, received_at FROM lending.doc_chase_items WHERE chase_id = :c ORDER BY id")
_INSERT_UPLOAD = text("""
    INSERT INTO lending.doc_chase_uploads (chase_id, item_code, original_name, stored_path, content_type, size_bytes,
                                           uploaded_at)
    VALUES (:c, :code, :name, :path, :type, :size, :now)
""")
_MARK_RECEIVED = text("UPDATE lending.doc_chase_items SET received_at = COALESCE(received_at, :now) "
                      "WHERE chase_id = :c AND item_code = :code")


def chase_items(db, chase_id: int) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(_ITEMS, {"c": chase_id}).mappings()]


def _write_private(folder: str, extension: str, data: bytes) -> str:
    """Write under a generated name, readable by this user only; the borrower's file name never reaches the disk."""
    os.makedirs(folder, mode=0o700, exist_ok=True)
    os.chmod(folder, 0o700)
    path = os.path.join(folder, f"{uuid.uuid4().hex}{extension}")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
    return path


def record_upload(db, chase: dict[str, Any], item_code: str, upload: Upload, *, upload_dir: str,
                  now: datetime) -> None:
    """Store one document for one checklist item and tick it. Raises InvalidSubmission for a bad file or item."""
    items = {item["item_code"]: item for item in chase_items(db, chase["id"])}
    if item_code not in items:
        raise InvalidSubmission("choose which document this is")
    present = check_uploads([upload])
    if not present:
        raise InvalidSubmission("choose a file to upload")
    extension = os.path.splitext(upload.original_name)[1].lower()  # one of UPLOAD_TYPES, checked above
    path = _write_private(os.path.join(upload_dir, str(chase["id"])), extension, upload.data)
    try:
        with db.begin_nested():
            db.execute(_INSERT_UPLOAD, {"c": chase["id"], "code": item_code, "name": upload.original_name[:255],
                                        "path": path, "type": upload.content_type, "size": len(upload.data),
                                        "now": now})
            db.execute(_MARK_RECEIVED, {"now": now, "c": chase["id"], "code": item_code})
    except Exception:
        os.remove(path)
        raise
    received = sum(1 for item in items.values() if item["received_at"] is not None or item["item_code"] == item_code)
    add_to_inbox_safely(db, event_type="doc_chase_upload", dedup_key=f"{chase['id']}:{item_code}:{now.isoformat()}",
                        summary=f"{_who(chase)} uploaded {items[item_code]['label']}: {received} of {len(items)} "
                                "documents received.", payload={"chase_id": chase["id"], "item_code": item_code})
    logger.info("[doc-chase] chase %s: %s uploaded (%s/%s)", chase["id"], item_code, received, len(items))
