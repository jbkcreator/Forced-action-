"""Quarantine of borrower uploads: mark the row, write the deal ledger, alert ops, and the admin-only release."""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.audit import deal_ledger
from src.lending.packaging.packet_validator import (
    DOC_CHASE_UPLOADS,
    PARTNER_FILE_UPLOADS,
    UPLOAD_TABLES,
    DocInput,
    Outcome,
    validate_injection_only,
)

logger = logging.getLogger(__name__)

QUARANTINED = "quarantined"
RELEASED = "released"

_LOAN_FILE_OF = {
    PARTNER_FILE_UPLOADS: text("SELECT file_id FROM lending.partner_file_uploads WHERE id = :id"),
    DOC_CHASE_UPLOADS: text("SELECT c.loan_file_id FROM lending.doc_chase_uploads u "
                            "JOIN lending.doc_chases c ON c.id = u.chase_id WHERE u.id = :id"),
}


class UnknownUpload(LookupError):
    pass


def _table(upload_table: str) -> str:
    if upload_table not in UPLOAD_TABLES:
        raise ValueError(f"unknown upload table {upload_table!r}")
    return upload_table


def loan_file_of(db: Session, upload_table: str, upload_id: int) -> Optional[int]:
    return db.execute(_LOAN_FILE_OF[_table(upload_table)], {"id": upload_id}).scalar()


def ledger(db: Session, loan_file_id: Optional[int], event_type: str, *, actor: str, detail: dict,
           ref_table: str, ref_id: int, dedupe_key: Optional[str] = None) -> None:
    """A deal-ledger row, or a warning when the document's file is a lead with no loan file (the ledger needs one)."""
    if loan_file_id is None:
        logger.warning("[packaging] %s on %s %s not ledgered: no loan file", event_type, ref_table, ref_id)
        return
    deal_ledger.record(db, loan_file_id, event_type, actor=actor, detail=detail, ref_table=ref_table, ref_id=ref_id,
                       dedupe_key=dedupe_key)


def record_quarantine(db: Session, upload_table: str, upload_id: int, reason: str) -> bool:
    """Quarantine the upload (first reason kept). True when this call quarantined it. The caller commits."""
    table = _table(upload_table)
    changed = db.execute(text(
        f"UPDATE lending.{table} SET quarantine_status = :q, quarantine_reason = :reason "
        "WHERE id = :id AND quarantine_status IS NULL"), {"id": upload_id, "q": QUARANTINED, "reason": reason[:300]},
    ).rowcount
    if changed:
        ledger(db, loan_file_of(db, table, upload_id), "doc_quarantined", actor=deal_ledger.system("packet_validator"),
               detail={"reason": reason[:300]}, ref_table=table, ref_id=upload_id,
               dedupe_key=f"doc_quarantined:{table}:{upload_id}")
    return bool(changed)


def alert_quarantine(upload_table: str, upload_id: int, reasons: tuple[str, ...]) -> None:
    """One ops alert naming the upload and why. Never raises."""
    try:
        from src.lending.ops_alerts import post_ops_alert

        post_ops_alert(f":no_entry: Document quarantined ({upload_table} #{upload_id}): {'; '.join(reasons)[:300]}. "
                       "It is kept on disk and kept out of view links, packages, AI and lenders until an admin "
                       "releases it.")
    except Exception as exc:
        logger.error("[packaging] quarantine alert failed for %s %s (%s)", upload_table, upload_id, type(exc).__name__)


def quarantine_and_alert(db: Session, upload_table: str, upload_id: int, reasons: tuple[str, ...]) -> None:
    """Quarantine, commit, then alert once. The caller's session is committed here."""
    if record_quarantine(db, upload_table, upload_id, "; ".join(reasons)):
        db.commit()
        alert_quarantine(upload_table, upload_id, reasons)


def release(db: Session, upload_table: str, upload_id: int, *, reason: str, actor: str) -> None:
    """Admin release of a quarantined upload, ledgered with who and why. The caller commits."""
    table = _table(upload_table)
    status = db.execute(text(f"SELECT quarantine_status FROM lending.{table} WHERE id = :id"), {"id": upload_id}).first()
    if status is None:
        raise UnknownUpload(upload_id)
    if status[0] != QUARANTINED:
        raise ValueError("that document is not quarantined")
    db.execute(text(f"UPDATE lending.{table} SET quarantine_status = :r WHERE id = :id"), {"id": upload_id, "r": RELEASED})
    ledger(db, loan_file_of(db, table, upload_id), "doc_released", actor=actor, detail={"reason": reason[:300]},
           ref_table=table, ref_id=upload_id)


def scan_upload(db: Session, upload_table: str, upload_id: int, stored_path: str) -> bool:
    """The upload-time injection scan of one stored upload; True when it was quarantined. Commits on a hit."""
    verdict = validate_injection_only(DocInput(upload_id, _table(upload_table), None, stored_path))
    if verdict.outcome is Outcome.QUARANTINE:
        quarantine_and_alert(db, upload_table, upload_id, verdict.reasons)
        return True
    return False


def quarantine_before_commit(db: Session, upload_table: str, stored_path: str) -> Optional[tuple[int, tuple[str, ...]]]:
    """Scan the upload just written at ``stored_path`` inside the uploader's still-open transaction, so no other
    reader (the Drive copier) sees the row before its verdict. Fails closed: a scan error quarantines. Never commits;
    returns (upload id, reasons) when it quarantined, for ``alert_quarantine`` once the caller has committed."""
    table = _table(upload_table)
    upload_id = db.execute(text(f"SELECT id FROM lending.{table} WHERE stored_path = :p ORDER BY id DESC LIMIT 1"),
                           {"p": stored_path}).scalar()
    if upload_id is None:
        return None
    try:
        verdict = validate_injection_only(DocInput(upload_id, table, None, stored_path))
        reasons = verdict.reasons if verdict.outcome is Outcome.QUARANTINE else ()
    except Exception as exc:  # class only
        reasons = (f"scan failed ({type(exc).__name__})",)
    if reasons and record_quarantine(db, table, upload_id, "; ".join(reasons)):
        return upload_id, tuple(reasons)
    return None


def scan_new_uploads(db: Session, upload_table: str, *, partner_file_id: int) -> None:
    """Scan every unscanned upload of a partner file. Never raises: the uploader's response must not depend on it (a
    failed scan is logged; the package check scans again). Nothing copies partner uploads before that check."""
    table = _table(upload_table)
    try:
        rows = db.execute(text("SELECT id, stored_path FROM lending.partner_file_uploads "
                               "WHERE file_id = :f AND quarantine_status IS NULL"), {"f": partner_file_id}).all()
        for upload_id, path in rows:
            scan_upload(db, table, upload_id, path)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        db.rollback()
        logger.error("[packaging] upload scan failed on %s (%s)", table, type(exc).__name__)
