"""What an admin can do to a package or a document; shared by the Slack card and the staff API."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_closing import DOCUMENT_TYPES
from src.lending.packaging import quarantine
from src.lending.packaging.package import (
    CLOSED_STATUSES,
    DISMISSED,
    LOAN_FILE,
    SUBMITTED,
    CompileResult,
    compile_package,
)
from src.lending.packaging.packet_validator import PARTNER_FILE_UPLOADS, Outcome, doc_key

_CLOSE = text("UPDATE lending.lender_packages SET status = :status, updated_at = now() "
              "WHERE id = :id AND status NOT IN ('submitted', 'dismissed')")
_ADD_OVERRIDE = text("UPDATE lending.lender_packages SET overrides = overrides || CAST(:entry AS jsonb), "
                     "updated_at = now() WHERE id = :id")


class ActionRefused(Exception):
    """The action was not applied; ``str(exc)`` says why, in words for the person who clicked."""


def _open(db: Session, package_id: int, today: date) -> CompileResult:
    result = compile_package(db, package_id, today=today, renderer=None)
    if result is None:
        raise ActionRefused("That package is no longer open (submitted, dismissed or removed).")
    return result


def mark_submitted(db: Session, package_id: int, *, actor: str, today: date) -> int:
    """Re-validate now, then record the lender submission. Returns the lender_submissions id."""
    from src.lending.state_machine import record_lender_submission

    result = _open(db, package_id, today)
    if not result.box_confirmed:
        raise ActionRefused("This lender's box is not confirmed. Do not submit.")
    if result.verdict.outcome is not Outcome.PASS:
        raise ActionRefused("A document is blocked or quarantined. Override or replace it first.")
    if result.source_type != LOAN_FILE:
        raise ActionRefused("This is a LendingFlow lead with no loan file, so the submission cannot be recorded. "
                            "Open a loan file for the borrower first.")
    submission_id = record_lender_submission(db, result.source_id, result.lender_key, actor=actor)
    db.execute(_CLOSE, {"id": package_id, "status": SUBMITTED})
    quarantine.ledger(db, result.source_id, "package_submitted", actor=actor,
                      detail={"lender_key": result.lender_key, "submission_id": submission_id},
                      ref_table="lender_packages", ref_id=package_id)
    return submission_id


def dismiss(db: Session, package_id: int, *, actor: str) -> None:
    row = db.execute(text("SELECT source_type, source_id, lender_key, status FROM lending.lender_packages "
                          "WHERE id = :id"), {"id": package_id}).mappings().first()
    if row is None or row["status"] in CLOSED_STATUSES:
        raise ActionRefused("That package is no longer open.")
    db.execute(_CLOSE, {"id": package_id, "status": DISMISSED})
    quarantine.ledger(db, row["source_id"] if row["source_type"] == LOAN_FILE else None, "package_dismissed",
                      actor=actor, detail={"lender_key": row["lender_key"]}, ref_table="lender_packages",
                      ref_id=package_id)


def override_block(db: Session, package_id: int, upload_table: str, upload_id: int, *, reason: str, actor: str,
                   today: date) -> CompileResult:
    """Clear a name / age / readability block on one document for this package only. A quarantine is never cleared."""
    if not reason.strip():
        raise ActionRefused("An override needs a reason.")
    result = _open(db, package_id, today)
    key = doc_key(upload_table, upload_id)
    doc = next((d for d in result.verdict.per_doc if d.key == key), None)
    if doc is None:
        raise ActionRefused("That document is not part of this package.")
    if doc.outcome is Outcome.QUARANTINE:
        raise ActionRefused("A quarantined document cannot be overridden; an admin must release it.")
    if doc.outcome is not Outcome.BLOCK:
        raise ActionRefused("That document is not blocked.")
    entry = {key: {"reason": reason.strip()[:300], "by": actor, "at": datetime.now(timezone.utc).isoformat(),
                   "blocked_for": list(doc.reasons)}}
    db.execute(_ADD_OVERRIDE, {"id": package_id, "entry": json.dumps(entry)})
    quarantine.ledger(db, result.source_id if result.source_type == LOAN_FILE else None, "doc_block_override",
                      actor=actor, detail={"package_id": package_id, "reason": reason.strip()[:300],
                                           "blocked_for": list(doc.reasons)},
                      ref_table=upload_table, ref_id=upload_id)
    return _open(db, package_id, today)


def tag_document_type(db: Session, upload_id: int, item_code: str, *, actor: str) -> None:
    """Set the type of a partner-door upload, so it can be checked and used."""
    if item_code not in DOCUMENT_TYPES:
        raise ActionRefused(f"Unknown document type {item_code!r}.")
    file_id = db.execute(text("SELECT file_id FROM lending.partner_file_uploads WHERE id = :id"),
                         {"id": upload_id}).scalar()
    if file_id is None:
        raise ActionRefused("No such partner document.")
    db.execute(text("UPDATE lending.partner_file_uploads SET item_code = :code WHERE id = :id"),
               {"id": upload_id, "code": item_code})
    quarantine.ledger(db, file_id, "doc_type_tagged", actor=actor, detail={"item_code": item_code},
                      ref_table=PARTNER_FILE_UPLOADS, ref_id=upload_id)


def release_document(db: Session, upload_table: str, upload_id: int, *, reason: str, actor: str) -> None:
    if not reason.strip():
        raise ActionRefused("A release needs a reason.")
    try:
        quarantine.release(db, upload_table, upload_id, reason=reason.strip(), actor=actor)
    except quarantine.UnknownUpload:
        raise ActionRefused("No such document.")
    except ValueError as exc:
        raise ActionRefused(str(exc))
