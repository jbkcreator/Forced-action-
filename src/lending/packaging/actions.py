"""What an admin can do to a package or a document; shared by the Slack card and the staff API."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_closing import DOCUMENT_TYPES
from src.lending.packaging import quarantine, screen
from src.lending.packaging.package import (
    CLOSED_STATUSES,
    DISMISSED,
    LOAN_FILE,
    SUBMITTED,
    CompileResult,
    compile_package,
)
from src.lending.packaging.packet_validator import DOC_CHASE_UPLOADS, PARTNER_FILE_UPLOADS, UPLOAD_TABLES, Outcome, doc_key

_CLOSE = text("UPDATE lending.lender_packages SET status = :status, updated_at = now() "
              "WHERE id = :id AND status NOT IN ('submitted', 'dismissed')")
# Held until the transaction ends, so two clicks on one package run one after the other, never both on an open row.
_LOCK = text("SELECT id FROM lending.lender_packages WHERE id = :id FOR UPDATE")
_ADD_OVERRIDE = text("UPDATE lending.lender_packages SET overrides = overrides || CAST(:entry AS jsonb), "
                     "updated_at = now() WHERE id = :id")


class ActionRefused(Exception):
    """The action was not applied; ``str(exc)`` says why, in words for the person who clicked."""


def _open(db: Session, package_id: int, today: date) -> CompileResult:
    db.execute(_LOCK, {"id": package_id})
    result = compile_package(db, package_id, today=today, renderer=None)
    if result is None:
        raise ActionRefused("That package is no longer open (submitted, dismissed or removed).")
    return result


def mark_submitted(db: Session, package_id: int, *, actor: str, today: date) -> int:
    """Re-validate the documents and re-check the fit against today's facts and rules, then close the package and
    record the lender submission in the same transaction. Returns the lender_submissions id."""
    from src.lending.state_machine import record_lender_submission

    result = _open(db, package_id, today)
    if not result.box_confirmed:
        raise ActionRefused("This lender's box is not confirmed. Do not submit.")
    if result.fit_needs:
        raise ActionRefused("The fit is not confirmed yet: " + "; ".join(result.fit_needs) + ".")
    if result.verdict.outcome is not Outcome.PASS:
        raise ActionRefused("A document is blocked or quarantined. Override or replace it first.")
    if result.source_type != LOAN_FILE:
        raise ActionRefused("This is a LendingFlow lead with no loan file, so the submission cannot be recorded. "
                            "Open a loan file for the borrower first.")
    problem = screen.current_fit_problem(db, result.source_type, result.source_id, result.lender_key)
    if problem:
        raise ActionRefused(problem)
    if db.execute(_CLOSE, {"id": package_id, "status": SUBMITTED}).rowcount != 1:
        raise ActionRefused("That package is no longer open (submitted, dismissed or removed).")
    submission_id = record_lender_submission(db, result.source_id, result.lender_key, actor=actor)
    quarantine.ledger(db, result.source_id, "package_submitted", actor=actor,
                      detail={"lender_key": result.lender_key, "submission_id": submission_id},
                      ref_table="lender_packages", ref_id=package_id)
    return submission_id


def dismiss(db: Session, package_id: int, *, actor: str) -> None:
    row = db.execute(text("SELECT source_type, source_id, lender_key, status FROM lending.lender_packages "
                          "WHERE id = :id FOR UPDATE"), {"id": package_id}).mappings().first()
    if row is None or row["status"] in CLOSED_STATUSES:
        raise ActionRefused("That package is no longer open.")
    if db.execute(_CLOSE, {"id": package_id, "status": DISMISSED}).rowcount != 1:
        raise ActionRefused("That package is no longer open.")
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


_UPLOAD_OWNER = {
    DOC_CHASE_UPLOADS: text("SELECT c.loan_file_id, c.lead_ref, u.item_code, u.superseded_by "
                            "FROM lending.doc_chase_uploads u JOIN lending.doc_chases c ON c.id = u.chase_id "
                            "WHERE u.id = :id"),
    PARTNER_FILE_UPLOADS: text("SELECT file_id AS loan_file_id, NULL AS lead_ref, item_code, superseded_by "
                               "FROM lending.partner_file_uploads WHERE id = :id"),
}


def _upload_owner(db: Session, upload_table: str, upload_id: int):
    if upload_table not in UPLOAD_TABLES:
        raise ActionRefused(f"Unknown document table {upload_table!r}.")
    row = db.execute(_UPLOAD_OWNER[upload_table], {"id": upload_id}).mappings().first()
    if row is None:
        raise ActionRefused(f"No such document #{upload_id}.")
    return row


def supersede_document(db: Session, upload_table: str, upload_id: int, by_table: str, by_id: int, *,
                       actor: str) -> None:
    """Mark one upload as replaced by a newer upload of the same type on the same file. The old upload and its
    history are kept; packages stop checking it. A quarantine on it stays (no link, no Drive copy)."""
    if (upload_table, upload_id) == (by_table, by_id):
        raise ActionRefused("A document cannot replace itself.")
    old, new = _upload_owner(db, upload_table, upload_id), _upload_owner(db, by_table, by_id)
    same_file = (old["loan_file_id"] is not None and old["loan_file_id"] == new["loan_file_id"]) or (
        old["loan_file_id"] is None and new["loan_file_id"] is None and old["lead_ref"] == new["lead_ref"])
    if not same_file:
        raise ActionRefused("The replacement must be a document on the same file.")
    if old["item_code"] is None or old["item_code"] != new["item_code"]:
        raise ActionRefused("The replacement must be a document of the same type.")
    if new["superseded_by"] is not None:
        raise ActionRefused(f"#{by_id} has itself been replaced.")
    by_key = doc_key(by_table, by_id)
    updated = db.execute(text(f"UPDATE lending.{upload_table} SET superseded_by = :by "
                              "WHERE id = :id AND superseded_by IS NULL"), {"id": upload_id, "by": by_key}).rowcount
    if updated != 1:
        raise ActionRefused(f"#{upload_id} has already been replaced.")
    quarantine.ledger(db, old["loan_file_id"], "doc_superseded", actor=actor, detail={"superseded_by": by_key},
                      ref_table=upload_table, ref_id=upload_id)


_ADD_GUARANTOR = text("""
    INSERT INTO lending.loan_file_guarantors (loan_file_id, name, added_by) VALUES (:id, :name, :actor)
    ON CONFLICT (loan_file_id, name) DO NOTHING
    RETURNING id
""")


def add_guarantor(db: Session, loan_file_id: int, name: str, *, actor: str) -> bool:
    """Record a guarantor on a loan file, so their ID and statements pass the name check. False when already there."""
    clean = " ".join(name.split())
    if len(clean.split(" ")) < 2 or len(clean) > 200:
        raise ActionRefused("Give the guarantor's full name (first and last).")
    if db.execute(text("SELECT 1 FROM lending.loan_files WHERE id = :id"), {"id": loan_file_id}).first() is None:
        raise ActionRefused("No such loan file.")
    if db.execute(_ADD_GUARANTOR, {"id": loan_file_id, "name": clean, "actor": actor}).first() is None:
        return False
    quarantine.ledger(db, loan_file_id, "guarantor_added", actor=actor, detail={"name": clean},
                      ref_table="loan_files", ref_id=loan_file_id)
    return True
