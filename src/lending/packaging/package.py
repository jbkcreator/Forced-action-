"""R1 1.7 — Package builder (D15, D16, D17, D18).

Compiles a lender package from a ``lending.lender_packages`` row:
  1. Runs the full validator over all docs tied to the file.
  2. Builds the missing-item checklist (required items minus passing docs).
  3. Renders the deal-facts PDF (lender-neutral, generalised from the Milo packet template).
  4. Updates the ``lending.lender_packages`` row with checklist + verdict + status.

The Milo (Central Lending) builder is NOT modified (D16, D39).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_closing import LENDER_CHECKLISTS
from config.settings import get_settings
from src.lending.packaging.packet_validator import (
    DocInput,
    FileContext,
    Outcome,
    Verdict,
    validate,
)


# ---------------------------------------------------------------------------
# Doc lookup helpers
# ---------------------------------------------------------------------------

def _load_docs_for_loan_file(db: Session, loan_file_id: int) -> list[DocInput]:
    """Return all non-quarantined uploads tied to a loan file id."""
    rows = db.execute(text("""
        SELECT
            u.id            AS upload_id,
            'doc_chase_uploads' AS upload_table,
            u.item_code,
            u.stored_path,
            u.quarantine_status
        FROM lending.doc_chase_uploads u
        JOIN lending.doc_chases dc ON dc.id = u.chase_id
        WHERE dc.loan_file_id = :fid
          AND (u.quarantine_status IS NULL OR u.quarantine_status <> 'quarantined')
        UNION ALL
        SELECT
            u.id            AS upload_id,
            'partner_file_uploads' AS upload_table,
            u.item_code,
            u.stored_path,
            u.quarantine_status
        FROM lending.partner_file_uploads u
        WHERE u.file_id = :fid
          AND (u.quarantine_status IS NULL OR u.quarantine_status <> 'quarantined')
    """), {"fid": loan_file_id}).mappings().all()

    return [
        DocInput(
            upload_id=r["upload_id"],
            upload_table=r["upload_table"],
            item_code=r["item_code"],
            stored_path=r["stored_path"],
        )
        for r in rows
        if Path(r["stored_path"]).exists()
    ]


def _load_file_context(db: Session, source_type: str, source_id: int) -> FileContext:
    """Build the FileContext (borrower names + other known names) for a file."""
    if source_type == "loan_file":
        row = db.execute(text("""
            SELECT borrower_first_name, borrower_last_name
            FROM lending.loan_files WHERE id = :id
        """), {"id": source_id}).mappings().first()
        if not row:
            return FileContext(file_id=source_id, source_type=source_type, borrower_names=())
        names: list[str] = []
        if row["borrower_first_name"]:
            names.append(row["borrower_first_name"])
        if row["borrower_last_name"]:
            names.append(row["borrower_last_name"])
        full = f"{row['borrower_first_name'] or ''} {row['borrower_last_name'] or ''}".strip()
        if full:
            names.append(full)
        # other file borrowers (cross-contamination check)
        others_rows = db.execute(text("""
            SELECT DISTINCT borrower_first_name || ' ' || borrower_last_name AS name
            FROM lending.loan_files
            WHERE id <> :id AND borrower_last_name IS NOT NULL
        """), {"id": source_id}).scalars().all()
        return FileContext(
            file_id=source_id,
            source_type=source_type,
            borrower_names=tuple(names),
            other_known_names=tuple(n for n in others_rows if n.strip()),
        )
    # LF lead — first/last only (D19)
    return FileContext(file_id=source_id, source_type=source_type, borrower_names=())


# ---------------------------------------------------------------------------
# Checklist builder
# ---------------------------------------------------------------------------

def _build_checklist(lender_key: str, docs: list[DocInput], verdict: Verdict) -> list[dict]:
    """Required items minus items held with a passing verdict (D7, D17)."""
    items = LENDER_CHECKLISTS.get(lender_key, ())
    passing_codes: set[str] = set()
    for dv in verdict.per_doc:
        if dv.outcome is Outcome.PASS and dv.item_code:
            passing_codes.add(dv.item_code)
    return [
        {
            "item_code": item.code,
            "label": item.label,
            "received": item.code in passing_codes,
            "verdict": "pass" if item.code in passing_codes else "missing",
        }
        for item in items
    ]


# ---------------------------------------------------------------------------
# Public compile entry point
# ---------------------------------------------------------------------------

@dataclass
class CompileResult:
    package_id: int
    lender_key: str
    status: str  # 'ready' | 'blocked' | 'draft'
    checklist: list[dict]
    verdict: Verdict
    box_confirmed: bool


def compile_package(db: Session, package_id: int, *, today: Optional[date] = None) -> Optional[CompileResult]:
    """Validate docs, build checklist, update the lender_packages row. Returns None if package not found."""
    s = get_settings()
    pkg_row = db.execute(text("""
        SELECT id, source_type, source_id, lender_key, box_confirmed, status
        FROM lending.lender_packages WHERE id = :id
    """), {"id": package_id}).mappings().first()
    if not pkg_row or pkg_row["status"] in ("submitted", "dismissed"):
        return None

    source_type = pkg_row["source_type"]
    source_id = pkg_row["source_id"]
    lender_key = pkg_row["lender_key"]

    # load docs
    if source_type == "loan_file":
        docs = _load_docs_for_loan_file(db, source_id)
    else:
        docs = []  # LF lead path — no doc chase yet

    file_ctx = _load_file_context(db, source_type, source_id)

    verdict = validate(
        file_ctx,
        docs,
        today=today,
        bank_max_days=s.lending_bank_statement_max_age_days,
        pricing_max_days=s.lending_pricing_sheet_max_age_days,
    )

    checklist = _build_checklist(lender_key, docs, verdict)

    if verdict.outcome is Outcome.QUARANTINE:
        new_status = "blocked"
    elif verdict.outcome is Outcome.BLOCK:
        new_status = "blocked"
    elif not pkg_row["box_confirmed"]:
        new_status = "draft"  # unconfirmed box keeps it in draft
    else:
        new_status = "ready"

    verdict_json = json.dumps({
        "outcome": verdict.outcome.value,
        "per_doc": [
            {
                "upload_id": dv.upload_id,
                "upload_table": dv.upload_table,
                "item_code": dv.item_code,
                "outcome": dv.outcome.value,
                "reasons": list(dv.reasons),
            }
            for dv in verdict.per_doc
        ],
    })
    checklist_json = json.dumps(checklist)

    db.execute(text("""
        UPDATE lending.lender_packages
        SET status = :status, checklist = :checklist::jsonb, validator_verdict = :verdict::jsonb,
            updated_at = now()
        WHERE id = :id
    """), {
        "id": package_id,
        "status": new_status,
        "checklist": checklist_json,
        "verdict": verdict_json,
    })

    return CompileResult(
        package_id=package_id,
        lender_key=lender_key,
        status=new_status,
        checklist=checklist,
        verdict=verdict,
        box_confirmed=pkg_row["box_confirmed"],
    )
