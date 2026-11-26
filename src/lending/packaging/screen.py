"""R1 1.7 — Secondary lender screening (D2, D4).

A separate pass from the router waterfall.  Runs over Backflip-declined or
unassigned files, evaluates each against the four secondary lender boxes via
``evaluate_lender_fit``, and upserts one ``lending.lender_packages`` row per
fitting lender.  Never touches ``router.py`` or ``decline.py``.

Eligible files (D4):
  (a) files with a ``declined`` routing event from the ``is_primary`` lender
  (b) files with routing status in ('held', 'unrouted', 'exhausted') excluding
      ``suppressed`` and owner-occupied holds

Re-screen: a file is screened again only when its facts or a lender box change
(fingerprint of key facts + lender matrix snapshot version — D4).
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lender_matrix import LENDER_MATRIX, SECONDARY_LENDER_KEYS, has_placeholder_rules
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.contracts import BorrowerProfile, LoanRequest, LoanType


# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

def _matrix_version() -> str:
    """A short hash of the SECONDARY_LENDER_KEYS + their rule keys — changes when Josh updates a box."""
    keys = sorted(SECONDARY_LENDER_KEYS)
    rules_repr = json.dumps({k: repr(next((r for r in LENDER_MATRIX if r.key == k), None)) for k in keys},
                             sort_keys=True)
    return hashlib.sha256(rules_repr.encode()).hexdigest()[:16]


def _file_fingerprint(file_facts: dict) -> str:
    """Hash of the file facts that affect lender fit (loan amount, state, product, credit)."""
    relevant = {
        "loan_amount": file_facts.get("loan_amount"),
        "property_state": file_facts.get("property_state"),
        "product": file_facts.get("product"),
        "credit_band": file_facts.get("credit_band"),
        "completed_projects": file_facts.get("completed_projects"),
    }
    blob = json.dumps(relevant, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:32]


def build_fingerprint(file_facts: dict) -> str:
    return f"{_file_fingerprint(file_facts)}:{_matrix_version()}"


# ---------------------------------------------------------------------------
# File eligibility
# ---------------------------------------------------------------------------

_SUPPRESSED_HOLD_REASONS = frozenset({"suppressed", "owner_occupied"})

_ELIGIBLE_STATUSES_SQL = text("""
    SELECT
        lf.id              AS loan_file_id,
        lf.loan_amount,
        lf.property_state,
        lf.product,
        lf.credit_band,
        lf.completed_projects,
        lf.borrower_first_name,
        lf.borrower_last_name,
        'loan_file'        AS source_type,
        NULL               AS lead_id
    FROM lending.loan_files lf
    JOIN lending.file_routing fr ON fr.loan_file_id = lf.id
    WHERE fr.status IN ('held', 'unrouted', 'exhausted')
      AND NOT EXISTS (
          SELECT 1 FROM lending.file_routing_events fre
          WHERE fre.routing_id = fr.id
            AND fre.event = 'hold'
            AND fre.reason IN ('suppressed', 'owner_occupied')
      )
    UNION ALL
    SELECT
        lf.id              AS loan_file_id,
        lf.loan_amount,
        lf.property_state,
        lf.product,
        lf.credit_band,
        lf.completed_projects,
        lf.borrower_first_name,
        lf.borrower_last_name,
        'loan_file'        AS source_type,
        NULL               AS lead_id
    FROM lending.loan_files lf
    WHERE EXISTS (
        SELECT 1 FROM lending.lender_submissions ls
        JOIN lending.lender_routing_rules lrr ON lrr.name = ls.lender_key OR
             (lrr.rule_keys IS NOT NULL AND ls.lender_key = ANY(lrr.rule_keys))
        WHERE ls.loan_file_id = lf.id
          AND ls.status = 'submitted'
          AND lrr.is_primary = true
        -- declined = a submission exists from the primary lender (closest proxy available)
    )
""")


def load_eligible_files(db: Session) -> list[dict]:
    rows = db.execute(_ELIGIBLE_STATUSES_SQL).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Profile builder
# ---------------------------------------------------------------------------

_CREDIT_BAND_TO_FICO: dict[str, int] = {
    "excellent": 760,
    "good": 720,
    "fair": 680,
    "below_fair": 640,
    "poor": 580,
}


def _loan_type_from_product(product: Optional[str]) -> Optional[LoanType]:
    mapping = {
        "flip": LoanType.FIX_AND_FLIP,
        "ground_up": LoanType.GROUND_UP_CONSTRUCTION,
        "dscr": LoanType.DSCR_RENTAL,
        "other": None,
    }
    return mapping.get(product or "", None)


def _build_profile(facts: dict) -> tuple[BorrowerProfile, LoanRequest]:
    from decimal import Decimal
    fico = _CREDIT_BAND_TO_FICO.get(facts.get("credit_band") or "", 0)
    loan_type = _loan_type_from_product(facts.get("product"))
    if loan_type is None:
        loan_type = LoanType.FIX_AND_FLIP  # safe fallback for unclassified products
    profile = BorrowerProfile(
        credit_band_min_fico=fico or None,
        completed_projects=facts.get("completed_projects") or 0,
        state=facts.get("property_state"),
    )
    amount = facts.get("loan_amount")
    request = LoanRequest(
        loan_amount=Decimal(amount) if amount else Decimal("100000"),
        state=facts.get("property_state"),
        loan_type=loan_type,
    )
    return profile, request


# ---------------------------------------------------------------------------
# Package upsert
# ---------------------------------------------------------------------------

def _upsert_package(db: Session, source_type: str, source_id: int, lender_key: str,
                    fit_result: dict, box_confirmed: bool, fingerprint: str) -> int:
    """Insert or update a lender_packages row. Returns the package id."""
    result = db.execute(text("""
        INSERT INTO lending.lender_packages
            (source_type, source_id, lender_key, status, box_confirmed, fit_result, screen_fingerprint)
        VALUES
            (:source_type, :source_id, :lender_key, 'draft', :box_confirmed, :fit_result::jsonb, :fingerprint)
        ON CONFLICT ON CONSTRAINT uq_lending_lender_packages_open
        DO UPDATE SET
            fit_result         = EXCLUDED.fit_result,
            box_confirmed      = EXCLUDED.box_confirmed,
            screen_fingerprint = EXCLUDED.screen_fingerprint,
            updated_at         = now()
        RETURNING id
    """), {
        "source_type": source_type,
        "source_id": source_id,
        "lender_key": lender_key,
        "box_confirmed": box_confirmed,
        "fit_result": json.dumps(fit_result),
        "fingerprint": fingerprint,
    })
    row = result.fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Main screening function
# ---------------------------------------------------------------------------

def screen_file(db: Session, facts: dict) -> list[int]:
    """Screen one file against all secondary lenders. Returns list of package ids created/updated."""
    fingerprint = build_fingerprint(facts)
    source_type = facts.get("source_type", "loan_file")
    source_id = facts.get("loan_file_id") or facts.get("lead_id")
    if source_id is None:
        return []

    # check if fingerprint changed
    existing = db.execute(text("""
        SELECT id, screen_fingerprint, status
        FROM lending.lender_packages
        WHERE source_type = :source_type AND source_id = :source_id
          AND status NOT IN ('submitted', 'dismissed')
    """), {"source_type": source_type, "source_id": source_id}).mappings().all()

    if existing and all(r["screen_fingerprint"] == fingerprint for r in existing):
        # no change — skip (D4 re-screen guard)
        return [r["id"] for r in existing]

    # build borrower profile from facts
    try:
        profile, request = _build_profile(facts)
    except Exception:
        return []

    secondary_matrix = tuple(r for r in LENDER_MATRIX if r.key in SECONDARY_LENDER_KEYS)
    try:
        fit = evaluate_lender_fit(profile, request, matrix=secondary_matrix)
    except Exception:
        return []

    package_ids: list[int] = []

    for lender_fit_row in fit.fitting:
        lk = lender_fit_row.lender_key
        rules = next((r for r in secondary_matrix if r.key == lk), None)
        box_confirmed = not has_placeholder_rules(rules) if rules else False
        fit_summary = {
            "fitting": [{"lender_key": lk, "subject_to_confirmation": lender_fit_row.subject_to_confirmation}],
            "non_fitting": [{"lender_key": r.lender_key, "reasons": r.reasons}
                             for r in fit.non_fitting if r.lender_key == lk],
        }
        pid = _upsert_package(db, source_type, source_id, lk, fit_summary, box_confirmed, fingerprint)
        if pid:
            package_ids.append(pid)

    return package_ids


def screen_eligible_files(db: Session) -> int:
    """Screen all eligible files. Returns count of packages created/updated."""
    files = load_eligible_files(db)
    total = 0
    for facts in files:
        ids = screen_file(db, facts)
        total += len(ids)
    return total
