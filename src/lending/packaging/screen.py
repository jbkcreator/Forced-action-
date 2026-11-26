"""Screen declined and unassigned files against the secondary lenders and draft one package per lender that fits.

Separate from the router: it never routes a file. Every fitting lender gets a package (an unconfirmed box still
drafts, flagged); every lender that misses is listed with why. A file is re-drafted only when its facts or the
lender rules change (the fingerprint).
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lender_matrix import SECONDARY_LENDER_KEYS, LenderRules, has_placeholder_rules
from config.lending_occupancy import HOMESTEAD_HOLD_REASON, OWNER_OCCUPIED
from config.lending_partners import CREDIT_BAND_MAX_FICO, CREDIT_BANDS, PRODUCTS
from src.lending.contracts import BorrowerProfile, LenderFitResult, LoanRequest, LoanType
from src.lending.enrichment.fit_card import parse_band_bounds
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.lender_rules_store import current_matrix_with_version
from src.lending.packaging.package import LF_LEAD, LOAN_FILE, file_facts

logger = logging.getLogger(__name__)

EXCLUDED_HOLDS = ["suppressed", HOMESTEAD_HOLD_REASON]
UNASSIGNED_STATUSES = ["held", "unrouted", "exhausted"]

# (a) a decline from the primary lender (by id), or (b) unassigned; never a test file, a suppressed or
# owner-occupied hold, or a file a lender already approved or funded.
ELIGIBLE_SQL = text("""
    SELECT fr.lead_id, fr.loan_file_id
      FROM lending.file_routing fr
     WHERE NOT fr.is_test
       AND NOT (COALESCE(fr.hold_reason, '') = ANY(:excluded_holds))
       AND (fr.status = ANY(:unassigned)
            OR EXISTS (SELECT 1 FROM lending.file_routing_events e
                        WHERE e.action = 'declined'
                          AND e.lender_id = (SELECT id FROM lending.lender_routing_rules WHERE is_primary LIMIT 1)
                          AND (e.lead_id = fr.lead_id OR e.loan_file_id = fr.loan_file_id)))
       AND NOT EXISTS (SELECT 1 FROM lending.file_routing_events e
                        WHERE e.action IN ('approved', 'funded')
                          AND (e.lead_id = fr.lead_id OR e.loan_file_id = fr.loan_file_id))
     ORDER BY fr.id
""")
_EXISTING = text("SELECT lender_key, screen_fingerprint FROM lending.lender_packages "
                 "WHERE source_type = :st AND source_id = :sid")
_UPSERT = text("""
    INSERT INTO lending.lender_packages
        (source_type, source_id, lender_key, status, box_confirmed, fit_result, screen_fingerprint)
    VALUES (:st, :sid, :lk, 'draft', :confirmed, CAST(:fit AS jsonb), :fp)
    ON CONFLICT (source_type, source_id, lender_key) WHERE status NOT IN ('submitted', 'dismissed')
    DO UPDATE SET box_confirmed = EXCLUDED.box_confirmed, fit_result = EXCLUDED.fit_result,
                  screen_fingerprint = EXCLUDED.screen_fingerprint, updated_at = now()
    RETURNING id
""")
_LEAD_FLAGS = text("SELECT suppressed, is_test FROM lending.lendingflow_leads WHERE id = :id")
_FILE_FLAGS = text("SELECT occupancy, is_test FROM lending.loan_files WHERE id = :id")


@dataclass(frozen=True)
class FileRef:
    source_type: str
    source_id: int


class NotScreenable(ValueError):
    """The file lacks a fact the lender boxes need; it is skipped, never filled in with a guess."""


def eligible_files(db: Session) -> list[FileRef]:
    rows = db.execute(ELIGIBLE_SQL, {"excluded_holds": EXCLUDED_HOLDS, "unassigned": UNASSIGNED_STATUSES}).all()
    return [FileRef(LOAN_FILE, row.loan_file_id) if row.loan_file_id is not None else FileRef(LF_LEAD, row.lead_id)
            for row in rows]


def fit_inputs(source_type: str, facts: dict) -> tuple[BorrowerProfile, LoanRequest]:
    """Borrower and loan for the fit evaluator. Raises NotScreenable when loan type, amount or state is unknown."""
    state, amount = facts.get("property_state"), facts.get("loan_amount")
    if source_type == LOAN_FILE:
        loan_type = PRODUCTS.get(facts.get("product") or "")
        band = facts.get("credit_band") or ""
        low, high = CREDIT_BANDS.get(band), CREDIT_BAND_MAX_FICO.get(band)
    else:
        try:
            loan_type = LoanType(facts.get("product") or "")
        except ValueError:
            loan_type = None
        bounds = parse_band_bounds(facts.get("credit_band"))
        low, high = (bounds if bounds else (facts.get("credit_band_min_fico"), None))
    missing = [name for name, value in (("loan type", loan_type), ("loan amount", amount), ("state", state))
               if value in (None, "")]
    if missing:
        raise NotScreenable(", ".join(missing))
    borrower = BorrowerProfile(credit_band_min_fico=low, credit_band_max_fico=high,
                               completed_projects=facts.get("completed_projects"), state=state)
    loan = LoanRequest(loan_type=loan_type, loan_amount=Decimal(amount), state=state,
                       purchase_price=_money(facts.get("purchase_price")),
                       rehab_budget=_money(facts.get("rehab_budget")), arv=_money(facts.get("arv")),
                       property_address=facts.get("property_address"))
    return borrower, loan


def _money(value) -> Optional[Decimal]:
    return Decimal(value) if value is not None else None


def secondary_matrix() -> tuple[tuple[LenderRules, ...], str]:
    """The secondary lenders' current rules and a version string that changes whenever they do."""
    matrix, version = current_matrix_with_version()
    secondary = tuple(r for r in matrix if r.key in SECONDARY_LENDER_KEYS)
    if version is not None:
        return secondary, f"v{version}"
    return secondary, hashlib.sha256(repr(secondary).encode()).hexdigest()[:16]


def fingerprint(facts: dict, rules_version: str) -> str:
    blob = json.dumps({k: facts.get(k) for k in sorted(facts)}, sort_keys=True, default=str)
    return hashlib.sha256(f"{blob}|{rules_version}".encode()).hexdigest()[:64]


def fit_summary(fit: LenderFitResult, lender_key: str) -> dict:
    """What the card shows: this lender's fit (``needs`` = the facts still missing when the fit is only possible), and
    every other secondary lender that missed or is undetermined, with why."""
    fits = {f.lender_key: f for f in fit.fitting}
    maybe = {p.lender_key: p for p in fit.possible}
    return {
        "lender_key": lender_key,
        "subject_to_confirmation": fits[lender_key].subject_to_confirmation if lender_key in fits else True,
        "needs": list(maybe[lender_key].why_unknown) if lender_key in maybe else [],
        "misses": [{"lender_key": m.lender_key, "lender_name": m.lender_name, "reasons": list(m.reasons)}
                   for m in fit.non_fitting],
        "possible": [{"lender_key": p.lender_key, "lender_name": p.lender_name, "why": "; ".join(p.why_unknown)}
                     for p in fit.possible if p.lender_key != lender_key],
        "other_fits": [f.lender_key for f in fit.fitting if f.lender_key != lender_key],
    }


def _excluded(db: Session, ref: FileRef) -> Optional[str]:
    if ref.source_type == LF_LEAD:
        flags = db.execute(_LEAD_FLAGS, {"id": ref.source_id}).first()
        if flags is None:
            return "lead not found"
        return "suppressed" if flags.suppressed else ("test" if flags.is_test else None)
    flags = db.execute(_FILE_FLAGS, {"id": ref.source_id}).first()
    if flags is None:
        return "file not found"
    return "owner occupied" if flags.occupancy == OWNER_OCCUPIED else ("test" if flags.is_test else None)


def screen_file(db: Session, ref: FileRef, *, matrix: tuple[LenderRules, ...], rules_version: str) -> list[int]:
    """Draft or refresh this file's packages; returns the ids of packages that are new or changed."""
    excluded = _excluded(db, ref)
    if excluded:
        logger.info("[packaging] %s %s not screened: %s", ref.source_type, ref.source_id, excluded)
        return []
    facts = file_facts(db, ref.source_type, ref.source_id)
    if facts is None:
        return []
    try:
        borrower, loan = fit_inputs(ref.source_type, facts)
    except NotScreenable as missing:
        logger.info("[packaging] %s %s not screened: missing %s", ref.source_type, ref.source_id, missing)
        return []
    fit = evaluate_lender_fit(borrower, loan, matrix=matrix)
    fp = fingerprint(facts, rules_version)
    seen = {(row.lender_key, row.screen_fingerprint)
            for row in db.execute(_EXISTING, {"st": ref.source_type, "sid": ref.source_id})}
    rules_by_key = {r.key: r for r in matrix}
    changed: list[int] = []
    for lender in (*fit.fitting, *fit.possible):
        if (lender.lender_key, fp) in seen:
            continue  # same facts, same rules: already drafted (or dismissed / submitted)
        rules = rules_by_key.get(lender.lender_key)
        confirmed = rules is not None and not has_placeholder_rules(rules)
        package_id = db.execute(_UPSERT, {
            "st": ref.source_type, "sid": ref.source_id, "lk": lender.lender_key, "confirmed": confirmed,
            "fit": json.dumps(fit_summary(fit, lender.lender_key)), "fp": fp}).scalar_one()
        changed.append(package_id)
    return changed
