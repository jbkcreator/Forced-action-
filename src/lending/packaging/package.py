"""Compile a drafted lender package: validate its documents, build the checklist, render the deal-facts PDF."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lender_matrix import LENDER_MATRIX
from config.lending_closing import LENDER_CHECKLISTS
from config.lending_packaging import DEAL_FACTS_TEMPLATE, DEAL_FACTS_WATERMARK
from config.settings import get_settings
from src.lending.audit import deal_ledger
from src.lending.packaging import quarantine
from src.lending.packaging.packet_validator import (
    DOC_CHASE_UPLOADS,
    PARTNER_FILE_UPLOADS,
    AgeLimits,
    DocInput,
    FileContext,
    Outcome,
    Verdict,
    validate,
)

logger = logging.getLogger(__name__)

LOAN_FILE = "loan_file"
LF_LEAD = "lf_lead"
DRAFT, BLOCKED, READY, SUBMITTED, DISMISSED = "draft", "blocked", "ready", "submitted", "dismissed"
CLOSED_STATUSES = (SUBMITTED, DISMISSED)

PdfRenderer = Callable[[str, dict], bytes]

_PACKAGE = text("SELECT id, source_type, source_id, lender_key, status, box_confirmed, fit_result, overrides "
                "FROM lending.lender_packages WHERE id = :id")
_LOAN_FILE_DOCS = text("""
    SELECT u.id, :chase AS upload_table, u.item_code, u.stored_path, u.quarantine_status, u.quarantine_reason
      FROM lending.doc_chase_uploads u JOIN lending.doc_chases c ON c.id = u.chase_id
     WHERE c.loan_file_id = :id
    UNION ALL
    SELECT u.id, :partner AS upload_table, u.item_code, u.stored_path, u.quarantine_status, u.quarantine_reason
      FROM lending.partner_file_uploads u WHERE u.file_id = :id
""")
_LEAD_DOCS = text("""
    SELECT u.id, :chase AS upload_table, u.item_code, u.stored_path, u.quarantine_status, u.quarantine_reason
      FROM lending.doc_chase_uploads u JOIN lending.doc_chases c ON c.id = u.chase_id
     WHERE c.lead_ref = :ref AND c.loan_file_id IS NULL
""")
_LOAN_FILE_FACTS = text("""
    SELECT f.id, f.borrower_first_name AS first_name, f.borrower_last_name AS last_name, f.borrower_phone AS phone,
           f.product, f.loan_amount, f.purchase_price, f.rehab_budget, f.arv, f.property_address, f.property_city,
           f.property_state, f.property_zip, f.credit_band, NULL AS credit_band_min_fico, f.completed_projects,
           e.name AS entity_name
      FROM lending.loan_files f LEFT JOIN lending.entities e ON e.id = f.entity_id
     WHERE f.id = :id
""")
_LEAD_FACTS = text("""
    SELECT id, first_name, last_name, phone, loan_type AS product, loan_amount, NULL AS purchase_price,
           NULL AS rehab_budget, NULL AS arv, property_address, property_city, property_state, property_zip,
           credit_band, credit_band_min_fico, NULL AS completed_projects, NULL AS entity_name
      FROM lending.lendingflow_leads WHERE id = :id
""")
_GUARANTORS = text("SELECT facts -> 'guarantors' FROM lending.soft_approvals WHERE phone = :phone")
_OTHER_PERSONS = text("""
    SELECT borrower_first_name || ' ' || borrower_last_name FROM lending.loan_files
     WHERE NOT COALESCE(is_test, false) AND NOT (:kind = 'loan_file' AND id = :id)
    UNION
    SELECT first_name || ' ' || last_name FROM lending.lendingflow_leads
     WHERE NOT COALESCE(is_test, false) AND NOT (:kind = 'lf_lead' AND id = :id)
       AND first_name IS NOT NULL AND last_name IS NOT NULL
""")
_OTHER_ENTITIES = text("""
    SELECT DISTINCT e.name FROM lending.entities e JOIN lending.loan_files f ON f.entity_id = e.id
     WHERE NOT COALESCE(f.is_test, false) AND NOT (:kind = 'loan_file' AND f.id = :id)
""")
_SAVE = text("""
    UPDATE lending.lender_packages
       SET status = :status, checklist = CAST(:checklist AS jsonb), validator_verdict = CAST(:verdict AS jsonb),
           deal_facts_path = COALESCE(:pdf, deal_facts_path), updated_at = now()
     WHERE id = :id
""")


@dataclass(frozen=True)
class CompileResult:
    package_id: int
    source_type: str
    source_id: int
    lender_key: str
    status: str
    box_confirmed: bool
    checklist: list[dict]
    verdict: Verdict
    fit_result: dict
    deal_facts_path: Optional[str]

    @property
    def submittable(self) -> bool:
        return self.status == READY


def lead_ref(lead_id: int) -> str:
    return f"lendingflow_lead:{lead_id}"


def lender_name(lender_key: str) -> str:
    return next((r.name for r in LENDER_MATRIX if r.key == lender_key), lender_key)


def file_facts(db: Session, source_type: str, source_id: int) -> Optional[dict]:
    query = _LOAN_FILE_FACTS if source_type == LOAN_FILE else _LEAD_FACTS
    row = db.execute(query, {"id": source_id}).mappings().first()
    return dict(row) if row else None


def load_docs(db: Session, source_type: str, source_id: int) -> list[DocInput]:
    params = {"chase": DOC_CHASE_UPLOADS, "partner": PARTNER_FILE_UPLOADS}
    if source_type == LOAN_FILE:
        rows = db.execute(_LOAN_FILE_DOCS, {**params, "id": source_id}).mappings().all()
    else:
        rows = db.execute(_LEAD_DOCS, {**params, "ref": lead_ref(source_id)}).mappings().all()
    return [DocInput(upload_id=r["id"], upload_table=r["upload_table"], item_code=r["item_code"],
                     stored_path=r["stored_path"],
                     quarantined=(r["quarantine_reason"] or "no reason recorded")
                     if r["quarantine_status"] == quarantine.QUARANTINED else None,
                     released=r["quarantine_status"] == quarantine.RELEASED)
            for r in rows]


def _guarantor_names(db: Session, phone: Optional[str]) -> list[str]:
    if not phone:
        return []
    try:
        with db.begin_nested():
            raw = db.execute(_GUARANTORS, {"phone": phone}).scalar()
    except Exception as exc:  # class only: the soft-approval table may be absent
        logger.warning("[packaging] guarantor lookup failed (%s)", type(exc).__name__)
        return []
    if isinstance(raw, str):
        raw = json.loads(raw)
    names = []
    for item in raw or []:
        name = item.get("name") if isinstance(item, dict) else item
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names


def file_context(db: Session, source_type: str, source_id: int, facts: dict) -> FileContext:
    borrower = " ".join(p for p in (facts.get("first_name"), facts.get("last_name")) if p)
    persons = [borrower] if borrower else []
    entities: list[str] = []
    if source_type == LOAN_FILE:
        persons += _guarantor_names(db, facts.get("phone"))
        if facts.get("entity_name"):
            entities.append(facts["entity_name"])
    params = {"kind": source_type, "id": source_id}
    other_persons = [n for n in db.execute(_OTHER_PERSONS, params).scalars() if n]
    other_entities = [n for n in db.execute(_OTHER_ENTITIES, params).scalars() if n]
    return FileContext(persons=tuple(persons), entities=tuple(entities), other_persons=tuple(other_persons),
                       other_entities=tuple(other_entities))


def build_checklist(lender_key: str, verdict: Verdict) -> list[dict]:
    """The lender's required items; an item is received when a document of that type passed or was overridden."""
    received = {d.item_code for d in verdict.per_doc if d.counts_as_received}
    return [{"item_code": item.code, "label": item.label, "received": item.code in received}
            for item in LENDER_CHECKLISTS.get(lender_key, ())]


def status_for(verdict: Verdict, box_confirmed: bool) -> str:
    if verdict.outcome is not Outcome.PASS:
        return BLOCKED
    return READY if box_confirmed else DRAFT


def _money(value: Any) -> str:
    return f"${int(value):,}" if value not in (None, "") else "not given"


def deal_facts_context(package_id: int, lender_key: str, box_confirmed: bool, facts: dict, ctx: FileContext,
                       checklist: list[dict], today: date) -> dict:
    address = ", ".join(p for p in (facts.get("property_address"), facts.get("property_city"),
                                    " ".join(x for x in (facts.get("property_state"), facts.get("property_zip")) if x))
                        if p) or "Address not given"
    rows = [("Product", str(facts.get("product") or "not given").replace("_", " ")),
            ("Loan amount", _money(facts.get("loan_amount"))),
            ("Purchase price", _money(facts.get("purchase_price"))),
            ("Rehab budget", _money(facts.get("rehab_budget"))),
            ("After-repair value", _money(facts.get("arv"))),
            ("Credit band", facts.get("credit_band") or "not given"),
            ("Completed projects", facts.get("completed_projects") if facts.get("completed_projects") is not None
             else "not given")]
    if facts.get("entity_name"):
        rows.insert(0, ("Borrowing entity", facts["entity_name"]))
    return {"generated_date": today.strftime("%B %d, %Y"), "package_ref": f"PKG-{package_id}",
            "lender_name": lender_name(lender_key), "box_confirmed": box_confirmed, "address": address,
            "deal_rows": rows, "parties": list(ctx.persons), "checklist": checklist}


def render_deal_facts_pdf(template: str, context: dict) -> bytes:
    from src.lending.pdf.render import render_pdf

    return render_pdf(template, context, watermark=DEAL_FACTS_WATERMARK)


def _store_pdf(package_id: int, data: bytes) -> Optional[str]:
    base = get_settings().lending_doc_upload_dir
    if not base or not os.path.isabs(base):
        logger.warning("[packaging] LENDING_DOC_UPLOAD_DIR is not an absolute path: deal facts PDF not stored")
        return None
    folder = os.path.join(base, "packages")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"package-{package_id}.pdf")
    with open(path, "wb") as handle:
        handle.write(data)
    return path


def compile_package(db: Session, package_id: int, *, today: date,
                    renderer: Optional[PdfRenderer] = render_deal_facts_pdf) -> Optional[CompileResult]:
    """Validate, build the checklist and the PDF, save. None for a missing or closed package. The caller commits;
    newly found quarantines are committed and alerted here."""
    row = db.execute(_PACKAGE, {"id": package_id}).mappings().first()
    if row is None or row["status"] in CLOSED_STATUSES:
        return None
    source_type, source_id, lender_key = row["source_type"], row["source_id"], row["lender_key"]
    facts = file_facts(db, source_type, source_id)
    if facts is None:
        logger.warning("[packaging] package %s: its %s %s is gone", package_id, source_type, source_id)
        return None
    ctx = file_context(db, source_type, source_id, facts)
    settings = get_settings()
    limits = AgeLimits(settings.lending_bank_statement_max_age_days, settings.lending_pricing_sheet_max_age_days)
    docs = load_docs(db, source_type, source_id)
    verdict = validate(ctx, docs, today=today, limits=limits, overrides=frozenset(row["overrides"] or {}))

    already = {d.key for d in docs if d.quarantined is not None}
    for found in verdict.quarantined:
        if found.key not in already:
            quarantine.quarantine_and_alert(db, found.upload_table, found.upload_id, found.reasons)

    checklist = build_checklist(lender_key, verdict)
    status = status_for(verdict, row["box_confirmed"])
    pdf_path = None
    if renderer is not None:
        try:
            pdf_path = _store_pdf(package_id, renderer(DEAL_FACTS_TEMPLATE, deal_facts_context(
                package_id, lender_key, row["box_confirmed"], facts, ctx, checklist, today)))
        except Exception as exc:
            logger.error("[packaging] package %s: deal facts PDF failed (%s)", package_id, type(exc).__name__)
    db.execute(_SAVE, {"id": package_id, "status": status, "checklist": json.dumps(checklist),
                       "verdict": json.dumps(verdict.to_json()), "pdf": pdf_path})
    if status == BLOCKED and row["status"] != BLOCKED and source_type == LOAN_FILE:
        deal_ledger.record(db, source_id, "package_blocked", actor=deal_ledger.system("packet_validator"),
                           detail={"lender_key": lender_key, "blocked": [d.key for d in verdict.blocked],
                                   "quarantined": [d.key for d in verdict.quarantined]},
                           ref_table="lender_packages", ref_id=package_id)
    fit = row["fit_result"]
    return CompileResult(package_id, source_type, source_id, lender_key, status, row["box_confirmed"], checklist,
                         verdict, json.loads(fit) if isinstance(fit, str) else dict(fit or {}), pdf_path)
