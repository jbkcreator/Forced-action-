"""Partner Door (P-1) pages, served by ``lending-api`` under the existing ``/api/lending/`` prefix.

GET  /api/lending/partners/{token}          the submission form (mobile first)
POST /api/lending/partners/{token}/files    submit a file (multipart, optional uploads)
GET  /api/lending/partners/{token}/status   the partner's own files, read only

The token in the URL is the partner's credential; no login. An unknown or revoked token gets a
plain "link no longer active" page. The form will not open until Josh's consent wording is
configured, and a submission without the consent checkbox is refused. Public, so the submit is
rate limited per IP and carries a honeypot field.
"""
from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from pydantic import ValidationError
from sqlalchemy.orm import Session

from config.lending_partners import (
    CREDIT_BANDS,
    MAX_UPLOAD_BYTES,
    MAX_UPLOADS,
    PRODUCT_LABELS,
    RATE_LIMIT_PER_WINDOW,
    RATE_LIMIT_SCOPE,
    RATE_LIMIT_WINDOW_SECONDS,
    STATUS_LABELS,
    TIMELINES,
)
from config.settings import get_settings
from src.lending.db import get_lending_db
from src.lending.partner_pipeline import process_new_file
from src.lending.partners import (
    ConsentEvidence,
    InvalidSubmission,
    Partner,
    SubmissionInput,
    Upload,
    partner_files,
    partner_for_token,
    save_submission,
)
from src.services.rate_limit import client_ip, enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/partners", tags=["lending-partners"])

_PAGES_DIR = Path(__file__).resolve().parents[1] / "lending" / "partner_pages"
_FIELD_LABELS = {
    "borrower_first_name": "Borrower first name", "borrower_last_name": "Borrower last name",
    "borrower_phone": "Borrower phone", "borrower_email": "Borrower email", "property_address": "Property address",
    "property_city": "City", "property_state": "State", "property_zip": "ZIP", "product": "Loan type",
    "purchase_price": "Purchase price", "rehab_budget": "Rehab or build budget", "loan_amount": "Loan amount",
    "arv": "ARV or completed value", "completed_projects": "Completed projects", "credit_band": "Credit range",
    "timeline": "Timeline", "notes": "Notes",
}


@lru_cache(maxsize=1)
def _pages() -> Environment:
    return Environment(loader=FileSystemLoader(str(_PAGES_DIR)), autoescape=True)


def _page(name: str, status_code: int = 200, **context: Any) -> HTMLResponse:
    return HTMLResponse(_pages().get_template(name).render(**context), status_code=status_code)


def _inactive() -> HTMLResponse:
    return _page("inactive.html", status_code=404)


def _form(token: str, partner: Partner, consent_text: str, *, values: Optional[dict[str, Any]] = None,
          errors: Optional[list[str]] = None, status_code: int = 200) -> HTMLResponse:
    return _page("form.html", status_code=status_code, token=token, partner=partner, consent_text=consent_text,
                 values=values or {}, errors=errors or [], products=PRODUCT_LABELS, credit_bands=list(CREDIT_BANDS),
                 timelines=TIMELINES, max_uploads=MAX_UPLOADS, max_upload_mb=MAX_UPLOAD_BYTES // (1024 * 1024))


def _not_configured() -> HTMLResponse:
    return _page("inactive.html", status_code=503,
                 message="This form isn't available yet. Please contact Next Deal Lending directly.")


def run_pipeline_in_background(file_id: int) -> None:
    """After the response: score, pre-qual, route, notify. Never raises; the file is already saved."""
    process_new_file(file_id)


def _read_upload(upload: UploadFile) -> Upload:
    # Read one byte past the limit so an oversize file is detected without loading all of it.
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    return Upload(original_name=upload.filename or "", content_type=upload.content_type or "", data=data)


@router.get("/{token}", response_class=HTMLResponse)
def partner_form(token: str, db: Session = Depends(get_lending_db)) -> HTMLResponse:
    partner = partner_for_token(db, token)
    if partner is None:
        return _inactive()
    consent_text = get_settings().lending_partner_consent_text.strip()
    if not consent_text:
        return _not_configured()
    return _form(token, partner, consent_text)


@router.post("/{token}/files", response_class=HTMLResponse)
def submit_file(
    token: str,
    request: Request,
    background_tasks: BackgroundTasks,
    borrower_first_name: str = Form(default=""),
    borrower_last_name: str = Form(default=""),
    borrower_phone: str = Form(default=""),
    borrower_email: str = Form(default=""),
    property_address: str = Form(default=""),
    property_city: str = Form(default=""),
    property_state: str = Form(default=""),
    property_zip: str = Form(default=""),
    product: str = Form(default=""),
    purchase_price: str = Form(default=""),
    rehab_budget: str = Form(default=""),
    loan_amount: str = Form(default=""),
    arv: str = Form(default=""),
    completed_projects: str = Form(default=""),
    credit_band: str = Form(default=""),
    timeline: str = Form(default=""),
    notes: str = Form(default=""),
    consent: str = Form(default=""),
    company_website: str = Form(default=""),  # honeypot: hidden on the page, bots fill it
    documents: list[UploadFile] = File(default=[]),
    db: Session = Depends(get_lending_db),
) -> HTMLResponse:
    enforce_or_429(request, RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    partner = partner_for_token(db, token)
    if partner is None:
        return _inactive()
    consent_text = get_settings().lending_partner_consent_text.strip()
    if not consent_text:
        return _not_configured()
    if company_website:
        logger.info("[partner-door] honeypot tripped: submission dropped")
        return _page("received.html", token=token, file_id=None)

    values = {
        "borrower_first_name": borrower_first_name, "borrower_last_name": borrower_last_name,
        "borrower_phone": borrower_phone, "borrower_email": borrower_email, "property_address": property_address,
        "property_city": property_city, "property_state": property_state, "property_zip": property_zip,
        "product": product, "purchase_price": purchase_price, "rehab_budget": rehab_budget,
        "loan_amount": loan_amount, "arv": arv, "completed_projects": completed_projects,
        "credit_band": credit_band, "timeline": timeline, "notes": notes,
    }
    if consent.lower() not in {"yes", "on", "true"}:
        return _form(token, partner, consent_text, values=values, status_code=400,
                     errors=["Please tick the consent box before submitting."])
    try:
        data = SubmissionInput.model_validate(values)
    except ValidationError as error:
        problems = [f"{_FIELD_LABELS.get(str(issue['loc'][0]), 'Form')}: {issue['msg'].removeprefix('Value error, ')}"
                    for issue in error.errors()]
        return _form(token, partner, consent_text, values=values, errors=problems, status_code=400)

    evidence = ConsentEvidence(
        consent_text=consent_text, ip_address=client_ip(request),
        page_url=str(request.url).replace(token, "<link>"), user_agent=request.headers.get("user-agent"),
    )
    try:
        file_id = save_submission(db, partner, data, [_read_upload(upload) for upload in documents],
                                  consent=evidence, upload_dir=get_settings().lending_partner_upload_dir)
        db.commit()
    except InvalidSubmission as error:
        db.rollback()
        return _form(token, partner, consent_text, values=values, errors=[str(error)], status_code=400)
    except Exception as error:
        db.rollback()
        logger.error("[partner-door] could not save submission partner=%s (%s)", partner.id, type(error).__name__)
        return _form(token, partner, consent_text, values=values, status_code=500,
                     errors=["We could not save this file. Please try again, or contact Next Deal Lending."])
    background_tasks.add_task(run_pipeline_in_background, file_id)
    return _page("received.html", token=token, file_id=file_id)


@router.get("/{token}/status", response_class=HTMLResponse)
def partner_status(token: str, db: Session = Depends(get_lending_db)) -> HTMLResponse:
    partner = partner_for_token(db, token)
    if partner is None:
        return _inactive()
    return _page("dashboard.html", token=token, partner=partner, files=partner_files(db, partner.id),
                 products=PRODUCT_LABELS, status_labels=STATUS_LABELS)
