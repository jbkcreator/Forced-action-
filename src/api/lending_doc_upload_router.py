"""P2.4 borrower document upload, served by ``lending-api`` under the existing ``/api/lending/`` prefix.

GET  /api/lending/docs/{token}          the chase's checklist with one upload per document (mobile first)
POST /api/lending/docs/{token}/upload   one file for one checklist item (multipart: item_code, document)
GET  /api/lending/docs/view/{token}     R1 0.2: an admin's 15-minute link to view or download one stored upload

R1 0.2 staff API (``Authorization: Bearer <staff token>``; admin tokens only, a caller token gets 403 everywhere):
POST /api/lending/docs/staff/borrowers/search              {"query": ...} -> matching borrowers, masked; never guesses
GET  /api/lending/docs/staff/borrowers/{lead_ref}/documents what each chase asked for, what arrived, upload ids
POST /api/lending/docs/staff/documents/{upload_id}/view-link -> {"url", "expires_at"}: a 15-minute view link

The token in the URL is the borrower's credential (``doc_chase_links`` keeps only its SHA-256; the full link is in
the drafted message until sent, shown masked on the approval card). An unknown, expired or revoked link,
or one whose chase is complete or cancelled, gets the plain "link no longer active" page. Uploads are rate limited
per IP and checked like the partner door's (count, size, type); files are stored under generated names.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from jinja2 import ChoiceLoader, Environment, FileSystemLoader
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from config.lending_partners import (
    MAX_UPLOAD_BYTES,
    RATE_LIMIT_PER_WINDOW,
    RATE_LIMIT_WINDOW_SECONDS,
)
from src.lending.closing.doc_access import (
    DocumentQuarantined,
    ViewRefused,
    document_list,
    find_document_borrowers,
    issue_view_link,
    resolve_view_token,
)
from src.lending.closing.doc_chase import (
    UploadStorageNotConfigured,
    chase_for_token,
    chase_items,
    record_upload,
    upload_dir,
)
from src.lending.audit import deal_ledger
from src.lending.db import get_lending_db
from src.lending.packaging.actions import ActionRefused, override_block, release_document, tag_document_type
from src.lending.packaging.packet_validator import DOC_CHASE_UPLOADS, UPLOAD_TABLES
from src.lending.packaging.quarantine import scan_new_uploads
from src.lending.partner_service import PartnerLinkNotConfigured
from src.lending.partners import InvalidSubmission, Upload
from src.lending.security.rbac import (
    TOKEN_ISSUER,
    CallerDbRoleUnavailable,
    DocumentAccessDenied,
    Role,
    assume_caller_db_role,
)
from src.lending.security.staff_tokens import StaffToken, token_for
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/docs", tags=["lending-docs"])

_LENDING_DIR = Path(__file__).resolve().parents[1] / "lending"
RATE_LIMIT_SCOPE = "lending_doc_upload"
# The borrower's upload token and an admin's view token are both credentials; the staff paths carry none.
_TOKEN_IN_PATH = re.compile(r"(/api/lending/docs/(?:view/)?)(?!staff/)[^/\s?\"]+")
VIEW_RATE_LIMIT_SCOPE = "lending_doc_view"
STAFF_RATE_LIMIT_SCOPE = "lending_staff_api"
_LEAD_REF = re.compile(r"^(web_lead|lendingflow_lead|contact):(\d+)$")
_BEARER = "bearer "
TOO_MANY_DETAIL = "More than 10 borrowers match. Add a last name or the last 4 digits of the phone."
_VIEW_REFUSED_MESSAGES = {
    410: "This document link has expired. Ask Cora for a new one.",
    403: "This document link is not valid.",
    404: "This document is no longer available.",
}


def redact_upload_tokens(value: str) -> str:
    """``/api/lending/docs/<token>/upload`` -> ``/api/lending/docs/[…]/upload``: the token is the borrower's key."""
    return _TOKEN_IN_PATH.sub(r"\1[…]", value)


class UploadTokenLogFilter(logging.Filter):
    """Keeps upload tokens out of the access log (uvicorn / gunicorn log the request path)."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(redact_upload_tokens(a) if isinstance(a, str) else a for a in record.args)
        elif isinstance(record.args, dict):
            record.args = {k: redact_upload_tokens(v) if isinstance(v, str) else v for k, v in record.args.items()}
        if isinstance(record.msg, str):
            record.msg = redact_upload_tokens(record.msg)
        return True


@lru_cache(maxsize=1)
def _pages() -> Environment:
    loader = ChoiceLoader([FileSystemLoader(str(_LENDING_DIR / "doc_pages")),
                           FileSystemLoader(str(_LENDING_DIR / "partner_pages"))])
    return Environment(loader=loader, autoescape=True)


def _page(name: str, status_code: int = 200, **context: Any) -> HTMLResponse:
    return HTMLResponse(_pages().get_template(name).render(**context), status_code=status_code)


def _inactive() -> HTMLResponse:
    return _page("inactive.html", status_code=404)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _checklist(db: Session, token: str, chase: dict[str, Any], *, errors: Optional[list[str]] = None,
               status_code: int = 200) -> HTMLResponse:
    items = chase_items(db, chase["id"])
    return _page("upload.html", status_code=status_code, token=token, first_name=chase["first_name"], items=items,
                 received=sum(1 for item in items if item["received_at"] is not None), errors=errors or [],
                 max_upload_mb=MAX_UPLOAD_BYTES // (1024 * 1024))


@router.get("/view/{token}")
def view_document(token: str, request: Request, db: Session = Depends(get_lending_db)) -> Response:
    enforce_or_429(request, VIEW_RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    try:
        found = resolve_view_token(db, token, _now())
    except ViewRefused as refused:
        return _page("inactive.html", status_code=refused.status, message=_VIEW_REFUSED_MESSAGES[refused.status])
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[doc-view] view link failed (%s)", type(exc).__name__)
        return _page("inactive.html", status_code=404, message=_VIEW_REFUSED_MESSAGES[404])
    return FileResponse(found.path, media_type=found.content_type, content_disposition_type="inline",
                        filename=found.download_name,
                        headers={"Cache-Control": "no-store, private", "X-Content-Type-Options": "nosniff",
                                 "Referrer-Policy": "no-referrer"})


@router.get("/{token}", response_class=HTMLResponse)
def document_page(token: str, db: Session = Depends(get_lending_db)) -> HTMLResponse:
    chase = chase_for_token(db, token, _now())
    return _checklist(db, token, chase) if chase else _inactive()


@router.post("/{token}/upload", response_class=HTMLResponse)
def upload_document(token: str, request: Request, item_code: str = Form(default=""),
                    document: UploadFile = File(...), db: Session = Depends(get_lending_db)) -> HTMLResponse:
    enforce_or_429(request, RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    now = _now()
    chase = chase_for_token(db, token, now)
    if chase is None:
        return _inactive()
    data = document.file.read(MAX_UPLOAD_BYTES + 1)  # one byte past the limit detects an oversize file
    upload = Upload(original_name=document.filename or "", content_type=document.content_type or "", data=data)
    stored: Optional[str] = None
    try:
        stored = record_upload(db, chase, item_code, upload, upload_dir=upload_dir(), now=now)
        db.commit()
    except InvalidSubmission as exc:  # refused before anything is written
        return _checklist(db, token, chase, errors=[str(exc)], status_code=400)
    except UploadStorageNotConfigured:
        logger.error("[doc-upload] chase %s: LENDING_DOC_UPLOAD_DIR is not an absolute path; upload refused", chase["id"])
        return _checklist(db, token, chase, errors=["The upload didn't go through. Please try again."], status_code=500)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        db.rollback()
        if stored is not None:  # written, but its row was rolled back: no file without a record
            try:
                os.remove(stored)
            except OSError:
                logger.error("[doc-upload] chase %s: orphaned upload not removed", chase["id"])
        logger.error("[doc-upload] chase %s upload failed (%s)", chase["id"], type(exc).__name__)
        return _checklist(db, token, chase, errors=["The upload didn't go through. Please try again."], status_code=500)

    if stored:
        scan_new_uploads(db, DOC_CHASE_UPLOADS, stored_path=stored)
    return RedirectResponse(f"/api/lending/docs/{token}", status_code=303)


# ---- R1 0.2 staff API ------------------------------------------------------------------------------------------------

class BorrowerSearchIn(BaseModel):
    query: str = Field(..., min_length=2, max_length=120)


def staff_admin(request: Request, authorization: Optional[str] = Header(default=None),
                db: Session = Depends(get_lending_db)) -> StaffToken:
    """401 without a valid token; 403 for a caller token. A caller's session is switched to the lending_caller DB
    role before the role check, so a route that forgot this dependency's check still could not read documents."""
    enforce_or_429(request, STAFF_RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    raw = authorization[len(_BEARER):].strip() if (authorization or "").lower().startswith(_BEARER) else ""
    token = token_for(db, raw, _now()) if raw else None
    if token is None:
        raise HTTPException(status_code=401, detail="invalid or expired token", headers={"WWW-Authenticate": "Bearer"})
    if token.role is not Role.ADMIN:
        try:
            assume_caller_db_role(db)
        except CallerDbRoleUnavailable:
            pass  # refused below either way
        logger.warning("[staff-api] caller token %s refused %s", token.id, request.url.path)
        raise HTTPException(status_code=403, detail="not permitted")
    return token


def _internal_error(db: Session, what: str, exc: Exception) -> HTTPException:
    db.rollback()
    logger.error("[staff-api] %s failed (%s)", what, type(exc).__name__)
    return HTTPException(status_code=500, detail="internal error")


@router.post("/staff/borrowers/search")
def staff_search(body: BorrowerSearchIn, token: StaffToken = Depends(staff_admin),
                 db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    try:
        found = find_document_borrowers(db, body.query)
    except Exception as exc:
        raise _internal_error(db, "search", exc) from exc
    result: dict[str, Any] = {"matches": [{"lead_ref": m.lead_ref, "borrower": m.display,
                                           "chase_started": m.chase_started.isoformat(),
                                           "uploaded_items": list(m.uploaded_items)} for m in found.matches],
                              "too_many": found.too_many}
    if found.too_many:
        result["detail"] = TOO_MANY_DETAIL
    return result


@router.get("/staff/borrowers/{lead_ref}/documents")
def staff_documents(lead_ref: str, token: StaffToken = Depends(staff_admin),
                    db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    if not _LEAD_REF.match(lead_ref):
        raise HTTPException(status_code=400, detail="lead_ref must look like web_lead:12")
    try:
        listed = document_list(db, lead_ref)
    except Exception as exc:
        raise _internal_error(db, "document list", exc) from exc
    if listed is None:
        raise HTTPException(status_code=404, detail="no documents for this borrower")
    return listed


@router.post("/staff/documents/{upload_id}/view-link", status_code=201)
def staff_view_link(upload_id: int, token: StaffToken = Depends(staff_admin),
                    db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    try:
        link = issue_view_link(db, upload_id, f"{TOKEN_ISSUER}{token.id}", _now())
        db.commit()
    except LookupError:
        db.rollback()
        raise HTTPException(status_code=404, detail="no such document")
    except DocumentAccessDenied:
        db.rollback()
        raise HTTPException(status_code=403, detail="not permitted")
    except DocumentQuarantined:
        db.rollback()
        raise HTTPException(status_code=403, detail="this document is quarantined; an admin must release it first")
    except PartnerLinkNotConfigured:
        db.rollback()
        raise HTTPException(status_code=503, detail="document links are not configured")
    except Exception as exc:
        raise _internal_error(db, "view link", exc) from exc
    return {"url": link.url, "expires_at": link.expires_at.isoformat()}


# ---- staff package and document actions ----------------------------------------------------------------------------

class DocTypeIn(BaseModel):
    item_code: str = Field(..., min_length=1, max_length=40)


class OverrideIn(BaseModel):
    upload_table: str = Field(..., pattern="^(doc_chase_uploads|partner_file_uploads)$")
    upload_id: int
    reason: str = Field(..., min_length=3, max_length=300)


class ReleaseIn(BaseModel):
    reason: str = Field(..., min_length=3, max_length=300)


def _staff_actor(token: StaffToken) -> str:
    return deal_ledger.human(f"staff_token:{token.id}")


def _apply(db: Session, what: str, fn: Any) -> Any:
    try:
        result = fn()
        db.commit()
        return result
    except ActionRefused as refused:
        db.rollback()
        raise HTTPException(status_code=409, detail=str(refused))
    except Exception as exc:
        raise _internal_error(db, what, exc) from exc


@router.post("/staff/documents/{upload_id}/type")
def staff_set_doc_type(upload_id: int, body: DocTypeIn, token: StaffToken = Depends(staff_admin),
                       db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    """Set the type of a partner-door upload (the borrower upload page already knows its type)."""
    _apply(db, "set document type",
           lambda: tag_document_type(db, upload_id, body.item_code, actor=_staff_actor(token)))
    return {"upload_id": upload_id, "item_code": body.item_code}


@router.post("/staff/packages/{package_id}/override")
def staff_override(package_id: int, body: OverrideIn, token: StaffToken = Depends(staff_admin),
                   db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    result = _apply(db, "override", lambda: override_block(
        db, package_id, body.upload_table, body.upload_id, reason=body.reason, actor=_staff_actor(token),
        today=_now().date()))
    return {"package_id": package_id, "status": result.status}


@router.post("/staff/documents/{upload_table}/{upload_id}/release")
def staff_release(upload_table: str, upload_id: int, body: ReleaseIn, token: StaffToken = Depends(staff_admin),
                  db: Session = Depends(get_lending_db)) -> dict[str, Any]:
    if upload_table not in UPLOAD_TABLES:
        raise HTTPException(status_code=404, detail="no such document table")
    _apply(db, "release", lambda: release_document(db, upload_table, upload_id, reason=body.reason,
                                                    actor=_staff_actor(token)))
    return {"upload_table": upload_table, "upload_id": upload_id, "quarantine_status": "released"}
