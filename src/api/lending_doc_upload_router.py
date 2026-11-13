"""P2.4 borrower document upload, served by ``lending-api`` under the existing ``/api/lending/`` prefix.

GET  /api/lending/docs/{token}          the chase's checklist with one upload per document (mobile first)
POST /api/lending/docs/{token}/upload   one file for one checklist item (multipart: item_code, document)

The token in the URL is the borrower's credential (only its SHA-256 is stored). An unknown, expired or revoked link,
or one whose chase is complete or cancelled, gets the plain "link no longer active" page. Uploads are rate limited
per IP and checked like the partner door's (count, size, type); files are stored under generated names.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import ChoiceLoader, Environment, FileSystemLoader
from sqlalchemy.orm import Session

from config.lending_partners import MAX_UPLOAD_BYTES, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS
from config.settings import get_settings
from src.lending.closing.doc_chase import chase_for_token, chase_items, record_upload
from src.lending.db import get_lending_db
from src.lending.partners import InvalidSubmission, Upload
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/docs", tags=["lending-docs"])

_LENDING_DIR = Path(__file__).resolve().parents[1] / "lending"
RATE_LIMIT_SCOPE = "lending_doc_upload"


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
    try:
        record_upload(db, chase, item_code, upload, upload_dir=get_settings().lending_doc_upload_dir, now=now)
        db.commit()
    except InvalidSubmission as exc:  # refused before anything is written
        return _checklist(db, token, chase, errors=[str(exc)], status_code=400)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        db.rollback()
        logger.error("[doc-upload] chase %s upload failed (%s)", chase["id"], type(exc).__name__)
        return _checklist(db, token, chase, errors=["The upload didn't go through. Please try again."], status_code=500)
    return RedirectResponse(f"/api/lending/docs/{token}", status_code=303)
