"""nextdeallending.com lead form endpoint (WP-GL-11).

POST /api/lending/web-leads (multipart form, as the static page submits it). The lead and its
consent evidence are committed first; delivery to GoHighLevel runs after the response, with
the retry sweep (src.tasks.lending_web_lead_sweep) as the safety net. Public and unauthenticated
by design, so it is rate limited per IP and has a honeypot field.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from sqlalchemy.orm import Session

from config.lending_web import RATE_LIMIT_PER_WINDOW, RATE_LIMIT_SCOPE, RATE_LIMIT_WINDOW_SECONDS
from src.api.deps import get_db
from src.lending.db import lending_session
from src.lending.web_lead_ghl import get_live_sink
from src.lending.web_leads import InvalidWebLead, build_input, deliver_pending, save_web_lead
from src.services.rate_limit import client_ip, enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending", tags=["lending"])


def deliver_in_background(lead_id: int) -> None:
    """Runs after the response. Never raises: the saved row and the sweep cover any failure."""
    try:
        with lending_session() as db:
            deliver_pending(db, get_live_sink(), lead_id=lead_id)
    except Exception as exc:
        logger.error("[lending-web] background delivery crashed lead=%s: %s", lead_id, type(exc).__name__)


@router.post("/web-leads")
def create_web_lead(
    request: Request,
    background_tasks: BackgroundTasks,
    name: str = Form(default=""),
    phone: str = Form(default=""),
    email: Optional[str] = Form(default=None),
    property_city: Optional[str] = Form(default=None),
    deal_type: Optional[str] = Form(default=None),
    completed_projects_3y: Optional[str] = Form(default=None),
    sms_consent: Optional[str] = Form(default=None),
    deal_drop_optin: Optional[str] = Form(default=None),
    consent_text: Optional[str] = Form(default=None),
    page_url: Optional[str] = Form(default=None),
    company_website: Optional[str] = Form(default=None),  # honeypot: hidden on the page, bots fill it
    db: Session = Depends(get_db),
) -> dict[str, bool]:
    enforce_or_429(request, RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    if company_website:
        logger.info("[lending-web] honeypot tripped: submission dropped")
        return {"received": True}
    try:
        data = build_input(
            {
                "name": name, "phone": phone, "email": email, "property_city": property_city,
                "deal_type": deal_type, "completed_projects_3y": completed_projects_3y,
                "sms_consent": sms_consent, "deal_drop_optin": deal_drop_optin,
                "consent_text": consent_text, "page_url": page_url,
            },
            ip_address=client_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except InvalidWebLead as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        lead_id, created = save_web_lead(db, data)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-web] could not save lead: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="We could not save your request. Please call us.") from exc
    if created:
        background_tasks.add_task(deliver_in_background, lead_id)
    return {"received": True}
