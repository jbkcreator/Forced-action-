"""
Email Campaign admin API router — Phase B2/B3/B4/B5/B7.

All endpoints require admin JWT (Depends(get_current_admin)).
Prefix: /api/admin

Routes implemented here:

  Templates (B2)
    GET    /email-templates
    POST   /email-templates
    GET    /email-templates/variables
    GET    /email-templates/{id}
    PUT    /email-templates/{id}
    DELETE /email-templates/{id}

  Campaigns (B3/B4/B7)
    GET    /email-campaigns
    POST   /email-campaigns
    GET    /email-campaigns/summary
    GET    /email-campaigns/eligible-count
    GET    /email-campaigns/{id}
    POST   /email-campaigns/{id}/pause
    POST   /email-campaigns/{id}/resume
    POST   /email-campaigns/{id}/duplicate
    POST   /email-campaigns/{id}/add-contacts
    GET    /email-campaigns/{id}/contacts
    GET    /email-campaigns/{id}/contacts/export

  Inboxes (B5)
    GET    /email-inboxes
"""

import csv
import io
import logging
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.core.database import get_db_context
from src.core.models import (
    CampaignContact,
    CampaignDailyAnalytics,
    DBPRContact,
    EmailCampaign,
    EmailSequenceTemplate,
)
from src.services import email_campaigns as campaign_svc
from src.services import email_templates as template_svc
from src.services import instantly_service as instantly

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["email-campaigns"])


# ============================================================================
# Pydantic schemas
# ============================================================================

class TemplateStepIn(BaseModel):
    step_number: int
    delay_days: int = 0
    subject: str
    body: str


class TemplateCreateIn(BaseModel):
    name: str
    steps: list[TemplateStepIn]


class TemplateOut(BaseModel):
    id: int
    name: str
    steps: list[dict]
    variables_used: list[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SendScheduleIn(BaseModel):
    from_time: str = Field("09:00", alias="from")          # HH:MM
    to_time:   str = Field("17:00", alias="to")            # HH:MM
    days:      dict = Field(default_factory=dict)           # Instantly days object
    timezone:  str = "America/New_York"
    schedule_name: str = "Default"

    class Config:
        populate_by_name = True


class CampaignCreateIn(BaseModel):
    name: str
    template_id: int
    county_id: Optional[str] = None
    zips: list[str] = Field(default_factory=list)
    vertical: Optional[str] = None
    max_contacts: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    send_schedule: SendScheduleIn = Field(default_factory=SendScheduleIn)


class CampaignUpdateIn(BaseModel):
    """PATCH body — every field optional; only provided fields change."""
    name: Optional[str] = None
    template_id: Optional[int] = None
    geo_filter: Optional[dict] = None
    vertical: Optional[str] = None
    max_contacts: Optional[int] = Field(None, alias="max_contact_count")
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    send_schedule: Optional[dict] = Field(None, alias="campaign_schedule")
    # Instantly-only knobs → stored in instantly_settings + PATCHed to Instantly
    daily_limit: Optional[int] = None
    daily_max_leads: Optional[int] = None
    email_list: Optional[list[str]] = None
    stop_on_reply: Optional[bool] = None
    open_tracking: Optional[bool] = None
    link_tracking: Optional[bool] = None

    class Config:
        populate_by_name = True


class CampaignOut(BaseModel):
    id: int
    name: str
    instantly_campaign_id: Optional[str]
    template_id: Optional[int]
    county_id: Optional[str]
    geo_filter: dict
    vertical: Optional[str]
    max_contacts: Optional[int]
    start_date: Optional[date]
    end_date: Optional[date]
    status: str
    last_synced_at: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class CampaignDetailOut(BaseModel):
    """Detail view: config + live contact count + latest analytics snapshot."""
    # config
    id: int
    name: str
    instantly_campaign_id: Optional[str]
    template_id: Optional[int]
    county_id: Optional[str]
    geo_filter: dict
    vertical: Optional[str]
    max_contacts: Optional[int]
    start_date: Optional[date]
    end_date: Optional[date]
    send_schedule: dict
    instantly_settings: dict
    status: str
    last_synced_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    # live count
    contact_count: int
    # latest snapshot (analytics cards); snapshot_date None when no snapshot yet
    snapshot_date: Optional[date]
    emails_sent: int
    opens: int
    open_rate: float
    replies: int
    reply_rate: float
    clicks: int
    bounces: int
    unsubscribes: int
    interested: int


class CampaignUpdateOut(BaseModel):
    """PATCH response — updated campaign detail + downstream-effect warnings."""
    campaign: CampaignDetailOut
    warnings: list[str]


class CampaignListItem(BaseModel):
    id: int
    name: str
    status: str
    contact_count: int
    open_rate: float
    reply_rate: float
    last_synced_at: Optional[datetime]


class ContactListItem(BaseModel):
    campaign_contact_id: int
    dbpr_contact_id: int
    full_name: Optional[str]
    company_name: Optional[str]
    email: Optional[str]
    engagement_status: str
    is_signed_up: bool
    last_activity_at: Optional[datetime]
    converted_at: Optional[datetime]


# ============================================================================
# B2 — Templates
# ============================================================================

@router.get("/email-templates/variables")
def list_allowed_variables(_: str = Depends(get_current_admin)):
    """Return the whitelisted personalization variables."""
    return {
        "variables": [
            {"name": k, "description": v}
            for k, v in template_svc.ALLOWED_VARIABLES.items()
        ]
    }


@router.get("/email-templates", response_model=list[TemplateOut])
def list_templates(_: str = Depends(get_current_admin)):
    with get_db_context() as db:
        rows = db.query(EmailSequenceTemplate).order_by(EmailSequenceTemplate.name).all()
        return [TemplateOut.model_validate(r) for r in rows]


@router.post("/email-templates", response_model=TemplateOut, status_code=201)
def create_template(body: TemplateCreateIn, _: str = Depends(get_current_admin)):
    steps = [s.model_dump() for s in body.steps]
    unknown = template_svc.validate_variables(steps)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown template variables: {unknown}. "
                   f"Allowed: {list(template_svc.ALLOWED_VARIABLES)}",
        )
    variables_used = template_svc.collect_variables_used(steps)
    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        existing = db.query(EmailSequenceTemplate).filter_by(name=body.name).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"Template '{body.name}' already exists")
        tmpl = EmailSequenceTemplate(
            name=body.name,
            steps=steps,
            variables_used=variables_used,
            created_at=now,
            updated_at=now,
        )
        db.add(tmpl)
        db.flush()
        db.refresh(tmpl)
        return TemplateOut.model_validate(tmpl)


@router.get("/email-templates/{template_id}", response_model=TemplateOut)
def get_template(template_id: int, _: str = Depends(get_current_admin)):
    with get_db_context() as db:
        tmpl = db.get(EmailSequenceTemplate, template_id)
        if not tmpl:
            raise HTTPException(status_code=404, detail="Template not found")
        return TemplateOut.model_validate(tmpl)


@router.put("/email-templates/{template_id}", response_model=TemplateOut)
def update_template(
    template_id: int,
    body: TemplateCreateIn,
    _: str = Depends(get_current_admin),
):
    steps = [s.model_dump() for s in body.steps]
    unknown = template_svc.validate_variables(steps)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown template variables: {unknown}",
        )
    variables_used = template_svc.collect_variables_used(steps)
    with get_db_context() as db:
        tmpl = db.get(EmailSequenceTemplate, template_id)
        if not tmpl:
            raise HTTPException(status_code=404, detail="Template not found")
        dup = db.query(EmailSequenceTemplate).filter(
            EmailSequenceTemplate.name == body.name,
            EmailSequenceTemplate.id != template_id,
        ).first()
        if dup:
            raise HTTPException(status_code=409, detail=f"Template name '{body.name}' already taken")
        tmpl.name = body.name
        tmpl.steps = steps
        tmpl.variables_used = variables_used
        tmpl.updated_at = datetime.now(timezone.utc)
        db.add(tmpl)
        db.refresh(tmpl)
        return TemplateOut.model_validate(tmpl)


@router.delete("/email-templates/{template_id}", status_code=204)
def delete_template(template_id: int, _: str = Depends(get_current_admin)):
    with get_db_context() as db:
        tmpl = db.get(EmailSequenceTemplate, template_id)
        if not tmpl:
            raise HTTPException(status_code=404, detail="Template not found")
        referenced = db.query(EmailCampaign).filter_by(template_id=template_id).first()
        if referenced:
            raise HTTPException(
                status_code=409,
                detail="Template is referenced by one or more campaigns and cannot be deleted",
            )
        db.delete(tmpl)


# ============================================================================
# B3/B7 — Campaigns
# ============================================================================

@router.get("/email-campaigns/summary")
def campaign_summary(_: str = Depends(get_current_admin)):
    """Dashboard widget: active count, total contractors, 30-day open/reply."""
    return campaign_svc.get_summary()


@router.get("/email-campaigns/eligible-count")
def eligible_count(
    county_id: Optional[str] = Query(None),
    zips: list[str] = Query(default=[]),
    vertical: Optional[str] = Query(None),
    campaign_id: Optional[int] = Query(None),
    _: str = Depends(get_current_admin),
):
    return {"count": campaign_svc.count_eligible(
        county_id=county_id,
        zips=zips,
        vertical=vertical,
        exclude_campaign_id=campaign_id,
    )}


@router.get("/email-campaigns", response_model=list[CampaignListItem])
def list_campaigns(
    status: Optional[str] = Query(None),
    _: str = Depends(get_current_admin),
):
    return campaign_svc.list_campaigns(status=status)


@router.post("/email-campaigns", response_model=CampaignOut, status_code=201)
def create_campaign(body: CampaignCreateIn, _: str = Depends(get_current_admin)):
    return campaign_svc.create_campaign(body)


@router.get("/email-campaigns/{campaign_id}", response_model=CampaignDetailOut)
def get_campaign(campaign_id: int, _: str = Depends(get_current_admin)):
    detail = campaign_svc.get_campaign_detail(campaign_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return detail


@router.patch("/email-campaigns/{campaign_id}", response_model=CampaignUpdateOut)
def update_campaign(
    campaign_id: int,
    body: CampaignUpdateIn,
    _: str = Depends(get_current_admin),
):
    """
    Edit a campaign (draft/active/paused; 409 on completed). PATCH semantics —
    only provided fields change. Local + Instantly stay in sync; the response
    `warnings` array describes downstream effects (re-pushed sequences, future-
    only top-up changes, Instantly sync failures).
    """
    patch = body.model_dump(exclude_unset=True)
    return campaign_svc.update_campaign(campaign_id, patch)


@router.post("/email-campaigns/{campaign_id}/pause", status_code=200)
def pause_campaign(campaign_id: int, _: str = Depends(get_current_admin)):
    campaign_svc.pause_campaign(campaign_id)
    return {"status": "paused"}


@router.post("/email-campaigns/{campaign_id}/resume", status_code=200)
def resume_campaign(campaign_id: int, _: str = Depends(get_current_admin)):
    campaign_svc.resume_campaign(campaign_id)
    return {"status": "active"}


@router.post("/email-campaigns/{campaign_id}/duplicate", response_model=CampaignOut, status_code=201)
def duplicate_campaign(campaign_id: int, _: str = Depends(get_current_admin)):
    return campaign_svc.duplicate_campaign(campaign_id)


@router.post("/email-campaigns/{campaign_id}/add-contacts", status_code=200)
def add_contacts(campaign_id: int, _: str = Depends(get_current_admin)):
    added = campaign_svc.topup_campaign(campaign_id)
    return {"added": added}


# ============================================================================
# B7 — Campaign contacts list + export
# ============================================================================

@router.get("/email-campaigns/{campaign_id}/contacts", response_model=list[ContactListItem])
def list_campaign_contacts(
    campaign_id: int,
    engagement_status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _: str = Depends(get_current_admin),
):
    return campaign_svc.list_contacts(
        campaign_id=campaign_id,
        engagement_status=engagement_status,
        search=search,
        page=page,
        page_size=page_size,
    )


@router.get("/email-campaigns/{campaign_id}/contacts/export")
def export_campaign_contacts(
    campaign_id: int,
    engagement_status: Optional[str] = Query(None),
    _: str = Depends(get_current_admin),
):
    rows = campaign_svc.list_contacts(
        campaign_id=campaign_id,
        engagement_status=engagement_status,
        search=None,
        page=1,
        page_size=10_000,
    )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "campaign_contact_id", "dbpr_contact_id", "full_name", "company_name",
        "email", "engagement_status", "is_signed_up", "last_activity_at", "converted_at",
    ])
    writer.writeheader()
    for r in rows:
        writer.writerow(r.model_dump())
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=campaign_{campaign_id}_contacts.csv"},
    )


# ============================================================================
# B5 — Inboxes / Warmup (read-only)
# ============================================================================

@router.get("/email-inboxes")
def list_inboxes(_: str = Depends(get_current_admin)):
    accounts = instantly.list_accounts()
    emails = [a.get("email") for a in accounts if a.get("email")]
    warmup_data: dict[str, dict] = {}
    if emails:
        for item in instantly.get_warmup_analytics(emails):
            warmup_data[item.get("email", "")] = item

    result = []
    for account in accounts:
        email = account.get("email", "")
        warmup = warmup_data.get(email, {})
        health_score = warmup.get("health_score") or warmup.get("warmup_score") or 0
        result.append({
            "email":          email,
            "warmup_enabled": account.get("warmup_enabled", False),
            "health_score":   health_score,
            "health_warning": health_score < 70,
            "instantly_url":  f"https://app.instantly.ai/app/accounts",
        })
    return result
