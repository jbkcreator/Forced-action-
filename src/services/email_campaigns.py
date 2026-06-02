"""
Email Campaign service — core business logic (B3/B4/B7).

Covers:
  - Eligibility query (reused by live-count, top-up, add-contacts)
  - Campaign create / pause / resume / duplicate (Instantly orchestration)
  - Top-up dispatch (push eligible contacts into Instantly in batches)
  - Read helpers for list/summary/contacts endpoints
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import and_, func, or_, text

from src.core.database import get_db_context
from src.core.models import (
    CampaignContact,
    CampaignDailyAnalytics,
    DBPRContact,
    EmailCampaign,
    EmailSequenceTemplate,
)
from src.services import email_templates as template_svc
from src.services import instantly_service as instantly
from src.services.email import send_alert
from src.utils.county_config import list_counties

logger = logging.getLogger(__name__)

_BATCH_SIZE = 1000  # Instantly max leads per request


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

def _eligibility_filters(
    county_id: Optional[str],
    zips: list[str],
    vertical: Optional[str],
    exclude_campaign_id: Optional[int] = None,
):
    """
    Build the SQLAlchemy filter list for eligible DBPR contacts.

    Core predicate:
      enrichment_status='enriched' AND email IS NOT NULL AND email_verified=TRUE
      AND NOT is_opted_out AND NOT is_hard_bounced AND NOT is_signed_up
      AND (license_expiry IS NULL OR license_expiry >= today)
      AND vertical matches (if set)
      AND geo filter (county OR zips)
      AND NOT already a member of the given campaign (if exclude_campaign_id set)
      AND county_id IN launched counties
    """
    today = date.today()

    # Launched county IDs
    try:
        launched = [c["county_id"] for c in list_counties() if c.get("status") == "launched"]
    except Exception:
        launched = ["hillsborough"]

    filters = [
        DBPRContact.enrichment_status == "enriched",
        DBPRContact.email.isnot(None),
        DBPRContact.email_verified.is_(True),
        DBPRContact.is_opted_out.is_(False),
        DBPRContact.is_hard_bounced.is_(False),
        DBPRContact.is_signed_up.is_(False),
        or_(
            DBPRContact.license_expiry.is_(None),
            DBPRContact.license_expiry >= today,
        ),
        DBPRContact.county_id.in_(launched),
    ]

    if vertical:
        filters.append(DBPRContact.vertical == vertical)

    # Geo: county OR specific ZIPs
    if county_id and zips:
        filters.append(
            or_(
                DBPRContact.county_id == county_id,
                DBPRContact.zip_code.in_(zips),
            )
        )
    elif county_id:
        filters.append(DBPRContact.county_id == county_id)
    elif zips:
        filters.append(DBPRContact.zip_code.in_(zips))

    # Exclude contacts already in this specific campaign
    if exclude_campaign_id is not None:
        subq = (
            CampaignContact.__table__
            .select()
            .where(CampaignContact.campaign_id == exclude_campaign_id)
            .with_only_columns(CampaignContact.dbpr_contact_id)
        )
        filters.append(DBPRContact.id.not_in(subq))

    return filters


def count_eligible(
    county_id: Optional[str],
    zips: list[str],
    vertical: Optional[str],
    exclude_campaign_id: Optional[int] = None,
) -> int:
    filters = _eligibility_filters(county_id, zips, vertical, exclude_campaign_id)
    with get_db_context() as db:
        return db.query(func.count(DBPRContact.id)).filter(*filters).scalar() or 0


def _fetch_eligible(
    db,
    county_id: Optional[str],
    zips: list[str],
    vertical: Optional[str],
    exclude_campaign_id: int,
    limit: int,
) -> list[DBPRContact]:
    filters = _eligibility_filters(county_id, zips, vertical, exclude_campaign_id)
    return (
        db.query(DBPRContact)
        .filter(*filters)
        .order_by(DBPRContact.id.asc())
        .limit(limit)
        .all()
    )


# ---------------------------------------------------------------------------
# Campaign CRUD + Instantly orchestration
# ---------------------------------------------------------------------------

def _build_instantly_schedule(body) -> dict:
    """Convert our send_schedule pydantic model into Instantly campaign_schedule."""
    sched = body.send_schedule
    return {
        "schedules": [
            {
                "name":     sched.schedule_name,
                "timing":   {"from": sched.from_time, "to": sched.to_time},
                "days":     sched.days or {
                    "monday": True, "tuesday": True, "wednesday": True,
                    "thursday": True, "friday": True,
                },
                "timezone": sched.timezone,
            }
        ],
        "start_date": body.start_date.isoformat() if body.start_date else None,
        "end_date":   body.end_date.isoformat()   if body.end_date   else None,
    }


def create_campaign(body) -> dict:
    """
    Persist a draft campaign, create + activate it in Instantly, return the model.
    Rolls back to draft and alerts if Instantly fails.
    """
    now = datetime.now(timezone.utc)
    geo_filter = {
        "county_id": body.county_id,
        "zips":      body.zips or [],
    }

    with get_db_context() as db:
        # Validate template exists
        tmpl = db.get(EmailSequenceTemplate, body.template_id)
        if not tmpl:
            raise HTTPException(status_code=404, detail="Template not found")

        campaign = EmailCampaign(
            name=body.name,
            template_id=body.template_id,
            county_id=body.county_id,
            geo_filter=geo_filter,
            vertical=body.vertical,
            max_contacts=body.max_contacts,
            start_date=body.start_date,
            end_date=body.end_date,
            send_schedule=_build_instantly_schedule(body),
            status="draft",
            created_at=now,
            updated_at=now,
        )
        db.add(campaign)
        db.flush()
        campaign_id = campaign.id

        if not instantly._is_configured():
            campaign.status = "active"
            db.add(campaign)
            db.flush()          # persist the status change before refresh reloads
            db.refresh(campaign)
            from src.api.email_campaign_router import CampaignOut
            return CampaignOut.model_validate(campaign)

        # Build Instantly payload
        steps = template_svc.build_instantly_sequence(tmpl.steps or [])
        schedule = campaign.send_schedule

        try:
            result = instantly.create_campaign(
                name=body.name,
                schedule=schedule,
                sequence_steps=steps,
            )
            if not result or not result.get("id"):
                raise RuntimeError("Instantly returned no campaign id")

            campaign.instantly_campaign_id = result["id"]
            instantly.activate_campaign(result["id"])
            campaign.status = "active"
            campaign.updated_at = datetime.now(timezone.utc)
            db.add(campaign)
            db.flush()          # persist before refresh reloads from the row
            db.refresh(campaign)

        except Exception as exc:
            logger.error("[EmailCampaign] Instantly create failed for campaign %d: %s", campaign_id, exc)
            send_alert(
                subject=f"[FA] Email campaign creation failed: {body.name}",
                body=str(exc),
            )
            campaign.status = "draft"
            db.add(campaign)
            db.flush()
            db.refresh(campaign)

        from src.api.email_campaign_router import CampaignOut
        return CampaignOut.model_validate(campaign)


def pause_campaign(campaign_id: int) -> None:
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            raise HTTPException(status_code=404, detail="Campaign not found")
        if camp.instantly_campaign_id:
            instantly.pause_campaign(camp.instantly_campaign_id)
        camp.status = "paused"
        camp.updated_at = datetime.now(timezone.utc)
        db.add(camp)


def resume_campaign(campaign_id: int) -> None:
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            raise HTTPException(status_code=404, detail="Campaign not found")
        if camp.instantly_campaign_id:
            instantly.activate_campaign(camp.instantly_campaign_id)
        camp.status = "active"
        camp.updated_at = datetime.now(timezone.utc)
        db.add(camp)


_INSTANTLY_SETTING_KEYS = (
    "daily_limit", "daily_max_leads", "email_list",
    "stop_on_reply", "open_tracking", "link_tracking",
)


def update_campaign(campaign_id: int, patch: dict) -> dict:
    """
    Edit a campaign in place (draft/active/paused — 409 on completed).
    Applies local field changes, PATCHes the matching fields to Instantly,
    and returns {"campaign": <detail dict>, "warnings": [...]}.

    Effect notes the caller surfaces:
      - template_id  → re-expands steps and PATCHes Instantly `sequences`
                       (in-flight leads continue from their current step).
      - geo_filter / vertical / max_contacts → local only; affect FUTURE
                       top-ups, existing leads untouched.
      - name / dates / send_schedule / Instantly knobs → PATCHed to Instantly.
    """
    warnings: list[str] = []
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            raise HTTPException(status_code=404, detail="Campaign not found")
        if camp.status == "completed":
            raise HTTPException(status_code=409, detail="Completed campaigns cannot be edited")

        instantly_patch: dict = {}

        if patch.get("name") is not None:
            camp.name = patch["name"]
            instantly_patch["name"] = patch["name"]

        if patch.get("template_id") is not None and patch["template_id"] != camp.template_id:
            tmpl = db.get(EmailSequenceTemplate, patch["template_id"])
            if not tmpl:
                raise HTTPException(status_code=404, detail="Template not found")
            camp.template_id = patch["template_id"]
            instantly_patch["sequences"] = [
                {"steps": template_svc.build_instantly_sequence(tmpl.steps or [])}
            ]
            warnings.append(
                "template changed: new sequence pushed to Instantly; in-flight "
                "leads continue from their current step"
            )

        if patch.get("geo_filter") is not None:
            camp.geo_filter = patch["geo_filter"]
            camp.county_id = patch["geo_filter"].get("county_id")
            warnings.append("geo_filter changed: affects future top-ups only; existing leads unchanged")

        if patch.get("vertical") is not None:
            camp.vertical = patch["vertical"]
            warnings.append("vertical changed: affects future top-ups only; existing leads unchanged")

        if patch.get("max_contacts") is not None:
            current = (
                db.query(func.count(CampaignContact.id))
                .filter(CampaignContact.campaign_id == campaign_id)
                .scalar() or 0
            )
            camp.max_contacts = patch["max_contacts"]
            if patch["max_contacts"] < current:
                warnings.append(
                    f"max_contacts ({patch['max_contacts']}) is below current membership "
                    f"({current}): no leads removed, future top-ups blocked until below cap"
                )

        # Schedule / dates → rebuild Instantly campaign_schedule
        schedule_touched = False
        if patch.get("send_schedule") is not None:
            camp.send_schedule = patch["send_schedule"]
            schedule_touched = True
        if patch.get("start_date") is not None:
            camp.start_date = patch["start_date"]
            schedule_touched = True
        if patch.get("end_date") is not None:
            camp.end_date = patch["end_date"]
            schedule_touched = True
        if schedule_touched:
            sched = dict(camp.send_schedule or {})
            if camp.start_date:
                sched["start_date"] = camp.start_date.isoformat()
            if camp.end_date:
                sched["end_date"] = camp.end_date.isoformat()
            camp.send_schedule = sched
            instantly_patch["campaign_schedule"] = sched

        # Instantly-only knobs → instantly_settings JSONB + pass-through
        settings_patch = {k: patch[k] for k in _INSTANTLY_SETTING_KEYS if patch.get(k) is not None}
        if settings_patch:
            camp.instantly_settings = {**(camp.instantly_settings or {}), **settings_patch}
            instantly_patch.update(settings_patch)

        # Push to Instantly
        if not camp.instantly_campaign_id:
            warnings.append("draft campaign has no Instantly campaign yet — changes saved locally only")
        elif instantly_patch and instantly._is_configured():
            ok = instantly.update_campaign(camp.instantly_campaign_id, instantly_patch)
            if not ok:
                warnings.append("Instantly PATCH failed — local changes saved but Instantly is out of sync")
                send_alert(
                    subject=f"[FA] Campaign update — Instantly PATCH failed: {camp.name}",
                    body=f"Campaign {campaign_id} fields {list(instantly_patch)} not applied in Instantly",
                )

        camp.updated_at = datetime.now(timezone.utc)
        db.add(camp)
        db.flush()

    return {"campaign": get_campaign_detail(campaign_id), "warnings": warnings}


def duplicate_campaign(campaign_id: int) -> dict:
    """Clone template/filters/cap/dates — fresh draft, new Instantly campaign."""
    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        src = db.get(EmailCampaign, campaign_id)
        if not src:
            raise HTTPException(status_code=404, detail="Campaign not found")

        new_camp = EmailCampaign(
            name=f"{src.name} (copy)",
            template_id=src.template_id,
            county_id=src.county_id,
            geo_filter=dict(src.geo_filter or {}),
            vertical=src.vertical,
            max_contacts=src.max_contacts,
            start_date=src.start_date,
            end_date=src.end_date,
            send_schedule=dict(src.send_schedule or {}),
            status="draft",
            created_at=now,
            updated_at=now,
        )
        db.add(new_camp)
        db.flush()
        db.refresh(new_camp)

        from src.api.email_campaign_router import CampaignOut
        return CampaignOut.model_validate(new_camp)


# ---------------------------------------------------------------------------
# Top-up dispatch (B4)
# ---------------------------------------------------------------------------

def topup_campaign(campaign_id: int) -> int:
    """
    Push newly eligible contacts into Instantly and record campaign_contacts rows.
    Returns count of contacts added.
    Idempotent: UNIQUE(campaign_id, dbpr_contact_id) prevents double-add.
    """
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            raise HTTPException(status_code=404, detail="Campaign not found")
        if camp.status not in ("active",):
            return 0

        # Check max_contacts cap
        existing_count = (
            db.query(func.count(CampaignContact.id))
            .filter(CampaignContact.campaign_id == campaign_id)
            .scalar() or 0
        )
        if camp.max_contacts and existing_count >= camp.max_contacts:
            logger.info("[TopUp] Campaign %d at cap (%d)", campaign_id, camp.max_contacts)
            return 0

        limit = _BATCH_SIZE
        if camp.max_contacts:
            limit = min(_BATCH_SIZE, camp.max_contacts - existing_count)

        geo = camp.geo_filter or {}
        contacts = _fetch_eligible(
            db,
            county_id=geo.get("county_id"),
            zips=geo.get("zips", []),
            vertical=camp.vertical,
            exclude_campaign_id=campaign_id,
            limit=limit,
        )
        if not contacts:
            return 0

        added = 0
        batch_leads = []
        batch_contacts = []

        for contact in contacts:
            batch_leads.append({
                "email":        contact.email or contact.work_email or "",
                "first_name":   _first_name(contact.full_name),
                "last_name":    _last_name(contact.full_name),
                "company_name": contact.company_name or "",
                "phone":        contact.phone or "",
                # Custom merge vars consumed by template tags {{website}}/{{location}}/{{linkedIn}}.
                # Instantly substitutes these server-side at send time.
                "website":      contact.domain or "",
                "location":     contact.city or "",
                "linkedin":     contact.linkedin_url or "",
            })
            batch_contacts.append(contact)

        now = datetime.now(timezone.utc)

        if instantly._is_configured() and camp.instantly_campaign_id:
            result = instantly.add_leads(camp.instantly_campaign_id, batch_leads)
            if result is None:
                send_alert(
                    subject=f"[FA] Campaign top-up failed: {camp.name}",
                    body=f"Campaign {campaign_id} — Instantly add_leads returned None",
                )
                return 0
        else:
            result = {"leads_created": len(batch_leads), "leads_skipped": 0}

        # Persist campaign_contacts (ON CONFLICT DO NOTHING via unique constraint)
        for contact in batch_contacts:
            try:
                db.add(CampaignContact(
                    campaign_id=campaign_id,
                    dbpr_contact_id=contact.id,
                    instantly_lead_id=None,  # populated on next sync
                    engagement_status="active",
                    added_at=now,
                ))
                db.flush()
                added += 1
            except Exception:
                db.rollback()  # unique violation — already a member, skip

    logger.info("[TopUp] Campaign %d: +%d contacts added", campaign_id, added)
    return added


def run_all_topups() -> dict:
    """Daily top-up job entry point — iterates all active campaigns."""
    with get_db_context() as db:
        active = db.query(EmailCampaign).filter_by(status="active").all()
        campaign_ids = [c.id for c in active]

    stats: dict[int, int] = {}
    for cid in campaign_ids:
        try:
            stats[cid] = topup_campaign(cid)
        except Exception as exc:
            logger.error("[TopUp] Campaign %d failed: %s", cid, exc)
            stats[cid] = -1
    return stats


# ---------------------------------------------------------------------------
# Read helpers (B7)
# ---------------------------------------------------------------------------

def list_campaigns(status: Optional[str] = None) -> list[dict]:
    with get_db_context() as db:
        q = db.query(EmailCampaign)
        if status:
            q = q.filter(EmailCampaign.status == status)
        campaigns = q.order_by(EmailCampaign.created_at.desc()).all()

        result = []
        for camp in campaigns:
            contact_count = (
                db.query(func.count(CampaignContact.id))
                .filter(CampaignContact.campaign_id == camp.id)
                .scalar() or 0
            )
            snapshot = (
                db.query(CampaignDailyAnalytics)
                .filter(CampaignDailyAnalytics.campaign_id == camp.id)
                .order_by(CampaignDailyAnalytics.snapshot_date.desc())
                .first()
            )
            from src.api.email_campaign_router import CampaignListItem
            result.append(CampaignListItem(
                id=camp.id,
                name=camp.name,
                status=camp.status,
                contact_count=contact_count,
                open_rate=float(snapshot.open_rate) if snapshot else 0.0,
                reply_rate=float(snapshot.reply_rate) if snapshot else 0.0,
                last_synced_at=camp.last_synced_at,
            ))
        return result


def get_campaign_detail(campaign_id: int) -> Optional[dict]:
    """
    Campaign config + live contact count + latest daily-analytics snapshot.
    Returns None if the campaign does not exist (router maps to 404).
    Analytics fields are 0 when no snapshot exists yet (fresh/draft campaign).
    """
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            return None

        contact_count = (
            db.query(func.count(CampaignContact.id))
            .filter(CampaignContact.campaign_id == campaign_id)
            .scalar() or 0
        )
        snap = (
            db.query(CampaignDailyAnalytics)
            .filter(CampaignDailyAnalytics.campaign_id == campaign_id)
            .order_by(CampaignDailyAnalytics.snapshot_date.desc())
            .first()
        )

        return {
            # config
            "id": camp.id,
            "name": camp.name,
            "instantly_campaign_id": camp.instantly_campaign_id,
            "template_id": camp.template_id,
            "county_id": camp.county_id,
            "geo_filter": camp.geo_filter,
            "vertical": camp.vertical,
            "max_contacts": camp.max_contacts,
            "start_date": camp.start_date,
            "end_date": camp.end_date,
            "send_schedule": camp.send_schedule,
            "instantly_settings": camp.instantly_settings or {},
            "status": camp.status,
            "last_synced_at": camp.last_synced_at,
            "created_at": camp.created_at,
            "updated_at": camp.updated_at,
            # live count
            "contact_count": contact_count,
            # latest snapshot (analytics cards) — 0 when no snapshot yet
            "snapshot_date":  snap.snapshot_date if snap else None,
            "emails_sent":    snap.emails_sent if snap else 0,
            "opens":          snap.opens if snap else 0,
            "open_rate":      float(snap.open_rate) if snap else 0.0,
            "replies":        snap.replies if snap else 0,
            "reply_rate":     float(snap.reply_rate) if snap else 0.0,
            "clicks":         snap.clicks if snap else 0,
            "bounces":        snap.bounces if snap else 0,
            "unsubscribes":   snap.unsubscribes if snap else 0,
            "interested":     snap.interested if snap else 0,
        }


def get_summary() -> dict:
    """Dashboard widget — last 30 days aggregates from snapshots."""
    cutoff = date.today() - timedelta(days=30)  # true rolling 30-day window

    with get_db_context() as db:
        active_count = db.query(func.count(EmailCampaign.id)).filter_by(status="active").scalar() or 0
        total_contractors = db.query(func.count(CampaignContact.id)).scalar() or 0

        agg = (
            db.query(
                func.avg(CampaignDailyAnalytics.open_rate).label("avg_open"),
                func.avg(CampaignDailyAnalytics.reply_rate).label("avg_reply"),
            )
            .filter(CampaignDailyAnalytics.snapshot_date >= cutoff)
            .first()
        )

    return {
        "active_campaigns":    active_count,
        "total_contractors":   total_contractors,
        "avg_open_rate_30d":   round(float(agg.avg_open or 0), 4),
        "avg_reply_rate_30d":  round(float(agg.avg_reply or 0), 4),
    }


def list_contacts(
    campaign_id: int,
    engagement_status: Optional[str],
    search: Optional[str],
    page: int,
    page_size: int,
) -> list:
    from src.api.email_campaign_router import ContactListItem
    with get_db_context() as db:
        camp = db.get(EmailCampaign, campaign_id)
        if not camp:
            raise HTTPException(status_code=404, detail="Campaign not found")

        q = (
            db.query(CampaignContact, DBPRContact)
            .join(DBPRContact, CampaignContact.dbpr_contact_id == DBPRContact.id)
            .filter(CampaignContact.campaign_id == campaign_id)
        )
        if engagement_status:
            q = q.filter(CampaignContact.engagement_status == engagement_status)
        if search:
            like = f"%{search}%"
            q = q.filter(
                or_(
                    DBPRContact.full_name.ilike(like),
                    DBPRContact.email.ilike(like),
                    DBPRContact.company_name.ilike(like),
                )
            )
        offset = (page - 1) * page_size
        rows = q.order_by(CampaignContact.added_at.desc()).offset(offset).limit(page_size).all()

        return [
            ContactListItem(
                campaign_contact_id=cc.id,
                dbpr_contact_id=dc.id,
                full_name=dc.full_name,
                company_name=dc.company_name,
                email=dc.email,
                engagement_status=cc.engagement_status,
                is_signed_up=dc.is_signed_up,
                last_activity_at=cc.last_activity_at,
                converted_at=cc.converted_at,
            )
            for cc, dc in rows
        ]


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------

def _first_name(full_name: Optional[str]) -> str:
    if not full_name:
        return ""
    parts = full_name.split(",", 1)
    if len(parts) == 2:
        first_tokens = parts[1].strip().split()
        return first_tokens[0] if first_tokens else ""
    tokens = full_name.split()
    return tokens[0] if tokens else ""


def _last_name(full_name: Optional[str]) -> str:
    if not full_name:
        return ""
    parts = full_name.split(",", 1)
    return parts[0].strip() if len(parts) == 2 else ""
