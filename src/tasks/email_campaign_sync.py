"""
Daily email campaign sync task.

Pulls per-campaign analytics + per-contact engagement status from Instantly.
Maps Instantly unsubscribe/hard-bounce → global suppression on dbpr_contacts.
Runs lifecycle check (auto-complete campaigns past end_date or exhausted).

Cron slot: 22:00 UTC (after top-up, before Daily Dashboard at 23:30).

Usage:
    python -m src.tasks.email_campaign_sync
    python -m src.tasks.email_campaign_sync --dry-run
"""

import logging
import sys
from datetime import date, datetime, timezone
from typing import Optional

from src.utils.logger import setup_logging, get_logger
from src.services.email import send_alert

setup_logging()
logger = get_logger(__name__)


def _sync_analytics(campaign, today: date, dry_run: bool) -> None:
    from src.core.database import get_db_context
    from src.core.models import CampaignDailyAnalytics
    from src.services import instantly_service as instantly
    from src.services.instantly_service import map_analytics

    if not campaign.instantly_campaign_id:
        return

    rows = instantly.get_daily_analytics(
        campaign_ids=[campaign.instantly_campaign_id],
        start_date=today.isoformat(),
        end_date=today.isoformat(),
    )
    if not rows:
        return

    raw = rows[0] if isinstance(rows[0], dict) else {}
    mapped = map_analytics(raw)

    if dry_run:
        logger.info("[Sync] DRY RUN — would upsert analytics for campaign %d: %s", campaign.id, mapped)
        return

    with get_db_context() as db:
        existing = (
            db.query(CampaignDailyAnalytics)
            .filter_by(campaign_id=campaign.id, snapshot_date=today)
            .first()
        )
        if existing:
            for k, v in mapped.items():
                setattr(existing, k, v)
            db.add(existing)
        else:
            from src.core.models import CampaignContact
            total = (
                db.query(__import__("sqlalchemy").func.count(CampaignContact.id))
                .filter(CampaignContact.campaign_id == campaign.id)
                .scalar() or 0
            )
            db.add(CampaignDailyAnalytics(
                campaign_id=campaign.id,
                snapshot_date=today,
                total_contacts=total,
                emails_sent=mapped.get("emails_sent", 0),
                opens=mapped.get("opens", 0),
                open_rate=mapped.get("open_rate", 0),
                replies=mapped.get("replies", 0),
                reply_rate=mapped.get("reply_rate", 0),
                clicks=mapped.get("clicks", 0),
                bounces=mapped.get("bounces", 0),
                unsubscribes=mapped.get("unsubscribes", 0),
                interested=0,
                created_at=datetime.now(timezone.utc),
            ))


def _sync_lead_statuses(campaign, dry_run: bool) -> dict:
    """
    Pull per-contact statuses from Instantly, update engagement_status,
    and map unsubscribe/hard-bounce to global suppression on DBPRContact.
    """
    from src.core.database import get_db_context
    from src.core.models import CampaignContact, DBPRContact
    from src.services import instantly_service as instantly
    from src.services.instantly_service import map_lead_status

    if not campaign.instantly_campaign_id:
        return {"synced": 0}

    synced = 0
    cursor: Optional[str] = None

    while True:
        page = instantly.list_leads(campaign.instantly_campaign_id, cursor=cursor)
        if not page:
            break
        leads = page.get("leads", [])
        if not leads:
            break

        if not dry_run:
            with get_db_context() as db:
                for lead in leads:
                    instantly_lead_id = lead.get("id") or lead.get("lead_id")
                    email = lead.get("email", "").lower()
                    raw_status = (
                        lead.get("interest_status")
                        or lead.get("status")
                        or "active"
                    )
                    eng_status = map_lead_status(raw_status)
                    now = datetime.now(timezone.utc)

                    # Find campaign_contact by instantly_lead_id or email
                    cc = None
                    if instantly_lead_id:
                        cc = (
                            db.query(CampaignContact)
                            .filter_by(campaign_id=campaign.id, instantly_lead_id=instantly_lead_id)
                            .first()
                        )
                    if not cc and email:
                        # fallback: match by email through DBPRContact
                        dc = db.query(DBPRContact).filter(
                            DBPRContact.email.ilike(email)
                        ).first()
                        if dc:
                            cc = (
                                db.query(CampaignContact)
                                .filter_by(campaign_id=campaign.id, dbpr_contact_id=dc.id)
                                .first()
                            )

                    if cc:
                        cc.engagement_status = eng_status
                        cc.last_activity_at = now
                        if instantly_lead_id:
                            cc.instantly_lead_id = instantly_lead_id
                        db.add(cc)

                        # Global suppression mapping
                        dc = db.get(DBPRContact, cc.dbpr_contact_id)
                        if dc:
                            if eng_status == "unsubscribed" and not dc.is_opted_out:
                                dc.is_opted_out = True
                                dc.updated_at = now
                                db.add(dc)
                                logger.info(
                                    "[Sync] is_opted_out=TRUE for dbpr_contact %d (campaign %d)",
                                    dc.id, campaign.id,
                                )
                            elif eng_status == "bounced" and not dc.is_hard_bounced:
                                dc.is_hard_bounced = True
                                dc.updated_at = now
                                db.add(dc)
                                logger.info(
                                    "[Sync] is_hard_bounced=TRUE for dbpr_contact %d (campaign %d)",
                                    dc.id, campaign.id,
                                )
                        synced += 1
        else:
            synced += len(leads)

        cursor = page.get("next_starting_after")
        if not cursor:
            break

    return {"synced": synced}


def _lifecycle_check(campaign, dry_run: bool) -> None:
    """Auto-complete campaigns past end_date or fully exhausted."""
    from src.core.database import get_db_context
    from src.core.models import CampaignContact
    from src.services import instantly_service as instantly
    import sqlalchemy as sa

    today = date.today()
    should_complete = False

    if campaign.end_date and campaign.end_date < today:
        should_complete = True
        reason = "end_date passed"
    else:
        # Check if all contacts finished and no eligible remain
        with get_db_context() as db:
            from src.services.email_campaigns import count_eligible
            from src.services.email_campaigns import _eligibility_filters
            geo = campaign.geo_filter or {}
            active_members = (
                db.query(sa.func.count(CampaignContact.id))
                .filter(
                    CampaignContact.campaign_id == campaign.id,
                    CampaignContact.engagement_status == "active",
                )
                .scalar() or 0
            )
            eligible_remaining = count_eligible(
                county_id=geo.get("county_id"),
                zips=geo.get("zips", []),
                vertical=campaign.vertical,
                exclude_campaign_id=campaign.id,
            )
            if active_members == 0 and eligible_remaining == 0:
                should_complete = True
                reason = "all contacts finished and no eligible remain"

    if not should_complete:
        return

    logger.info("[Sync] Completing campaign %d (%s): %s", campaign.id, campaign.name, reason)
    if not dry_run:
        if campaign.instantly_campaign_id:
            instantly.pause_campaign(campaign.instantly_campaign_id)
        with get_db_context() as db:
            camp = db.get(type(campaign), campaign.id)
            if camp:
                camp.status = "completed"
                camp.updated_at = datetime.now(timezone.utc)
                db.add(camp)


def run(dry_run: bool = False) -> dict:
    from src.core.database import get_db_context
    from src.core.models import EmailCampaign

    started = datetime.now(timezone.utc).isoformat()
    today = date.today()

    logger.info("[Sync] Starting email campaign sync (dry_run=%s)", dry_run)

    with get_db_context() as db:
        campaigns = (
            db.query(EmailCampaign)
            .filter(EmailCampaign.status.in_(["active", "paused"]))
            .all()
        )

    stats: dict = {"campaigns": len(campaigns), "analytics": 0, "leads_synced": 0, "errors": []}

    for camp in campaigns:
        try:
            _sync_analytics(camp, today, dry_run)
            stats["analytics"] += 1
        except Exception as exc:
            logger.error("[Sync] Analytics failed for campaign %d: %s", camp.id, exc)
            stats["errors"].append(f"analytics:{camp.id}:{exc}")

        try:
            result = _sync_lead_statuses(camp, dry_run)
            stats["leads_synced"] += result.get("synced", 0)
        except Exception as exc:
            logger.error("[Sync] Lead status sync failed for campaign %d: %s", camp.id, exc)
            stats["errors"].append(f"leads:{camp.id}:{exc}")

        try:
            _lifecycle_check(camp, dry_run)
        except Exception as exc:
            logger.error("[Sync] Lifecycle check failed for campaign %d: %s", camp.id, exc)
            stats["errors"].append(f"lifecycle:{camp.id}:{exc}")

        if not dry_run:
            with get_db_context() as db:
                c = db.get(EmailCampaign, camp.id)
                if c:
                    c.last_synced_at = datetime.now(timezone.utc)
                    db.add(c)

    if stats["errors"]:
        send_alert(
            subject=f"[FA] Email campaign sync had {len(stats['errors'])} error(s)",
            body="\n".join(stats["errors"]),
        )

    stats["started_at"] = started
    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("[Sync] Done. %s", stats)
    return stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run)
    print(result)
    sys.exit(0 if not result.get("errors") else 1)
