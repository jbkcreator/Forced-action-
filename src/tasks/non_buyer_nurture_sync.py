"""
Daily non-buyer nurture Instantly sync.

Pulls per-lead status from the shared nurture campaign and applies
unsubscribe/bounce terminal states + lead-id backfill via
src.services.non_buyer_nurture.apply_instantly_status. Never touches DBPR
CampaignContact — this is a dedicated campaign/sync, not the contractor
outbound suppression list.

Cron slot: daily, after the nurture sweep.

Usage:
    python -m src.tasks.non_buyer_nurture_sync
    python -m src.tasks.non_buyer_nurture_sync --dry-run
"""

import sys
from datetime import datetime, timezone
from typing import Optional

from src.utils.logger import setup_logging, get_logger
from src.services.email import send_alert

setup_logging()
logger = get_logger(__name__)


def run(dry_run: bool = False) -> dict:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services import instantly_service as instantly
    from src.services import non_buyer_nurture

    started = datetime.now(timezone.utc).isoformat()
    logger.info("[NurtureSync] Starting non-buyer nurture sync (dry_run=%s)", dry_run)

    campaign_id = get_settings().non_buyer_nurture_campaign_id
    if not campaign_id:
        logger.warning("[NurtureSync] NON_BUYER_NURTURE_CAMPAIGN_ID not configured — skipping")
        return {"skipped": "campaign_id not configured", "started_at": started}

    synced = 0
    try:
        cursor: Optional[str] = None
        while True:
            page = instantly.list_leads(campaign_id, cursor=cursor)
            if not page:
                break
            leads = page.get("leads", [])
            if not leads:
                break

            if not dry_run:
                with get_db_context() as db:
                    for lead in leads:
                        email = (lead.get("email") or "").lower().strip()
                        if not email:
                            continue
                        lead_id = lead.get("id") or lead.get("lead_id")
                        raw_status = lead.get("interest_status") or lead.get("status") or "active"
                        mapped_status = instantly.map_lead_status(raw_status)
                        non_buyer_nurture.apply_instantly_status(
                            db, email, mapped_status, instantly_lead_id=lead_id,
                        )
                        synced += 1
            else:
                synced += len(leads)

            cursor = page.get("next_starting_after")
            if not cursor:
                break
    except Exception as exc:
        logger.error("[NurtureSync] Sync crashed: %s", exc, exc_info=True)
        send_alert(subject="[FA] Non-buyer nurture sync failed", body=str(exc))
        return {"error": str(exc), "started_at": started}

    logger.info("[NurtureSync] Done. synced=%d", synced)
    return {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "synced": synced,
        "dry_run": dry_run,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run)
    print(result)
    sys.exit(0 if "error" not in result else 1)
