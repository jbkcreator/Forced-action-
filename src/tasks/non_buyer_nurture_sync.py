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
from datetime import datetime, timedelta, timezone
from typing import Optional

# Grace before an enrolled-but-missing row is reset to eligible: a lead sent in
# the last sweep may not appear in Instantly's list yet, so only reconcile rows
# older than this to avoid resetting a just-submitted lead mid-flight.
_RECONCILE_GRACE = timedelta(hours=6)

from src.utils.logger import setup_logging, get_logger
from src.services.email import send_alert

setup_logging()
logger = get_logger(__name__)


def run(dry_run: bool = False) -> dict:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services import instantly_service as instantly
    from src.services import non_buyer_nurture

    started_dt = datetime.now(timezone.utc)
    started = started_dt.isoformat()
    logger.info("[NurtureSync] Starting non-buyer nurture sync (dry_run=%s)", dry_run)

    campaign_id = get_settings().non_buyer_nurture_campaign_id
    if not campaign_id:
        logger.warning("[NurtureSync] NON_BUYER_NURTURE_CAMPAIGN_ID not configured — skipping")
        return {"skipped": "campaign_id not configured", "started_at": started}

    synced = 0
    # Every email Instantly actually holds in the campaign. Any row we marked
    # `enrolled` for this campaign that is NOT here was rejected at add time
    # (Instantly's add response gives batch counts, not per-email results, so
    # the enrol step marks the whole batch optimistically). Reconcile those
    # back to `eligible` so the next sweep retries them instead of losing them.
    seen: set[str] = set()
    reset = 0
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
                        seen.add(email)
                        lead_id = lead.get("id") or lead.get("lead_id")
                        raw_status = lead.get("interest_status") or lead.get("status") or "active"
                        mapped_status = instantly.map_lead_status(raw_status)
                        non_buyer_nurture.apply_instantly_status(
                            db, email, mapped_status, instantly_lead_id=lead_id,
                        )
                        synced += 1
            else:
                for lead in leads:
                    email = (lead.get("email") or "").lower().strip()
                    if email:
                        seen.add(email)
                synced += len(leads)

            cursor = page.get("next_starting_after")
            if not cursor:
                break
    except Exception as exc:
        logger.error("[NurtureSync] Sync crashed: %s", exc, exc_info=True)
        send_alert(subject="[FA] Non-buyer nurture sync failed", body=str(exc))
        return {"error": str(exc), "started_at": started}

    # Reconcile rejected-at-add leads. Only when the campaign returned at least
    # one lead — an empty list is indistinguishable from an API hiccup and must
    # never reset the whole campaign to eligible.
    if seen and not dry_run:
        reset = _reset_missing_enrolled(campaign_id, seen, started_dt - _RECONCILE_GRACE)

    logger.info("[NurtureSync] Done. synced=%d reset=%d", synced, reset)
    return {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "synced": synced,
        "reset": reset,
        "dry_run": dry_run,
    }


def _reset_missing_enrolled(campaign_id: str, seen: set[str], cutoff: datetime) -> int:
    """Reset rows marked `enrolled` for this campaign that Instantly never
    actually accepted (not in `seen`) back to `eligible`, so the next sweep
    retries them. Only touches rows enrolled before `cutoff` (grace) and skips
    terminal/converted rows. Returns the number reset."""
    from sqlalchemy import text
    from src.core.database import get_db_context

    with get_db_context() as db:
        result = db.execute(
            text("""
                UPDATE non_buyer_nurture_sequences
                SET status = 'eligible',
                    enrolled_at = NULL,
                    instantly_campaign_id = NULL
                WHERE instantly_campaign_id = :cid
                  AND status = 'enrolled'
                  AND enrolled_at < :cutoff
                  AND lower(email) <> ALL(:seen)
            """),
            {"cid": campaign_id, "cutoff": cutoff, "seen": list(seen)},
        )
        count = result.rowcount or 0
        if count:
            logger.warning(
                "[NurtureSync] reconciled %d enrolled-but-missing lead(s) back to eligible "
                "(campaign %s)", count, campaign_id,
            )
        return count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run)
    logger.info("[NurtureSync] result: %s", result)
    sys.exit(0 if "error" not in result else 1)
