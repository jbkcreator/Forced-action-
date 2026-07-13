"""
Daily non-buyer nurture sweep.

Finds eligible non-buyer candidates (free-signup / checkout-abandon /
waitlist) and enrolls up to the daily cap into the shared Instantly nurture
campaign. Thin — all logic lives in src.services.non_buyer_nurture.

Cron slot: daily, any time after the app's normal signup/waitlist traffic.

Usage:
    python -m src.tasks.non_buyer_nurture_sweep
    python -m src.tasks.non_buyer_nurture_sweep --dry-run
"""

import sys
from datetime import datetime, timezone

from src.utils.logger import setup_logging, get_logger
from src.services.email import send_alert

setup_logging()
logger = get_logger(__name__)


def run(dry_run: bool = False) -> dict:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services import non_buyer_nurture

    started = datetime.now(timezone.utc).isoformat()
    logger.info("[NurtureSweep] Starting non-buyer nurture sweep (dry_run=%s)", dry_run)

    campaign_id = get_settings().non_buyer_nurture_campaign_id
    if not campaign_id:
        logger.warning("[NurtureSweep] NON_BUYER_NURTURE_CAMPAIGN_ID not configured — skipping")
        return {"skipped": "campaign_id not configured", "started_at": started}

    try:
        with get_db_context() as db:
            candidates = non_buyer_nurture.find_candidates(db)
            if dry_run:
                logger.info("[NurtureSweep] DRY RUN — would enroll %d candidates", len(candidates))
                return {"dry_run": True, "candidates": len(candidates), "started_at": started}

            reconciled = non_buyer_nurture.reconcile_conversions(db)
            result = non_buyer_nurture.enroll(db, candidates, campaign_id=campaign_id)
    except Exception as exc:
        logger.error("[NurtureSweep] Sweep crashed: %s", exc, exc_info=True)
        send_alert(subject="[FA] Non-buyer nurture sweep failed", body=str(exc))
        return {"error": str(exc), "started_at": started}

    logger.info("[NurtureSweep] Done. %s reconciled=%d", result, reconciled)
    return {
        "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "reconciled": reconciled,
        **result,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run(dry_run=args.dry_run)
    logger.info("[NurtureSweep] result: %s", result)
    sys.exit(0 if "error" not in result else 1)
