"""
Daily email campaign top-up task.

Runs after DBPR/Clay enrichment. Iterates all active campaigns and pushes
newly eligible contractors into Instantly, inserting campaign_contacts rows.

Cron slot: after enrichment (e.g. 08:30 UTC), before analytics sync.

Usage:
    python -m src.tasks.email_campaign_topup
    python -m src.tasks.email_campaign_topup --campaign-id 5
    python -m src.tasks.email_campaign_topup --dry-run
"""

import logging
import sys
from datetime import datetime, timezone

from src.utils.logger import setup_logging, get_logger
from src.services.email import send_alert

setup_logging()
logger = get_logger(__name__)


def run(campaign_id: int | None = None, dry_run: bool = False) -> dict:
    from src.services.email_campaigns import run_all_topups, topup_campaign

    started = datetime.now(timezone.utc).isoformat()
    logger.info("[TopUp] Starting email campaign top-up (dry_run=%s)", dry_run)

    if dry_run:
        logger.info("[TopUp] DRY RUN — no DB writes or Instantly calls")
        return {"dry_run": True, "started_at": started}

    try:
        if campaign_id is not None:
            added = topup_campaign(campaign_id)
            stats = {campaign_id: added}
        else:
            stats = run_all_topups()
    except Exception as exc:
        logger.error("[TopUp] Top-up task crashed: %s", exc, exc_info=True)
        send_alert(
            subject="[FA] Email campaign top-up task failed",
            body=str(exc),
        )
        return {"error": str(exc), "started_at": started}

    total = sum(v for v in stats.values() if v >= 0)
    errors = [cid for cid, v in stats.items() if v < 0]
    logger.info("[TopUp] Done. total_added=%d campaigns=%d errors=%s", total, len(stats), errors)
    return {
        "started_at":   started,
        "finished_at":  datetime.now(timezone.utc).isoformat(),
        "total_added":  total,
        "per_campaign": stats,
        "errors":       errors,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-id", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run(campaign_id=args.campaign_id, dry_run=args.dry_run)
    print(result)
    sys.exit(0 if "error" not in result else 1)
