"""
Nightly retry of failed Slack posts for human close escalations.

Picks up rows where posted_at IS NULL and post_attempts < 3.
On 3rd failure, logs ERROR — manual action required.

Cron: 30 1 * * * (01:30 UTC nightly)

Usage:
    python -m src.tasks.human_close_retry [--dry-run]
"""
import logging
import sys

from src.core.database import get_db_context
from src.services.human_close_routing import retry_failed_posts

logger = logging.getLogger(__name__)


def run_retry(dry_run: bool = False) -> dict:
    if dry_run:
        with get_db_context() as db:
            from sqlalchemy import select
            from src.core.models import HumanCloseEscalation
            from datetime import datetime, timedelta, timezone
            cutoff = datetime.now(timezone.utc) - timedelta(days=3)
            rows = db.execute(
                select(HumanCloseEscalation).where(
                    HumanCloseEscalation.posted_at.is_(None),
                    HumanCloseEscalation.post_attempts < 3,
                    HumanCloseEscalation.routed_at >= cutoff,
                )
            ).scalars().all()
            result = {"dry_run": True, "eligible": len(rows)}
            logger.info("[HumanCloseRetry] dry_run eligible=%d", len(rows))
            return result

    with get_db_context() as db:
        result = retry_failed_posts(db)

    logger.info(
        "[HumanCloseRetry] retried=%d succeeded=%d failed=%d capped=%d",
        result["retried"], result["succeeded"], result["failed"], result["capped"],
    )
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_retry(dry_run=dry))
