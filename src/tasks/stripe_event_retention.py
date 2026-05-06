"""
Stripe webhook event retention — fa004 (2026-05-04).

Stripe retries events for at most 30 days, so retaining StripeWebhookEvent
ids beyond 90 days has no idempotency value and grows the table unbounded.
This weekly job deletes rows older than the retention window.

Schedule (add to crontab):
    0 4 * * 0    cd /opt/forced-action && python -m src.tasks.stripe_event_retention
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func, delete

from src.core.database import Database
from src.core.models import StripeWebhookEvent

logger = logging.getLogger(__name__)

RETENTION_DAYS = 90


def prune_old_events(retention_days: int = RETENTION_DAYS, dry_run: bool = False) -> dict:
    """Delete StripeWebhookEvent rows older than `retention_days`.

    Returns a dict {'before': N, 'deleted': M, 'after': K} for ops visibility.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    db = Database()
    with db.session_scope() as session:
        before = session.execute(select(func.count()).select_from(StripeWebhookEvent)).scalar() or 0
        eligible = session.execute(
            select(func.count())
            .select_from(StripeWebhookEvent)
            .where(StripeWebhookEvent.processed_at < cutoff)
        ).scalar() or 0

        if dry_run:
            logger.info(
                "[StripeEventRetention] dry-run: would delete %d/%d rows older than %s",
                eligible, before, cutoff.isoformat(),
            )
            return {"before": before, "deleted": 0, "after": before, "would_delete": eligible, "dry_run": True}

        if eligible == 0:
            logger.info("[StripeEventRetention] nothing to prune (cutoff=%s)", cutoff.isoformat())
            return {"before": before, "deleted": 0, "after": before}

        result = session.execute(
            delete(StripeWebhookEvent).where(StripeWebhookEvent.processed_at < cutoff)
        )
        deleted = result.rowcount or 0
        after = before - deleted
        logger.info(
            "[StripeEventRetention] pruned %d rows (before=%d, after=%d, cutoff=%s)",
            deleted, before, after, cutoff.isoformat(),
        )
        return {"before": before, "deleted": deleted, "after": after}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry_run = "--dry-run" in sys.argv
    days = RETENTION_DAYS
    for arg in sys.argv:
        if arg.startswith("--days="):
            days = int(arg.split("=", 1)[1])
    result = prune_old_events(retention_days=days, dry_run=dry_run)
    print(result)
