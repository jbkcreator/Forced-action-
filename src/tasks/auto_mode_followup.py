"""
Auto Mode follow-up — Stage 5.

Runs every 30 min via cron. For every `auto_mode_first_text` MessageOutcome
that was sent more than 24 hours ago and has no `replied_at`, this job fires
a Synthflow voicemail by tagging the subscriber's GHL contact with
`auto_mode_vm`. The actual VM dispatch is handled by a GHL workflow
(triggered on tag) so we never block on Synthflow latency.

Idempotency: a row's `clicked_at` field is repurposed as a "vm_dispatched_at"
flag — if already set, we skip the row. (Avoids adding a new column for a
single-purpose tracker.)

Run via `python -m src.tasks.auto_mode_followup [--dry-run]`.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import MessageOutcome, Subscriber

logger = logging.getLogger(__name__)


_LOOKBACK_HOURS = 24
_MAX_AGE_HOURS = 96     # don't VM-drop on stale leads (>4 days)


def run(dry_run: bool = False) -> dict:
    stats = {"checked": 0, "vm_triggered": 0, "skipped_already_done": 0, "skipped_replied": 0, "errors": 0}
    now = datetime.now(timezone.utc)
    cutoff_min = now - timedelta(hours=_MAX_AGE_HOURS)
    cutoff_max = now - timedelta(hours=_LOOKBACK_HOURS)

    with get_db_context() as db:
        rows = db.execute(
            select(MessageOutcome).where(
                MessageOutcome.template_id == "auto_mode_first_text",
                MessageOutcome.sent_at <= cutoff_max,
                MessageOutcome.sent_at >= cutoff_min,
            )
        ).scalars().all()

        for outcome in rows:
            stats["checked"] += 1
            if outcome.replied_at is not None:
                stats["skipped_replied"] += 1
                continue
            if outcome.clicked_at is not None:
                # `clicked_at` reused as vm_dispatched_at flag (see module docstring).
                stats["skipped_already_done"] += 1
                continue

            sub = outcome.subscriber_id and db.get(Subscriber, outcome.subscriber_id)
            if not sub:
                continue

            if dry_run:
                logger.info(
                    "[AutoModeFollowup] DRY-RUN would VM subscriber=%d outcome=%d",
                    sub.id, outcome.id,
                )
                continue

            try:
                _trigger_vm_for_subscriber(sub)
                outcome.clicked_at = now   # mark "vm dispatched"
                db.flush()
                stats["vm_triggered"] += 1
            except Exception as exc:
                logger.error(
                    "[AutoModeFollowup] VM trigger failed: subscriber=%d outcome=%d err=%s",
                    sub.id, outcome.id, exc,
                )
                stats["errors"] += 1

    logger.info("[AutoModeFollowup] %s", stats)
    return stats


def _trigger_vm_for_subscriber(sub: Subscriber) -> None:
    """Apply the auto_mode_vm tag to the subscriber's GHL contact (workflow handles dispatch)."""
    if not sub.ghl_contact_id:
        logger.debug("[AutoModeFollowup] subscriber=%d has no GHL contact - skipping", sub.id)
        return
    from src.services.synthflow_service import _apply_tags_to_contact
    _apply_tags_to_contact(sub.ghl_contact_id, ["auto_mode_vm"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
