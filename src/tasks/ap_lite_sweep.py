"""
AP Lite upgrade sweep task.

Runs daily at 0 14 * * * (2 PM UTC). Finds territory_lock subscribers
who performed >= 10 manual actions in the trailing 7 days and emits
Cora AP Lite upsell events. Idempotency key is bucketed by ISO week,
so each subscriber fires at most once per calendar week even though
the sweep runs every day.

Usage:
    python -m src.tasks.ap_lite_sweep [--dry-run]
"""
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.ap_lite import (
    AP_LITE_ELIGIBLE_TIERS,
    AP_LITE_IDEMPOTENCY_WINDOW,
    AP_LITE_THRESHOLD_PER_WEEK,
)
from src.core.database import get_db_context
from src.core.models import ManualActionLog, Subscriber

logger = logging.getLogger(__name__)


def _last_monday(ref: date) -> date:
    """Monday of the previous week (kept for idempotency key bucketing)."""
    this_monday = ref - timedelta(days=ref.weekday())
    return this_monday - timedelta(weeks=1)


def count_actions_trailing_7d(db: Session, subscriber_id: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    return db.execute(
        select(func.count()).select_from(ManualActionLog).where(
            ManualActionLog.subscriber_id == subscriber_id,
            ManualActionLog.created_at >= cutoff,
        )
    ).scalar() or 0


def _emit_event(subscriber_id: int, actions_count: int, window_end: date) -> None:
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event

    iso_week = window_end.strftime(AP_LITE_IDEMPOTENCY_WINDOW)
    evt = Event(
        event_type="subscriber_crossed_ap_lite_threshold",
        subscriber_id=subscriber_id,
        payload={
            "manual_actions_trailing_7d": actions_count,
            "window_end": window_end.isoformat(),
            "threshold": AP_LITE_THRESHOLD_PER_WEEK,
        },
        source="cron",
        decision_id=str(uuid.uuid4()),
        idempotency_key=f"aplite:{subscriber_id}:{iso_week}",
    )
    try:
        dispatch_event(evt.to_dispatch_dict())
    except Exception as exc:
        logger.error("ap_lite_sweep emit failed sub=%s: %s", subscriber_id, exc)


def run_sweep(dry_run: bool = False) -> dict:
    results = {
        "lock_holders_checked": 0,
        "candidates_found": 0,
        "events_emitted": 0,
        "errors": 0,
    }

    today = date.today()

    with get_db_context() as db:
        subs = db.execute(
            select(Subscriber).where(
                Subscriber.tier.in_(AP_LITE_ELIGIBLE_TIERS),
                Subscriber.status == "active",
            )
        ).scalars().all()

        for sub in subs:
            results["lock_holders_checked"] += 1
            try:
                n = count_actions_trailing_7d(db, sub.id)
                if n < AP_LITE_THRESHOLD_PER_WEEK:
                    continue

                results["candidates_found"] += 1
                logger.info(
                    "ap_lite: candidate sub=%s actions=%d window_end=%s",
                    sub.id, n, today,
                )

                if not dry_run:
                    sub.ap_lite_candidate_at = datetime.now(timezone.utc)
                    db.flush()
                    _emit_event(sub.id, n, today)
                    results["events_emitted"] += 1

            except Exception as exc:
                logger.error("ap_lite_sweep error sub=%s: %s", sub.id, exc)
                results["errors"] += 1

    logger.info(
        "[ApLiteSweep] checked=%d candidates=%d emitted=%d errors=%d dry_run=%s",
        results["lock_holders_checked"],
        results["candidates_found"],
        results["events_emitted"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run_sweep(dry_run=dry))
