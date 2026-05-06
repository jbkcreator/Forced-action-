"""
AP Lite upgrade sweep task.

Runs every Monday at 0 14 * * 1 (2 PM UTC) after the weekly reset.
Finds territory_lock subscribers who performed >= 10 manual actions
in the previous week and emits Cora AP Lite upsell events.

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
    """Monday of the previous week."""
    this_monday = ref - timedelta(days=ref.weekday())
    return this_monday - timedelta(weeks=1)


def count_actions_week(db: Session, subscriber_id: int, week_start: date) -> int:
    return db.execute(
        select(func.count()).select_from(ManualActionLog).where(
            ManualActionLog.subscriber_id == subscriber_id,
            ManualActionLog.week_start == week_start,
        )
    ).scalar() or 0


def _emit_event(subscriber_id: int, actions_count: int, week_start: date) -> None:
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event

    iso_week = week_start.strftime(AP_LITE_IDEMPOTENCY_WINDOW)
    evt = Event(
        event_type="subscriber_crossed_ap_lite_threshold",
        subscriber_id=subscriber_id,
        payload={
            "manual_actions_last_week": actions_count,
            "week_start": week_start.isoformat(),
            "threshold": AP_LITE_THRESHOLD_PER_WEEK,
        },
        source="cron",
        decision_id=str(uuid.uuid4()),
        idempotency_key=f"aplite:{subscriber_id}:{iso_week}",
    )
    try:
        from src.agents.supervisor import dispatch_event
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

    prev_week_start = _last_monday(date.today())

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
                n = count_actions_week(db, sub.id, prev_week_start)
                if n < AP_LITE_THRESHOLD_PER_WEEK:
                    continue

                results["candidates_found"] += 1
                logger.info(
                    "ap_lite: candidate sub=%s actions=%d week=%s",
                    sub.id, n, prev_week_start,
                )

                if not dry_run:
                    sub.ap_lite_candidate_at = datetime.now(timezone.utc)
                    db.flush()
                    _emit_event(sub.id, n, prev_week_start)
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
