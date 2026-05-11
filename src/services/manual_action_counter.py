"""
Manual action counter service.

Counts tracked manual actions logged in manual_action_log for a given
subscriber over a trailing 7-day window. Used by:
  - /api/feed/{uuid} to populate ap_lite_eligible + manual_actions_this_week
  - ap_lite_sweep.py (daily cron)

Rolling window (created_at >= now-7d) replaces the previous ISO-week bucket
so that activity spanning a Mon 00:00 UTC boundary still trips the threshold.
Backed by Index("idx_mal_created", "created_at") in core.models.
"""
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import ManualActionLog


def _monday_of_week(ref: date) -> date:
    return ref - timedelta(days=ref.weekday())


def count_this_week(db: Session, subscriber_id: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    return db.execute(
        select(func.count()).select_from(ManualActionLog).where(
            ManualActionLog.subscriber_id == subscriber_id,
            ManualActionLog.created_at >= cutoff,
        )
    ).scalar() or 0


def log_action(db: Session, subscriber_id: int, action_type: str) -> None:
    week_start = _monday_of_week(date.today())
    db.add(ManualActionLog(
        subscriber_id=subscriber_id,
        action_type=action_type,
        week_start=week_start,
    ))
    db.flush()
