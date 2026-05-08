"""
Manual action counter service.

Counts tracked manual actions logged in manual_action_log for a given
subscriber within the current ISO week. Used by:
  - /api/feed/{uuid} to populate ap_lite_eligible + manual_actions_this_week
  - ap_lite_sweep.py for the Monday cron
"""
from datetime import date, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.models import ManualActionLog


def _monday_of_week(ref: date) -> date:
    return ref - timedelta(days=ref.weekday())


def count_this_week(db: Session, subscriber_id: int) -> int:
    week_start = _monday_of_week(date.today())
    return db.execute(
        select(func.count()).select_from(ManualActionLog).where(
            ManualActionLog.subscriber_id == subscriber_id,
            ManualActionLog.week_start == week_start,
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
