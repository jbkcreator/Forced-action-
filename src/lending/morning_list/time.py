"""Eastern-time helpers shared by the morning list modules."""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def et_date(now: datetime) -> date:
    return now.astimezone(ET).date()


def et_day_start(now: datetime) -> datetime:
    """Midnight ET of ``now``'s ET day, in UTC."""
    return datetime.combine(et_date(now), time.min, ET).astimezone(timezone.utc)
