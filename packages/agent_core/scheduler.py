"""Standing-job clock: which jobs are due at a given minute. Job bodies live with the host agent."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, time
from typing import Literal
from zoneinfo import ZoneInfo

Cadence = Literal["daily", "weekly", "interval"]


@dataclass(frozen=True)
class StandingJob:
    name: str
    cadence: Cadence
    fire_time: time | None = None
    weekday: int | None = None  # 0 = Monday
    interval_minutes: int | None = None

    def __post_init__(self) -> None:
        if self.cadence in ("daily", "weekly") and self.fire_time is None:
            raise ValueError(f"{self.name}: {self.cadence} jobs need fire_time")
        if self.cadence == "weekly" and self.weekday not in range(7):
            raise ValueError(f"{self.name}: weekly jobs need weekday 0-6")
        if self.cadence == "interval" and not (self.interval_minutes and 0 < self.interval_minutes <= 1440):
            raise ValueError(f"{self.name}: interval jobs need interval_minutes in 1..1440")


def due_jobs(now: datetime, jobs: Iterable[StandingJob], timezone_name: str = "America/New_York") -> list[StandingJob]:
    """Jobs due at ``now``; the caller checks once a minute. Interval jobs align to minutes since local midnight."""
    local_now = now.astimezone(ZoneInfo(timezone_name))
    minute_of_day = local_now.hour * 60 + local_now.minute
    due: list[StandingJob] = []
    for job in jobs:
        if job.cadence == "interval":
            if minute_of_day % job.interval_minutes == 0:
                due.append(job)
            continue
        if (local_now.hour, local_now.minute) != (job.fire_time.hour, job.fire_time.minute):
            continue
        if job.cadence == "weekly" and local_now.weekday() != job.weekday:
            continue
        due.append(job)
    return due
