from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import pytest

from packages.agent_core.briefing import BriefSection, render_brief_blocks
from packages.agent_core.calendarport import CalendarEvent, FakeCalendarPort, find_conflicts, free_slots
from packages.agent_core.scheduler import StandingJob, due_jobs

ET_OFFSET = timedelta(hours=-4)  # October: Eastern Daylight Time


def _et(hour: int, minute: int, day: int = 9) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=timezone(ET_OFFSET))


JOBS = [
    StandingJob("morning_brief", "daily", fire_time=time(7, 30)),
    StandingJob("friday_review", "weekly", fire_time=time(17, 0), weekday=4),
    StandingJob("relay_sweep", "interval", interval_minutes=5),
]


def test_daily_and_interval_jobs_fire_on_their_minute() -> None:
    assert {job.name for job in due_jobs(_et(7, 30), JOBS)} == {"morning_brief", "relay_sweep"}
    assert [job.name for job in due_jobs(_et(7, 31), JOBS)] == []


def test_weekly_job_fires_only_on_its_weekday() -> None:
    assert "friday_review" in {job.name for job in due_jobs(_et(17, 0, day=9), JOBS)}  # Friday
    assert "friday_review" not in {job.name for job in due_jobs(_et(17, 0, day=8), JOBS)}


def test_utc_input_is_converted_to_the_job_timezone() -> None:
    assert "morning_brief" in {job.name for job in due_jobs(datetime(2026, 10, 9, 11, 30, tzinfo=timezone.utc), JOBS)}


@pytest.mark.parametrize("bad", [dict(cadence="daily"), dict(cadence="weekly", fire_time=time(9, 0)),
                                 dict(cadence="interval", interval_minutes=0)])
def test_misconfigured_jobs_are_rejected(bad: dict) -> None:
    with pytest.raises(ValueError):
        StandingJob("bad", **bad)


def _event(title: str, start_hour: float, end_hour: float) -> CalendarEvent:
    base = _et(0, 0)
    return CalendarEvent(title, base + timedelta(hours=start_hour), base + timedelta(hours=end_hour))


def test_conflicts_and_free_slots() -> None:
    events = [_event("call A", 9, 10), _event("call B", 9.5, 10.5), _event("lunch", 12, 13)]
    assert [(a.title, b.title) for a, b in find_conflicts(events)] == [("call A", "call B")]
    slots = free_slots(events, _et(9, 0), _et(14, 0), min_duration=timedelta(minutes=60))
    assert [(slot.start.hour, slot.end.hour) for slot in slots] == [(10, 12), (13, 14)]


def test_fake_calendar_returns_only_overlapping_events() -> None:
    port = FakeCalendarPort([_event("early", 6, 7), _event("in window", 9, 10)])
    assert [event.title for event in port.events(_et(8, 0), _et(12, 0))] == ["in window"]


def test_brief_renders_sections_with_empty_placeholder() -> None:
    blocks = render_brief_blocks("Cora brief", [BriefSection("Leads", ["3 new"]), BriefSection("Alarms")])
    assert blocks[0]["text"]["text"] == "Cora brief"
    texts = [block["text"]["text"] for block in blocks if block["type"] == "section"]
    assert texts == ["*Leads*\n3 new", "*Alarms*\n_Nothing to report._"]
