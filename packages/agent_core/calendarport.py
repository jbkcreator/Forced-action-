"""Read-only calendar access and availability.

Read-only by construction: the port exposes ``events()`` and nothing else, so writing to a
calendar is structurally unavailable, not merely unused. The live adapter uses a service account
on the ``calendar.readonly`` scope.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CalendarEvent:
    title: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class FreeSlot:
    start: datetime
    end: datetime


class CalendarUnavailable(RuntimeError):
    """The live calendar could not be read; callers must not treat that as an empty day."""


class CalendarPort(Protocol):
    def events(self, time_min: datetime, time_max: datetime) -> list[CalendarEvent]: ...


class FakeCalendarPort:
    def __init__(self, events: list[CalendarEvent]) -> None:
        self._events = list(events)

    def events(self, time_min: datetime, time_max: datetime) -> list[CalendarEvent]:
        return [event for event in self._events if event.end > time_min and event.start < time_max]


class GoogleCalendarPort:
    SCOPES = ("https://www.googleapis.com/auth/calendar.readonly",)

    def __init__(self, service_account_key_path: str, calendar_id: str) -> None:
        self._key_path = service_account_key_path
        self._calendar_id = calendar_id

    def _service(self):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_file(self._key_path, scopes=list(self.SCOPES))
        return build("calendar", "v3", credentials=credentials, cache_discovery=False)

    @staticmethod
    def _parse(value: dict) -> datetime:
        if "dateTime" in value:
            return datetime.fromisoformat(value["dateTime"])
        return datetime.fromisoformat(value["date"]).replace(tzinfo=timezone.utc)

    def events(self, time_min: datetime, time_max: datetime) -> list[CalendarEvent]:
        try:
            response = self._service().events().list(
                calendarId=self._calendar_id,
                timeMin=time_min.astimezone(timezone.utc).isoformat(),
                timeMax=time_max.astimezone(timezone.utc).isoformat(),
                singleEvents=True, orderBy="startTime",
            ).execute()
        except Exception as exc:
            logger.error("google calendar read failed (%s)", type(exc).__name__)
            raise CalendarUnavailable("calendar could not be read") from exc
        events = []
        for item in response.get("items", []):
            start, end = item.get("start"), item.get("end")
            if start and end:
                events.append(CalendarEvent(item.get("summary", "(no title)"), self._parse(start), self._parse(end)))
        return events


def find_conflicts(events: list[CalendarEvent]) -> list[tuple[CalendarEvent, CalendarEvent]]:
    """Every overlapping pair. Sorted sweep: the inner loop stops at the first non-overlap."""
    ordered = sorted(events, key=lambda event: event.start)
    conflicts = []
    for index, first in enumerate(ordered):
        for second in ordered[index + 1:]:
            if second.start >= first.end:
                break
            conflicts.append((first, second))
    return conflicts


def free_slots(events: list[CalendarEvent], window_start: datetime, window_end: datetime,
               min_duration: timedelta = timedelta(minutes=30)) -> list[FreeSlot]:
    """Gaps of at least ``min_duration`` inside the window, after merging overlapping events."""
    slots: list[FreeSlot] = []
    cursor = window_start
    for event in sorted(events, key=lambda item: item.start):
        if event.end <= cursor or event.start >= window_end:
            continue
        if event.start - cursor >= min_duration:
            slots.append(FreeSlot(cursor, event.start))
        cursor = max(cursor, event.end)
    if window_end - cursor >= min_duration:
        slots.append(FreeSlot(cursor, window_end))
    return slots
