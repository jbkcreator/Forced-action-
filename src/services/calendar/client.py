"""The seam between scheduling logic and whoever actually holds the calendar.

Only the four operations the scheduling tool needs are declared. Availability
is read through free/busy rather than by listing events: knowing the client is
committed at 2pm requires no access to who he is meeting or what about, and
not requesting that detail keeps his calendar contents out of this system.

The live Google implementation arrives with the credential work; until then
FakeCalendar in fakes.py is the only implementation, so nothing here can reach
a real calendar.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol, runtime_checkable

from src.services.calendar.availability import BusyBlock


@dataclass(frozen=True)
class CalendarEvent:
    """A booking as the calendar provider reports it back."""

    event_id: str
    start: datetime
    end: datetime
    summary: str
    attendee_email: Optional[str] = None
    html_link: Optional[str] = None
    status: str = "confirmed"


@runtime_checkable
class CalendarClient(Protocol):
    """What the scheduling tool requires of a calendar provider."""

    def get_busy(
        self, *, calendar_id: str, start: datetime, end: datetime
    ) -> list[BusyBlock]:
        """Committed spans overlapping [start, end). No event detail."""
        ...

    def create_event(
        self,
        *,
        calendar_id: str,
        start: datetime,
        end: datetime,
        summary: str,
        attendee_email: str,
        description: str = "",
    ) -> CalendarEvent:
        """Create a booking and invite the attendee."""
        ...

    def get_event(self, *, calendar_id: str, event_id: str) -> Optional[CalendarEvent]:
        """Fetch one booking, or None if it does not exist."""
        ...

    def cancel_event(self, *, calendar_id: str, event_id: str) -> None:
        """Cancel a booking. Succeeds silently if already cancelled."""
        ...


_fake_client = None


def get_calendar_client() -> CalendarClient:
    """Resolve the configured client.

    "live" is never the default, so an environment that has not been
    configured books into memory rather than onto the client's real day. A
    mode of "live" raises rather than falling back, because silently faking a
    booking someone believes is real is the worse failure.

    The fake is a module singleton: a task that reads availability and then
    books must see its own booking, which a fresh instance per call would
    discard.
    """
    from config.settings import get_settings

    mode = get_settings().fa_max_calendar_mode
    if mode == "live":
        raise NotImplementedError(
            "FA_MAX_CALENDAR_MODE=live but no live calendar client exists yet"
        )
    if mode != "fake":
        raise ValueError(f"Unknown FA_MAX_CALENDAR_MODE {mode!r} — expected 'fake' or 'live'")

    from src.services.calendar.fakes import FakeCalendar

    global _fake_client
    if _fake_client is None:
        _fake_client = FakeCalendar()
    return _fake_client


def reset_calendar_client() -> None:
    """Drop the cached fake so one test's bookings cannot leak into another."""
    global _fake_client
    _fake_client = None


def get_calendar_id() -> str:
    """The calendar bookings are written to."""
    from config.settings import get_settings

    calendar_id = get_settings().fa_max_calendar_id
    if not calendar_id:
        raise ValueError("FA_MAX_CALENDAR_ID is not set")
    return calendar_id
