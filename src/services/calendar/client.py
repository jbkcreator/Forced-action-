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
