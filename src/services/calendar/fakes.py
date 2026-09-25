"""FakeCalendar — the WP-T2-7 deliverable that unblocks the other agents.

Same call shape as the real provider, zero network, zero credentials, every
call recorded so a test can assert on exactly what would have been booked.
Mirrors src/services/relay/fakes.py, including its central discipline: a fake
must never promise more than the real thing does.

Two consequences of that discipline are deliberate and load-bearing:

  * A created event immediately appears in get_busy(), because a real booking
    does. A test that books and then re-reads availability sees the slot gone.
  * Booking the same span twice succeeds both times. Google Calendar has no
    slot reservation and will happily double-book; a fake that raised here
    would let callers believe in a guarantee that does not exist, and the
    re-check that actually prevents this belongs above the client.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.services.calendar.availability import BusyBlock
from src.services.calendar.client import CalendarEvent


class FakeCalendarError(RuntimeError):
    """Raised for an attendee listed in `fail_attendees`."""


@dataclass
class FakeCalendar:
    """In-memory stand-in for a calendar provider.

    `busy` seeds pre-existing commitments. `fail_attendees` reproduces a
    provider rejecting a specific invitee so error paths are reachable
    without a live account.
    """

    busy: list[BusyBlock] = field(default_factory=list)
    fail_attendees: set[str] = field(default_factory=set)
    events: dict[str, CalendarEvent] = field(default_factory=dict)
    created: list[CalendarEvent] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    _sequence: int = 0

    def get_busy(
        self, *, calendar_id: str, start: datetime, end: datetime
    ) -> list[BusyBlock]:
        live = [
            BusyBlock(start=event.start, end=event.end)
            for event in self.events.values()
            if event.status == "confirmed"
        ]
        overlapping = [
            block for block in [*self.busy, *live] if block.start < end and block.end > start
        ]
        return sorted(overlapping, key=lambda block: block.start)

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
        if attendee_email in self.fail_attendees:
            raise FakeCalendarError(f"calendar provider rejected attendee {attendee_email!r}")

        self._sequence += 1
        event = CalendarEvent(
            event_id=f"fake-event-{self._sequence}",
            start=start,
            end=end,
            summary=summary,
            attendee_email=attendee_email,
            html_link=f"https://example.invalid/event/{self._sequence}",
            status="confirmed",
        )
        self.events[event.event_id] = event
        self.created.append(event)
        return event

    def get_event(self, *, calendar_id: str, event_id: str) -> Optional[CalendarEvent]:
        return self.events.get(event_id)

    def cancel_event(self, *, calendar_id: str, event_id: str) -> None:
        existing = self.events.get(event_id)
        if existing is None:
            return
        self.events[event_id] = CalendarEvent(
            event_id=existing.event_id,
            start=existing.start,
            end=existing.end,
            summary=existing.summary,
            attendee_email=existing.attendee_email,
            html_link=existing.html_link,
            status="cancelled",
        )
        self.cancelled.append(event_id)
