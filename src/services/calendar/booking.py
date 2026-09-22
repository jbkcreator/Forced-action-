"""The three scheduling operations agents call.

Reads and writes sit on opposite sides of the agent runtime by design.
get_slots() is a plain call: the agent deciding what to offer needs the answer
before it can build anything, and routing that through the work queue would
cost a poll interval per lookup. book() is the side-effecting half and is
registered as a tool so it inherits the audit log.

book() runs the suppression gate itself rather than relying on the send layer.
The calendar provider emails the invitation, so a booking reaches a borrower
without ever passing through the outbound path — the gate has to live here or
it does not run at all for this channel.

Business outcomes are returned; provider failures are raised. A suppressed
contact and an already-taken slot are answers the caller must act on, whereas
a provider rejection is an error and belongs in the agent loop's handler.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from config.calendar import DEFAULT_SLOT_DURATION_MINUTES
from src.services.calendar.availability import Slot, compute_free_slots
from src.services.calendar.client import CalendarClient, CalendarEvent

logger = logging.getLogger(__name__)

RESCHEDULE_ALTERNATIVE_LIMIT = 3


@dataclass(frozen=True)
class BookingResult:
    """Outcome of a booking attempt. `reason` is a stable machine-readable code."""

    booked: bool
    event: Optional[CalendarEvent] = None
    reason: Optional[str] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class RescheduleRequest:
    """A reschedule surfaced for a human, not an automated rebooking.

    v1 deliberately stops here: it gathers the current booking and some
    alternatives for the EXCEPTIONS queue and changes nothing. Autonomous
    negotiation is out of scope.
    """

    event_id: str
    current: Optional[CalendarEvent]
    alternatives: list[Slot]


def get_slots(
    *,
    client: CalendarClient,
    calendar_id: str,
    window_start: datetime,
    window_end: datetime,
    now: datetime,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
    include_weekends: bool = False,
) -> list[Slot]:
    """Bookable slots in the window, honouring every availability rule."""
    busy = client.get_busy(calendar_id=calendar_id, start=window_start, end=window_end)
    return compute_free_slots(
        busy=busy,
        window_start=window_start,
        window_end=window_end,
        now=now,
        duration_minutes=duration_minutes,
        include_weekends=include_weekends,
    )


def book(
    *,
    client: CalendarClient,
    session,
    calendar_id: str,
    slot: Slot,
    attendee_email: str,
    topic: str,
    description: str = "",
) -> BookingResult:
    """Book a slot for an attendee, refusing if suppressed or already taken."""
    from src.agents.fa_max.tool_registry import check_suppression

    suppression = check_suppression(
        recipient=attendee_email, channel="email", session=session
    )
    if suppression["suppressed"]:
        logger.info(
            "calendar.book: refused — recipient suppressed (reason=%s)",
            suppression["reason"],
        )
        return BookingResult(
            booked=False, reason="suppressed", detail=suppression["reason"]
        )

    if _is_taken(client=client, calendar_id=calendar_id, slot=slot):
        logger.info("calendar.book: refused — slot taken since it was offered")
        return BookingResult(booked=False, reason="slot_taken")

    event = client.create_event(
        calendar_id=calendar_id,
        start=slot.start,
        end=slot.end,
        summary=topic,
        attendee_email=attendee_email,
        description=description,
    )
    logger.info("calendar.book: booked event_id=%s", event.event_id)
    return BookingResult(booked=True, event=event)


def reschedule(
    *,
    client: CalendarClient,
    calendar_id: str,
    event_id: str,
    now: datetime,
    window_days: int = 7,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
) -> RescheduleRequest:
    """Collect a booking and some alternatives for a human to resolve."""
    current = client.get_event(calendar_id=calendar_id, event_id=event_id)

    window_start = now
    window_end = now + timedelta(days=window_days)
    alternatives = get_slots(
        client=client,
        calendar_id=calendar_id,
        window_start=window_start,
        window_end=window_end,
        now=now,
        duration_minutes=duration_minutes,
    )
    return RescheduleRequest(
        event_id=event_id,
        current=current,
        alternatives=alternatives[:RESCHEDULE_ALTERNATIVE_LIMIT],
    )


def _is_taken(*, client: CalendarClient, calendar_id: str, slot: Slot) -> bool:
    """Re-read the provider to catch a slot booked since it was offered.

    Availability is read once when an offer is built and the borrower may act
    on it minutes or days later. No calendar provider reserves a slot on read,
    so this narrows the race to the moment between this check and the write.
    """
    conflicts = client.get_busy(
        calendar_id=calendar_id, start=slot.start, end=slot.end
    )
    return any(
        block.start < slot.end and block.end > slot.start for block in conflicts
    )
