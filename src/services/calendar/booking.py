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
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text

from config.calendar import (
    CALENDAR_TIMEZONE,
    CALENDAR_VENTURE_KEY,
    DEFAULT_SLOT_DURATION_MINUTES,
    RESCHEDULE_ALERT_RULE,
)
from src.services.calendar.availability import Slot, compute_free_slots
from src.services.calendar.client import CalendarClient, CalendarEvent

logger = logging.getLogger(__name__)

RESCHEDULE_ALTERNATIVE_LIMIT = 3

_TZ = ZoneInfo(CALENDAR_TIMEZONE)


@dataclass(frozen=True)
class BookingResult:
    """Outcome of a booking attempt. `reason` is a stable machine-readable code."""

    booked: bool
    event: Optional[CalendarEvent] = None
    booking_ref: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class RescheduleRequest:
    """A reschedule surfaced for a human, not an automated rebooking.

    v1 deliberately stops short of moving anything: it gathers the booking and
    some alternatives, pages EXCEPTIONS, and marks the row so the booking is
    visibly in question. Autonomous negotiation is out of scope.
    """

    booking_ref: str
    current: Optional[CalendarEvent]
    alternatives: list[Slot]
    alerted: bool


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
    person_id: Optional[Any] = None,
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

    booking_ref = _record_booking(
        session=session,
        calendar_id=calendar_id,
        event=event,
        attendee_email=attendee_email,
        topic=topic,
        person_id=person_id,
    )
    logger.info(
        "calendar.book: booked booking_ref=%s event_id=%s", booking_ref, event.event_id
    )
    return BookingResult(booked=True, event=event, booking_ref=booking_ref)


def _record_booking(
    *,
    session,
    calendar_id: str,
    event: CalendarEvent,
    attendee_email: str,
    topic: str,
    person_id: Optional[Any],
) -> str:
    """Persist the booking and return its stable reference."""
    booking_ref = secrets.token_urlsafe(9)
    session.execute(
        sa_text(
            """
            INSERT INTO fa_max_bookings
                (booking_ref, calendar_id, provider_event_id, person_id,
                 attendee_email, topic, starts_at, ends_at, status)
            VALUES
                (:booking_ref, :calendar_id, :provider_event_id, :person_id,
                 :attendee_email, :topic, :starts_at, :ends_at, 'confirmed')
            """
        ),
        {
            "booking_ref": booking_ref,
            "calendar_id": calendar_id,
            "provider_event_id": event.event_id,
            "person_id": person_id,
            "attendee_email": attendee_email,
            "topic": topic,
            "starts_at": event.start,
            "ends_at": event.end,
        },
    )
    return booking_ref


def reschedule(
    *,
    client: CalendarClient,
    session,
    calendar_id: str,
    booking_ref: str,
    now: datetime,
    window_days: int = 7,
    duration_minutes: int = DEFAULT_SLOT_DURATION_MINUTES,
) -> RescheduleRequest:
    """Page a human with the booking and some alternatives. Moves nothing."""
    booking = session.execute(
        sa_text(
            """
            SELECT provider_event_id, attendee_email, topic, starts_at
            FROM fa_max_bookings
            WHERE booking_ref = :booking_ref
            """
        ),
        {"booking_ref": booking_ref},
    ).mappings().first()
    if booking is None:
        raise ValueError(f"unknown booking_ref {booking_ref!r}")

    current = None
    if booking["provider_event_id"]:
        current = client.get_event(
            calendar_id=calendar_id, event_id=booking["provider_event_id"]
        )

    alternatives = get_slots(
        client=client,
        calendar_id=calendar_id,
        window_start=now,
        window_end=now + timedelta(days=window_days),
        now=now,
        duration_minutes=duration_minutes,
    )[:RESCHEDULE_ALTERNATIVE_LIMIT]

    session.execute(
        sa_text(
            """
            UPDATE fa_max_bookings
            SET status = 'reschedule_requested', updated_at = NOW()
            WHERE booking_ref = :booking_ref
            """
        ),
        {"booking_ref": booking_ref},
    )

    alerted = _alert_exceptions(
        booking_ref=booking_ref,
        attendee_email=booking["attendee_email"],
        topic=booking["topic"],
        starts_at=booking["starts_at"],
        alternatives=alternatives,
    )
    return RescheduleRequest(
        booking_ref=booking_ref,
        current=current,
        alternatives=alternatives,
        alerted=alerted,
    )


def _alert_exceptions(
    *,
    booking_ref: str,
    attendee_email: str,
    topic: str,
    starts_at: datetime,
    alternatives: list[Slot],
) -> bool:
    """Surface the request on the EXCEPTIONS lane. Never raises.

    A failed page must not roll back the status change: the booking really is
    in question either way, and the alert queue retries on its own.
    """
    from src.services.relay import exceptions_alert_queue

    offered = (
        ", ".join(_human_time(slot.start) for slot in alternatives)
        if alternatives
        else "none available in the next week"
    )
    message = (
        f"*Reschedule requested* — {topic}\n"
        f"Booking: `{booking_ref}`\n"
        f"Attendee: {attendee_email}\n"
        f"Currently: {_human_time(starts_at)}\n"
        f"Alternatives: {offered}"
    )
    try:
        return exceptions_alert_queue.enqueue_and_attempt(
            venture_key=CALENDAR_VENTURE_KEY,
            rule=RESCHEDULE_ALERT_RULE,
            message=message,
        )
    except Exception:
        logger.exception(
            "calendar.reschedule: EXCEPTIONS alert failed for booking_ref=%s", booking_ref
        )
        return False


def _human_time(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(_TZ).strftime("%a %d %b %H:%M %Z")


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
