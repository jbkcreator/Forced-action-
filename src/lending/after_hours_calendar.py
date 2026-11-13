"""Night responder calendar: the open call slots on the morning calendar, and the booking of the one a lead picks.

:func:`calendar_hooks` returns the ``slot_finder`` and ``booker`` the responder takes
(``src/lending/after_hours_responder.py``), or none when ``LENDING_AFTER_HOURS_CALENDAR_ID`` is unset: the responder
then offers no slots. The calendar is a GHL calendar in the Next Deal Lending account, read and written through
``GHLCalendarClient``. It is deliberately not the booking page's calendar and does not go through
``src/services/calendar/booking.py``: that path books only after a caller-completed booking gate and offers nothing
inside 24 hours, which a night inquiry cannot meet.

A GHL appointment is created against a contact in that account, so live the booking first resolves the lead's GHL
contact: the id GHL gave on the lead's reply, else found or created by phone and email. A lead whose contact cannot
be resolved, or whose email address is suppressed, is refused and stays on the morning call list. The email is the
conversation's own, else the one on the lead's contact row; a lead with a phone only can still be booked. The slot
is read again just before it is booked, so one taken since the offer is reported as taken. A booking call that gets
no answer raises: it may exist, and the responder never repeats it. After a booking the lead's GHL card is moved to
Booked; a failed move never undoes the booking.

Logs carry ids only, never phones, names or addresses.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_after_hours_conversation import SLOT_DURATION_MINUTES, SLOT_TIMEZONE
from src.lending.after_hours_responder import (BOOKING_BOOKED, BOOKING_REFUSED, BOOKING_SLOT_TAKEN, Booker,
                                               BookingOutcome, BookingRequest, SlotFinder)
from src.services.calendar.availability import BusyBlock
from src.services.calendar.client import CalendarClient, CalendarOutcomeUnknown, CalendarUnavailable
from src.services.email_suppression import is_email_suppressed

logger = logging.getLogger(__name__)

APPOINTMENT_TITLE = "Next Deal Lending call: {name}"

# (request, email) -> the lead's GHL contact id, or None when it cannot be resolved.
ContactResolver = Callable[[BookingRequest, Optional[str]], Optional[str]]

_CONTACT_EMAIL = text("SELECT email FROM lending.contacts WHERE phone = :phone AND email IS NOT NULL")


def open_slots(busy: Sequence[BusyBlock], window_start: datetime, window_end: datetime) -> list[datetime]:
    """The slot starts in [window_start, window_end) that overlap no busy span, on the slot-length grid in the
    calendar's own timezone (9:00, 9:30, ...). Pure."""
    span = timedelta(minutes=SLOT_DURATION_MINUTES)
    local = window_start.astimezone(ZoneInfo(SLOT_TIMEZONE))
    past_grid = timedelta(minutes=local.minute % SLOT_DURATION_MINUTES, seconds=local.second,
                          microseconds=local.microsecond)
    start = window_start + (span - past_grid if past_grid else timedelta(0))
    slots = []
    while start + span <= window_end:
        if not any(block.start < start + span and block.end > start for block in busy):
            slots.append(start)
        start += span
    return slots


def _booking_email(db, request: BookingRequest) -> Optional[str]:
    if request.email:
        return request.email
    if not request.phone:
        return None
    return db.execute(_CONTACT_EMAIL, {"phone": request.phone}).scalar()


def ghl_contact_id(request: BookingRequest, email: Optional[str]) -> Optional[str]:
    """The lead's contact in the Next Deal Lending GHL account: the id already known, else found or created."""
    if request.ghl_contact_id:
        return request.ghl_contact_id
    from src.services.calendar.ghl_pipeline import _upsert_contact

    return _upsert_contact(phone=request.phone, email=email, first_name=request.first_name)


def _move_card_to_booked(request: BookingRequest, email: Optional[str], title: str) -> None:
    try:
        from src.services.calendar.ghl_pipeline import push_booking_to_booked_stage

        if not push_booking_to_booked_stage(phone=request.phone, email=email, first_name=request.first_name,
                                            opportunity_name=title):
            logger.warning("[after-hours-calendar] id=%s booked, but the GHL card was not moved to Booked",
                           request.conversation_id)
    except Exception as exc:  # class only: the booking stands whatever happens here
        logger.error("[after-hours-calendar] id=%s booked, but the GHL card move failed (%s)",
                     request.conversation_id, type(exc).__name__)


def calendar_hooks(db, *, client: Optional[CalendarClient] = None, calendar_id: Optional[str] = None,
                   contact_resolver: Optional[ContactResolver] = None,
                   ) -> tuple[Optional[SlotFinder], Optional[Booker]]:
    """The slot finder and booker for the configured morning calendar, or (None, None) when none is configured.
    ``client``, ``calendar_id`` and ``contact_resolver`` are injected in tests; live they are the GHL calendar
    client, the configured calendar and :func:`ghl_contact_id`. With no resolver the booking is made against the
    email address alone."""
    if calendar_id is None:
        from config.settings import get_settings

        calendar_id = get_settings().lending_after_hours_calendar_id
    if not calendar_id:
        return None, None
    if client is None:
        from src.services.calendar.ghl_client import GHLCalendarClient

        client = GHLCalendarClient.from_settings()
        contact_resolver = contact_resolver or ghl_contact_id

    def find_slots(window_start: datetime, window_end: datetime, now: datetime) -> list[datetime]:
        busy = client.get_busy(calendar_id=calendar_id, start=window_start, end=window_end)
        return open_slots(busy, window_start, window_end)

    def book(request: BookingRequest) -> BookingOutcome:
        email = _booking_email(db, request)
        if email and is_email_suppressed(db, email):
            logger.info("[after-hours-calendar] id=%s not booked: the email address is suppressed",
                        request.conversation_id)
            return BookingOutcome(BOOKING_REFUSED)
        contact_id = contact_resolver(request, email) if contact_resolver else None
        if not (contact_id or email):
            logger.info("[after-hours-calendar] id=%s not booked: no GHL contact and no email address",
                        request.conversation_id)
            return BookingOutcome(BOOKING_REFUSED)
        contact = {"contact_id": contact_id} if contact_id else {}
        start, end = request.slot_start, request.slot_start + timedelta(minutes=SLOT_DURATION_MINUTES)
        title = APPOINTMENT_TITLE.format(name=request.first_name or "night inquiry")
        try:
            if open_slots(client.get_busy(calendar_id=calendar_id, start=start, end=end), start, end) != [start]:
                return BookingOutcome(BOOKING_SLOT_TAKEN)
            event = client.create_event(calendar_id=calendar_id, start=start, end=end, summary=title,
                                        attendee_email=email or "", description="\n".join(request.answers),
                                        **contact)
        except CalendarOutcomeUnknown:
            raise
        except CalendarUnavailable as exc:  # the calendar answered no, or could not be read: nothing was booked
            logger.error("[after-hours-calendar] id=%s not booked: the calendar refused (%s)",
                         request.conversation_id, type(exc).__name__)
            return BookingOutcome(BOOKING_REFUSED)
        _move_card_to_booked(request, email, title)
        return BookingOutcome(BOOKING_BOOKED, event.event_id)

    return find_slots, book
