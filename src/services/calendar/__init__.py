"""Calendar scheduling for Forced Action agents.

The public surface every consumer should import from. Agents call get_slots()
directly when deciding what to offer; book() and reschedule() are the
side-effecting half and run through the agent runtime.
"""
from src.services.calendar.availability import BusyBlock, Slot, compute_free_slots
from src.services.calendar.booking import (
    BookingResult,
    RescheduleRequest,
    book,
    get_slots,
    reschedule,
)
from src.services.calendar.client import (
    CalendarClient,
    CalendarEvent,
    get_calendar_client,
    get_calendar_id,
    reset_calendar_client,
)
from src.services.calendar.fakes import FakeCalendar, FakeCalendarError

__all__ = [
    "BookingResult",
    "BusyBlock",
    "CalendarClient",
    "CalendarEvent",
    "FakeCalendar",
    "FakeCalendarError",
    "RescheduleRequest",
    "Slot",
    "book",
    "compute_free_slots",
    "get_calendar_client",
    "get_calendar_id",
    "get_slots",
    "reschedule",
    "reset_calendar_client",
]
