"""Availability rules for the Forced Action scheduling tool.

Hours here are wall-clock in CALENDAR_TIMEZONE, never UTC. Business hours must
stay at 9am local on both sides of a daylight-saving transition, and a fixed
UTC offset cannot express that.
"""
from __future__ import annotations

CALENDAR_TIMEZONE = "America/New_York"

BUSINESS_HOURS_START_HOUR = 9
BUSINESS_HOURS_END_HOUR = 17

# A booking made inside this window gives the client no warning of a meeting
# that is already on his day.
MINIMUM_NOTICE_HOURS = 24

# Upper bound on how far ahead a borrower may book. Without it the spec's
# 24-hour floor leaves the ceiling open and slots months out are offerable.
BOOKING_HORIZON_DAYS = 21

# Applied to BOTH edges of every busy block. Padding only the trailing edge
# still permits a new booking that ends exactly as an existing meeting begins,
# which is the back-to-back case this buffer exists to prevent.
INTER_MEETING_BUFFER_MINUTES = 15

SUPPORTED_SLOT_DURATIONS_MINUTES = (30, 60)
DEFAULT_SLOT_DURATION_MINUTES = 30

# Slot starts snap to this grid so an offer reads "2:00"/"2:30" instead of an
# arbitrary offset inherited from when the previous meeting happened to end.
SLOT_ALIGNMENT_MINUTES = 30

# datetime.weekday(): Monday is 0, so Saturday and Sunday are 5 and 6.
WEEKEND_WEEKDAYS = frozenset({5, 6})

# Which venture's EXCEPTIONS lane a reschedule request is surfaced on.
CALENDAR_VENTURE_KEY = "fa_max_lending"

RESCHEDULE_ALERT_RULE = "calendar_reschedule_requested"


def validate_calendar_config() -> None:
    """Raise ValueError if the configured rules cannot produce bookable slots."""
    if not 0 <= BUSINESS_HOURS_START_HOUR < BUSINESS_HOURS_END_HOUR <= 24:
        raise ValueError(
            "BUSINESS_HOURS_START_HOUR must be before BUSINESS_HOURS_END_HOUR "
            f"and both within 0-24 (got {BUSINESS_HOURS_START_HOUR}, {BUSINESS_HOURS_END_HOUR})"
        )

    business_day_minutes = (BUSINESS_HOURS_END_HOUR - BUSINESS_HOURS_START_HOUR) * 60
    longest_slot = max(SUPPORTED_SLOT_DURATIONS_MINUTES)
    if longest_slot > business_day_minutes:
        raise ValueError(
            f"Longest supported slot ({longest_slot}m) does not fit in a "
            f"business day ({business_day_minutes}m)"
        )

    if DEFAULT_SLOT_DURATION_MINUTES not in SUPPORTED_SLOT_DURATIONS_MINUTES:
        raise ValueError(
            f"DEFAULT_SLOT_DURATION_MINUTES ({DEFAULT_SLOT_DURATION_MINUTES}) is not "
            f"one of SUPPORTED_SLOT_DURATIONS_MINUTES {SUPPORTED_SLOT_DURATIONS_MINUTES}"
        )

    if SLOT_ALIGNMENT_MINUTES <= 0:
        raise ValueError("SLOT_ALIGNMENT_MINUTES must be positive")

    if MINIMUM_NOTICE_HOURS < 0 or INTER_MEETING_BUFFER_MINUTES < 0:
        raise ValueError("Notice and buffer values must not be negative")

    if BOOKING_HORIZON_DAYS * 24 <= MINIMUM_NOTICE_HOURS:
        raise ValueError(
            f"BOOKING_HORIZON_DAYS ({BOOKING_HORIZON_DAYS}d) does not extend past "
            f"MINIMUM_NOTICE_HOURS ({MINIMUM_NOTICE_HOURS}h) — no slot could ever qualify"
        )
