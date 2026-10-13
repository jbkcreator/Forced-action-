"""T-09: ``text_window_open`` - 8:00-20:00 for the borrower in EVERY timezone we can derive, else no text.

October dates, so Eastern is UTC-4, Central UTC-5, Mountain UTC-6, Pacific UTC-7.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.lending.compliance import text_window_open

FL_ET_PHONE = "+17275550100"   # 727: Eastern
GA_ET_PHONE = "+14045550100"   # 404: Eastern
CA_PT_PHONE = "+16502530000"   # 650: Pacific (a valid number: the zone comes from libphonenumber)


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 14, hour, minute, tzinfo=timezone.utc)


@pytest.mark.parametrize("now, expected", [
    (_utc(11, 59), False),  # 7:59 ET
    (_utc(12, 0), True),    # 8:00 ET: the window opens
    (_utc(23, 59), True),   # 19:59 ET
    (_utc(0, 0), False),    # 20:00 ET: closed (evening of the 13th)
])
def test_single_zone_state_is_8_to_8_local(now, expected):
    assert text_window_open(GA_ET_PHONE, property_state="GA", now=now) is expected


@pytest.mark.parametrize("now, expected", [
    (_utc(12, 0), False),   # 8:00 ET but 7:00 CT: the Panhandle half of Florida is not open yet
    (_utc(13, 0), True),    # 9:00 ET = 8:00 CT: both zones open
    (_utc(23, 59), True),   # 19:59 ET
    (_utc(0, 0), False),    # 20:00 ET
])
def test_a_state_that_spans_zones_needs_every_zone_open(now, expected):
    assert text_window_open(FL_ET_PHONE, property_state="FL", now=now) is expected


def test_phone_and_property_zones_must_both_be_open():
    # Pacific number, Florida property: 8:00 PT is 11:00 ET, so 10:59 ET is still too early for the Pacific zone.
    assert text_window_open(CA_PT_PHONE, property_state="FL", now=_utc(14, 59)) is False
    assert text_window_open(CA_PT_PHONE, property_state="FL", now=_utc(15, 0)) is True
    assert text_window_open(CA_PT_PHONE, property_state="FL", now=_utc(23, 59)) is True  # 19:59 ET


def test_property_state_alone_is_enough_when_the_phone_has_no_zone():
    assert text_window_open("not-a-phone", property_state="NY", now=_utc(16)) is True
    assert text_window_open("not-a-phone", property_state="NY", now=_utc(10)) is False


def test_state_code_is_normalised():
    assert text_window_open("not-a-phone", property_state=" ga ", now=_utc(16)) is True


def test_no_derivable_timezone_means_no_text():
    assert text_window_open("not-a-phone", now=_utc(16)) is False
    assert text_window_open("not-a-phone", property_state="ZZ", now=_utc(16)) is False
    assert text_window_open("not-a-phone", property_state=None, now=_utc(16)) is False


def test_defaults_to_the_real_clock():
    assert isinstance(text_window_open(GA_ET_PHONE, property_state="GA"), bool)


# ------------------------------------------------------------------ PR #360 review: the property ZIP must not hide the phone's zone

TAMPA_ZIP = "33602"  # Hillsborough: recipient_timezone() alone would answer Eastern and ignore the phone


def test_a_tampa_zip_does_not_override_a_pacific_phone():
    # 9:00 ET is 6:00 PT: the Pacific borrower must not be texted just because the property is in Tampa.
    assert text_window_open(CA_PT_PHONE, property_state="FL", property_zip=TAMPA_ZIP, now=_utc(13, 0)) is False
    assert text_window_open(CA_PT_PHONE, property_state="FL", property_zip=TAMPA_ZIP, now=_utc(14, 59)) is False
    assert text_window_open(CA_PT_PHONE, property_state="FL", property_zip=TAMPA_ZIP, now=_utc(15, 0)) is True  # 8:00 PT


def test_a_tampa_zip_alone_is_still_eastern():
    assert text_window_open("not-a-phone", property_zip=TAMPA_ZIP, now=_utc(12, 0)) is True
    assert text_window_open("not-a-phone", property_zip=TAMPA_ZIP, now=_utc(11, 59)) is False


# ------------------------------------------------------------------ PR #360 review: the hold must outlast the longest closed stretch

def _longest_closed_gap_hours(zones, day) -> float:
    """Longest run of 5-minute steps, in a UTC day plus the next, for which no text may go out in ``zones``."""
    from datetime import timedelta

    from src.lending.compliance import _zones_inside_window

    start = datetime(*day, tzinfo=timezone.utc)
    longest = run = 0
    for step in range(2 * 24 * 12):
        if _zones_inside_window(zones, start + timedelta(minutes=5 * step)):
            run = 0
        else:
            run += 1
            longest = max(longest, run)
    return longest * 5 / 60


def test_the_hold_cap_outlasts_the_longest_closed_stretch_of_any_supported_zone_combination():
    """A lead that arrives as the window closes must still be inside INTRO_SMS_HOLD_MAX_HOURS when it next opens,
    for every phone zone x property state we support, in both daylight and standard time."""
    from config.lending_compliance import STATE_TZ
    from config.lending_lendingflow import INTRO_SMS_HOLD_MAX_HOURS

    phone_zones = ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles", "America/Phoenix",
                   "America/Anchorage", "Pacific/Honolulu"]
    worst = 0.0
    for state_zones in set(STATE_TZ.values()):
        for phone_zone in phone_zones:
            zones = set(state_zones) | {phone_zone}
            for day in ((2026, 10, 14), (2026, 1, 14)):
                worst = max(worst, _longest_closed_gap_hours(zones, day))
    sweep_delay_hours = 5 / 60
    assert worst + sweep_delay_hours < INTRO_SMS_HOLD_MAX_HOURS, f"closed for {worst} h, cap {INTRO_SMS_HOLD_MAX_HOURS} h"
