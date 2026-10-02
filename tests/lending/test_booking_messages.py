"""Tests for WP-GL-10 booking_messages: scheduling, rendering and cancellation.

All tests run without a real DB (mock_db / MagicMock).
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, call, patch

import pytest

from config.lending_reminders import (
    KIND_CONFIRMATION,
    KIND_NIGHT_BEFORE,
    KIND_NINETY_MIN,
    MAX_TEXT_CHARS,
)
from src.lending.booking_messages import (
    cancel_booking_messages,
    render_email,
    render_text,
    schedule_booking_messages,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _slot(hours_from_now: float = 48.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=hours_from_now)


def _booking(
    *,
    slot_hours: float = 48.0,
    address: Optional[str] = "412 Oak Ave, Tampa FL",
    text_consent: bool = True,
    booked_by: str = "caller@example.com",
    phone: Optional[str] = "+18135550001",
    email: Optional[str] = "borrower@example.com",
) -> dict:
    return {
        "booking_ref": "test-ref-123",
        "first_name": "Jane",
        "contact_phone": phone,
        "contact_email": email,
        "property_address": address,
        "slot_start_utc": _slot(slot_hours),
        "booked_by": booked_by,
        "text_consent": text_consent,
        "status": "confirmed",
        "ghl_contact_id": "ghl-cid-abc",
    }


def _db() -> MagicMock:
    db = MagicMock()
    # execute returns a result whose rowcount is the number of rows inserted
    db.execute.return_value.rowcount = 3
    return db


# ── Rendering ─────────────────────────────────────────────────────────────────

class TestRenderText:
    def test_confirmation_with_address(self):
        slot = _slot(48)
        body = render_text(KIND_CONFIRMATION, first_name="Jane",
                           slot_start_utc=slot, property_address="412 Oak Ave")
        assert "Next Deal Lending" in body
        assert "Josh" in body
        assert "412 Oak Ave" in body
        assert "Reply STOP to opt out" in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_confirmation_no_address_drops_phrase(self):
        slot = _slot(48)
        body = render_text(KIND_CONFIRMATION, first_name="Jane",
                           slot_start_utc=slot, property_address=None)
        assert "about" not in body.lower() or "about" not in body  # address phrase gone
        assert "Reply STOP to opt out" in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_night_before_with_address(self):
        slot = _slot(48)
        body = render_text(KIND_NIGHT_BEFORE, first_name="Bob",
                           slot_start_utc=slot, property_address="412 Oak Ave")
        assert "tomorrow" in body
        assert "412 Oak Ave" in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_night_before_no_address(self):
        slot = _slot(48)
        body = render_text(KIND_NIGHT_BEFORE, first_name="Bob",
                           slot_start_utc=slot, property_address=None)
        assert "tomorrow" in body
        assert "412 Oak" not in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_ninety_min_with_address(self):
        slot = _slot(48)
        body = render_text(KIND_NINETY_MIN, first_name="Alice",
                           slot_start_utc=slot, property_address="412 Oak Ave",
                           number="(813) 555-0001")
        assert "90 minutes" in body
        assert "412 Oak Ave" in body
        assert "(813) 555-0001" in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_ninety_min_no_address(self):
        slot = _slot(48)
        body = render_text(KIND_NINETY_MIN, first_name="Alice",
                           slot_start_utc=slot, property_address=None,
                           number="(813) 555-0001")
        assert "90 minutes" in body
        assert "412 Oak" not in body
        assert len(body) <= MAX_TEXT_CHARS

    def test_truncates_to_max_chars(self):
        """A very long address is still truncated to MAX_TEXT_CHARS."""
        slot = _slot(48)
        long_addr = "A" * 400
        body = render_text(KIND_CONFIRMATION, first_name="Jane",
                           slot_start_utc=slot, property_address=long_addr)
        assert len(body) == MAX_TEXT_CHARS

    def test_blank_first_name_falls_back(self):
        body = render_text(KIND_CONFIRMATION, first_name="",
                           slot_start_utc=_slot(48), property_address=None)
        assert "there" in body

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="unknown reminder kind"):
            render_text("bad_kind", first_name="X", slot_start_utc=_slot(48))


class TestRenderEmail:
    def test_confirmation_subject(self):
        subject, _ = render_email(KIND_CONFIRMATION, first_name="Jane",
                                   slot_start_utc=_slot(48), property_address="412 Oak")
        assert "confirmed" in subject.lower()

    def test_night_before_subject(self):
        subject, _ = render_email(KIND_NIGHT_BEFORE, first_name="Jane",
                                   slot_start_utc=_slot(48))
        assert "tomorrow" in subject.lower()

    def test_ninety_min_subject(self):
        subject, _ = render_email(KIND_NINETY_MIN, first_name="Jane",
                                   slot_start_utc=_slot(48))
        assert "90 minutes" in subject.lower()

    def test_no_stop_in_email(self):
        """Email bodies do not include STOP language (handled by unsubscribe links)."""
        _, body = render_email(KIND_CONFIRMATION, first_name="Jane",
                                slot_start_utc=_slot(48), property_address="412 Oak")
        assert "Reply STOP" not in body


# ── Scheduling ────────────────────────────────────────────────────────────────

_SETTINGS_PATH = "config.settings.AppSettings"


def _patch_text_enabled(enabled: bool):
    return patch("src.lending.booking_messages._channel",
                 side_effect=lambda *, text_consent: "text" if enabled and text_consent else "email")


class TestScheduleBookingMessages:
    def test_schedules_confirmation_and_two_reminders(self):
        """A 48-hour booking produces a confirmation, night_before and ninety_min row."""
        db = _db()
        db.execute.return_value.rowcount = 3
        with _patch_text_enabled(False):
            n = schedule_booking_messages(db, _booking(slot_hours=48))
        assert db.execute.call_count == 1
        rows = db.execute.call_args[0][1]
        assert len(rows) == 3
        kinds = {r["kind"] for r in rows}
        assert kinds == {KIND_CONFIRMATION, KIND_NIGHT_BEFORE, KIND_NINETY_MIN}

    def test_booking_in_90_min_skips_ninety_min_reminder(self):
        """A slot only 85 minutes away skips the 90-min reminder."""
        db = _db()
        db.execute.return_value.rowcount = 2
        with _patch_text_enabled(False):
            n = schedule_booking_messages(db, _booking(slot_hours=1.4))  # ~84 min
        rows = db.execute.call_args[0][1]
        kinds = {r["kind"] for r in rows}
        assert KIND_NINETY_MIN not in kinds

    def test_idempotency_on_conflict(self):
        """schedule_booking_messages uses ON CONFLICT DO NOTHING — 0 rows inserted on repeat."""
        db = _db()
        db.execute.return_value.rowcount = 0
        with _patch_text_enabled(False):
            n = schedule_booking_messages(db, _booking())
        assert n == 0

    def test_channel_is_email_when_no_consent(self):
        db = _db()
        with _patch_text_enabled(True):
            schedule_booking_messages(db, _booking(text_consent=False))
        rows = db.execute.call_args[0][1]
        assert all(r["channel"] == "email" for r in rows)

    def test_channel_is_email_when_text_not_enabled(self):
        db = _db()
        with _patch_text_enabled(False):
            schedule_booking_messages(db, _booking(text_consent=True))
        rows = db.execute.call_args[0][1]
        assert all(r["channel"] == "email" for r in rows)

    def test_channel_is_text_when_enabled_and_consented(self):
        db = _db()
        with _patch_text_enabled(True):
            schedule_booking_messages(db, _booking(text_consent=True))
        rows = db.execute.call_args[0][1]
        assert all(r["channel"] == "text" for r in rows)

    def test_address_stored_in_rows(self):
        db = _db()
        with _patch_text_enabled(False):
            schedule_booking_messages(db, _booking(address="412 Oak Ave"))
        rows = db.execute.call_args[0][1]
        assert all(r["property_address"] == "412 Oak Ave" for r in rows)

    def test_no_address_stored_as_none(self):
        db = _db()
        with _patch_text_enabled(False):
            schedule_booking_messages(db, _booking(address=None))
        rows = db.execute.call_args[0][1]
        assert all(r["property_address"] is None for r in rows)


# ── Cancellation ─────────────────────────────────────────────────────────────

class TestCancelBookingMessages:
    def test_cancels_pending_rows(self):
        db = MagicMock()
        db.execute.return_value.rowcount = 2
        n = cancel_booking_messages(db, "test-ref-123", "booking_cancelled")
        assert n == 2
        db.execute.assert_called_once()
        sql_str = str(db.execute.call_args[0][0])  # the text() object
        params = db.execute.call_args[0][1]
        assert params["booking_ref"] == "test-ref-123"
        assert params["reason"] == "booking_cancelled"

    def test_cancel_on_zero_pending_rows(self):
        db = MagicMock()
        db.execute.return_value.rowcount = 0
        n = cancel_booking_messages(db, "no-rows-ref", "booking_rescheduled")
        assert n == 0
