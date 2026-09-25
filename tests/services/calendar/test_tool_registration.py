"""The calendar tools as the agent runtime sees them.

A steps payload is JSON, so these cover the conversion the runtime depends on:
ISO strings in, JSON-safe values out, and the client resolved internally
because it cannot travel through a work item.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.agents.fa_max.tool_registry import (
    FA_MAX_TOOL_REGISTRY,
    get_fa_max_tool,
    select_task_tools,
)

CALENDAR_ID = "client@example.invalid"


def _window() -> tuple[str, str]:
    """A window far enough out to clear the minimum-notice rule."""
    start = datetime.now(timezone.utc) + timedelta(days=2)
    # Four days always spans a weekday, whatever day the suite runs on.
    return start.isoformat(), (start + timedelta(days=4)).isoformat()


class TestRegistration:
    @pytest.mark.parametrize(
        "name", ["calendar.get_slots", "calendar.book", "calendar.reschedule"]
    )
    def test_tool_is_registered_under_its_published_name(self, name):
        assert name in FA_MAX_TOOL_REGISTRY

    def test_booking_is_a_gated_write_and_reading_is_not(self):
        book_spec = get_fa_max_tool("calendar.book")
        slots_spec = get_fa_max_tool("calendar.get_slots")

        assert book_spec.category == "write"
        assert book_spec.requires_send_gate, (
            "booking emails the borrower via the provider — it is an outbound"
        )
        assert not book_spec.idempotent, "a second call books a second meeting"

        assert slots_spec.category == "read"
        assert slots_spec.idempotent
        assert not slots_spec.requires_send_gate

    def test_every_tool_carries_a_description(self):
        for name in ("calendar.get_slots", "calendar.book", "calendar.reschedule"):
            assert get_fa_max_tool(name).description


class TestTaskRouting:
    def test_availability_request_routes_to_get_slots(self):
        window_start, window_end = _window()
        steps = select_task_tools(
            "find open slots",
            {"window_start": window_start, "window_end": window_end},
        )
        assert steps == [{
            "tool": "calendar.get_slots",
            "args": {"window_start": window_start, "window_end": window_end},
        }]

    def test_booking_request_routes_to_book(self):
        context = {
            "starts_at": "2026-06-16T14:00:00+00:00",
            "ends_at": "2026-06-16T14:30:00+00:00",
            "attendee_email": "borrower@example.invalid",
            "topic": "Intro call",
        }
        steps = select_task_tools("book the call", context)
        assert steps[0]["tool"] == "calendar.book"

    def test_reschedule_wins_over_book_when_both_words_appear(self):
        steps = select_task_tools(
            "reschedule the booking", {"booking_ref": "abc123"}
        )
        assert steps[0]["tool"] == "calendar.reschedule", (
            "'reschedule the booking' contains 'book'; only the reschedule "
            "reading of it is correct"
        )

    def test_missing_required_context_is_rejected(self):
        with pytest.raises(ValueError, match="task_context_missing"):
            select_task_tools("book the call", {"topic": "Intro call"})

    def test_existing_routes_are_unaffected(self):
        steps = select_task_tools(
            "check suppression", {"recipient": "a@example.invalid", "channel": "email"}
        )
        assert steps[0]["tool"] == "check_suppression"


class TestWrapperConversion:
    def test_get_slots_returns_json_safe_iso_strings(self):
        window_start, window_end = _window()
        result = get_fa_max_tool("calendar.get_slots").func(
            window_start=window_start, window_end=window_end, calendar_id=CALENDAR_ID,
        )
        assert result["slots"]
        first = result["slots"][0]
        assert isinstance(first["start"], str)
        datetime.fromisoformat(first["start"])

    def test_book_wrapper_reports_a_refusal_rather_than_raising(self):
        window_start, _ = _window()
        start = datetime.fromisoformat(window_start).replace(
            hour=14, minute=0, second=0, microsecond=0
        )
        with patch(
            "src.agents.fa_max.tool_registry.check_suppression",
            return_value={"suppressed": True, "reason": "opted_out"},
        ):
            result = get_fa_max_tool("calendar.book").func(
                starts_at=start.isoformat(),
                ends_at=(start + timedelta(minutes=30)).isoformat(),
                attendee_email="borrower@example.invalid",
                topic="Intro call",
                calendar_id=CALENDAR_ID,
                session=None,
            )
        assert result["booked"] is False
        assert result["reason"] == "suppressed"
        assert result["booking_ref"] is None

    def test_live_mode_builds_the_google_client_not_the_fake(self):
        from src.services.calendar.client import get_calendar_client
        from src.services.calendar.fakes import FakeCalendar

        with patch("config.settings.get_settings") as settings, patch(
            "src.services.calendar.google_client.GoogleCalendarClient.from_settings"
        ) as from_settings:
            settings.return_value.fa_max_calendar_mode = "live"
            client = get_calendar_client()

        from_settings.assert_called_once()
        assert not isinstance(client, FakeCalendar)

    def test_unknown_mode_raises_rather_than_guessing(self):
        from src.services.calendar.client import get_calendar_client

        with patch("config.settings.get_settings") as settings:
            settings.return_value.fa_max_calendar_mode = "stage"
            with pytest.raises(ValueError, match="Unknown FA_MAX_CALENDAR_MODE"):
                get_calendar_client()
