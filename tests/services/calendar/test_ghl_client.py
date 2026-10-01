"""GHLCalendarClient against a stubbed requests.request.

Payloads mirror the documented v2 shapes (see ghl_client.py's module
docstring for the confirmed-vs-best-effort caveat) so the request/response
shaping is exercised without credentials and without writing to a real
GHL calendar.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services.calendar.client import CalendarUnavailable
from src.services.calendar.ghl_client import GHLCalendarClient

CALENDAR_ID = "cal_abc123"
LOCATION_ID = "loc_xyz789"

START = datetime(2026, 6, 15, 12, tzinfo=timezone.utc)
END = START + timedelta(days=2)


def _client() -> GHLCalendarClient:
    return GHLCalendarClient(api_key="test-key", location_id=LOCATION_ID)


def _response(status_code=200, json_body=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


class TestGetBusy:
    def test_parses_events_as_busy_spans(self):
        client = _client()
        payload = {
            "events": [
                {"startTime": "2026-06-15T14:00:00+00:00", "endTime": "2026-06-15T14:30:00+00:00"},
                {"startTime": 1781000000000, "endTime": 1781001800000},
            ]
        }
        with patch("requests.request", return_value=_response(json_body=payload)) as mock_req:
            busy = client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

        assert len(busy) == 2
        assert busy[0].start.tzinfo is not None
        called_kwargs = mock_req.call_args.kwargs
        assert called_kwargs["params"]["calendarId"] == CALENDAR_ID
        assert called_kwargs["params"]["locationId"] == LOCATION_ID

    def test_cancelled_events_are_excluded_from_busy(self):
        client = _client()
        payload = {
            "events": [
                {
                    "startTime": "2026-06-15T14:00:00+00:00",
                    "endTime": "2026-06-15T14:30:00+00:00",
                    "appointmentStatus": "cancelled",
                },
            ]
        }
        with patch("requests.request", return_value=_response(json_body=payload)):
            busy = client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)
        assert busy == []

    def test_empty_calendar_returns_no_spans(self):
        client = _client()
        with patch("requests.request", return_value=_response(json_body={"events": []})):
            busy = client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)
        assert busy == []

    def test_http_error_raises_calendar_unavailable_not_empty(self):
        """A failed read must never be silently reported as a free calendar."""
        client = _client()
        with patch("requests.request", return_value=_response(status_code=500, text="boom")):
            with pytest.raises(CalendarUnavailable):
                client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

    def test_naive_datetime_is_rejected_before_the_call(self):
        client = _client()
        with pytest.raises(ValueError):
            client.get_busy(
                calendar_id=CALENDAR_ID,
                start=datetime(2026, 6, 15, 12),
                end=datetime(2026, 6, 16, 12),
            )


class TestCreateEvent:
    def test_returns_the_created_event(self):
        client = _client()
        payload = {
            "appointment": {
                "id": "appt_1",
                "title": "Intro call",
                "startTime": "2026-06-15T14:00:00+00:00",
                "endTime": "2026-06-15T14:30:00+00:00",
                "contactEmail": "borrower@example.invalid",
                "appointmentStatus": "confirmed",
            }
        }
        with patch("requests.request", return_value=_response(status_code=201, json_body=payload)) as mock_req:
            event = client.create_event(
                calendar_id=CALENDAR_ID,
                start=START,
                end=START + timedelta(minutes=30),
                summary="Intro call",
                attendee_email="borrower@example.invalid",
            )

        assert event.event_id == "appt_1"
        assert event.attendee_email == "borrower@example.invalid"
        assert event.status == "confirmed"
        sent_json = mock_req.call_args.kwargs["json"]
        assert sent_json["locationId"] == LOCATION_ID
        assert sent_json["calendarId"] == CALENDAR_ID

    def test_failed_create_raises(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=422, text="bad request")):
            with pytest.raises(CalendarUnavailable):
                client.create_event(
                    calendar_id=CALENDAR_ID,
                    start=START,
                    end=START + timedelta(minutes=30),
                    summary="Intro call",
                    attendee_email="borrower@example.invalid",
                )


class TestGetEvent:
    def test_returns_the_event(self):
        client = _client()
        payload = {
            "appointment": {
                "id": "appt_1",
                "title": "Intro call",
                "startTime": "2026-06-15T14:00:00+00:00",
                "endTime": "2026-06-15T14:30:00+00:00",
                "appointmentStatus": "confirmed",
            }
        }
        with patch("requests.request", return_value=_response(json_body=payload)):
            event = client.get_event(calendar_id=CALENDAR_ID, event_id="appt_1")
        assert event is not None
        assert event.event_id == "appt_1"

    def test_missing_event_is_none_not_an_error(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=404)):
            event = client.get_event(calendar_id=CALENDAR_ID, event_id="gone")
        assert event is None

    def test_other_failures_propagate(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=500, text="boom")):
            with pytest.raises(CalendarUnavailable):
                client.get_event(calendar_id=CALENDAR_ID, event_id="appt_1")


class TestCancelEvent:
    def test_cancels_via_status_update(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=200)) as mock_req:
            client.cancel_event(calendar_id=CALENDAR_ID, event_id="appt_1")
        sent_json = mock_req.call_args.kwargs["json"]
        assert sent_json["appointmentStatus"] == "cancelled"

    def test_already_gone_is_silent(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=404)):
            client.cancel_event(calendar_id=CALENDAR_ID, event_id="gone")  # no raise

    def test_other_failures_propagate(self):
        client = _client()
        with patch("requests.request", return_value=_response(status_code=500, text="boom")):
            with pytest.raises(CalendarUnavailable):
                client.cancel_event(calendar_id=CALENDAR_ID, event_id="appt_1")


class TestFromSettings:
    def test_requires_api_key(self):
        with patch("config.settings.get_settings") as settings:
            settings.return_value.ghl_api_key = None
            settings.return_value.ghl_location_id = LOCATION_ID
            with pytest.raises(ValueError, match="GHL_API_KEY"):
                GHLCalendarClient.from_settings()

    def test_requires_location_id(self):
        with patch("config.settings.get_settings") as settings:
            settings.return_value.ghl_api_key = MagicMock(get_secret_value=lambda: "key")
            settings.return_value.ghl_location_id = None
            with pytest.raises(ValueError, match="GHL_LOCATION_ID"):
                GHLCalendarClient.from_settings()
