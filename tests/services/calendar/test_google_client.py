"""GoogleCalendarClient against a stubbed discovery service.

Payloads mirror what the Calendar API actually returns, so the request and
response shaping is exercised without credentials and without writing to a
real calendar.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.services.calendar.client import CalendarUnavailable
from src.services.calendar.google_client import (
    MAX_FREEBUSY_WINDOW,
    GoogleCalendarClient,
)

ET = ZoneInfo("America/New_York")
CALENDAR_ID = "leads@example.invalid"

START = datetime(2026, 6, 15, 12, tzinfo=timezone.utc)
END = START + timedelta(days=2)


class _HttpError(Exception):
    """Stands in for googleapiclient.errors.HttpError."""

    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.resp = MagicMock(status=status)


@pytest.fixture(autouse=True)
def _patch_http_error(monkeypatch):
    """Make the client's lazily imported HttpError our stub."""
    import googleapiclient.errors

    monkeypatch.setattr(googleapiclient.errors, "HttpError", _HttpError)


def _client(*, freebusy=None, event=None, insert=None, get_error=None, delete_error=None):
    service = MagicMock()

    if freebusy is not None:
        service.freebusy.return_value.query.return_value.execute.return_value = freebusy
    if insert is not None:
        service.events.return_value.insert.return_value.execute.return_value = insert
    if event is not None:
        service.events.return_value.get.return_value.execute.return_value = event
    if get_error is not None:
        service.events.return_value.get.return_value.execute.side_effect = get_error
    if delete_error is not None:
        service.events.return_value.delete.return_value.execute.side_effect = delete_error

    return GoogleCalendarClient(service=service), service


def _event_payload(**overrides) -> dict:
    payload = {
        "id": "evt_abc123",
        "status": "confirmed",
        "summary": "Intro call",
        "htmlLink": "https://calendar.google.com/event?eid=abc123",
        "start": {"dateTime": "2026-06-16T14:00:00-04:00", "timeZone": "America/New_York"},
        "end": {"dateTime": "2026-06-16T14:30:00-04:00", "timeZone": "America/New_York"},
        "attendees": [{"email": "borrower@example.invalid", "responseStatus": "needsAction"}],
    }
    payload.update(overrides)
    return payload


class TestGetBusy:
    def test_parses_busy_spans(self):
        client, _ = _client(freebusy={
            "calendars": {CALENDAR_ID: {"busy": [
                {"start": "2026-06-16T16:00:00Z", "end": "2026-06-16T17:00:00Z"},
                {"start": "2026-06-17T13:00:00Z", "end": "2026-06-17T13:30:00Z"},
            ]}}
        })
        busy = client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

        assert len(busy) == 2
        assert busy[0].start == datetime(2026, 6, 16, 16, tzinfo=timezone.utc)
        assert busy[1].end == datetime(2026, 6, 17, 13, 30, tzinfo=timezone.utc)

    def test_empty_calendar_returns_no_spans(self):
        client, _ = _client(freebusy={"calendars": {CALENDAR_ID: {"busy": []}}})
        assert client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END) == []

    def test_requests_the_window_it_was_given(self):
        client, service = _client(freebusy={"calendars": {CALENDAR_ID: {"busy": []}}})
        client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

        body = service.freebusy.return_value.query.call_args.kwargs["body"]
        assert body["timeMin"] == START.isoformat()
        assert body["timeMax"] == END.isoformat()
        assert body["items"] == [{"id": CALENDAR_ID}]

    def test_an_alias_resolved_to_another_id_is_still_matched(self):
        # "primary" can come back as the owner's address; one requested
        # calendar means the single entry is unambiguously it.
        client, _ = _client(freebusy={
            "calendars": {"owner@example.invalid": {"busy": [
                {"start": "2026-06-16T16:00:00Z", "end": "2026-06-16T17:00:00Z"},
            ]}}
        })
        assert len(client.get_busy(calendar_id="primary", start=START, end=END)) == 1

    def test_errors_raise_rather_than_reading_as_free(self):
        client, _ = _client(freebusy={
            "calendars": {CALENDAR_ID: {
                "busy": [],
                "errors": [{"domain": "global", "reason": "notFound"}],
            }}
        })
        with pytest.raises(CalendarUnavailable, match="notFound"):
            client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

    def test_missing_calendar_entry_raises(self):
        client, _ = _client(freebusy={"calendars": {
            "one@example.invalid": {"busy": []},
            "two@example.invalid": {"busy": []},
        }})
        with pytest.raises(CalendarUnavailable):
            client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

    def test_empty_response_raises(self):
        client, _ = _client(freebusy={})
        with pytest.raises(CalendarUnavailable):
            client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)

    def test_window_wider_than_google_allows_is_rejected_before_the_call(self):
        client, service = _client(freebusy={"calendars": {CALENDAR_ID: {"busy": []}}})
        with pytest.raises(ValueError, match="exceeds Google"):
            client.get_busy(
                calendar_id=CALENDAR_ID,
                start=START,
                end=START + MAX_FREEBUSY_WINDOW + timedelta(days=1),
            )
        service.freebusy.return_value.query.assert_not_called()

    def test_timestamp_without_an_offset_is_rejected(self):
        client, _ = _client(freebusy={
            "calendars": {CALENDAR_ID: {"busy": [
                {"start": "2026-06-16T16:00:00", "end": "2026-06-16T17:00:00"},
            ]}}
        })
        with pytest.raises(ValueError, match="no offset"):
            client.get_busy(calendar_id=CALENDAR_ID, start=START, end=END)


class TestCreateEvent:
    def test_returns_the_created_event(self):
        client, _ = _client(insert=_event_payload())
        event = client.create_event(
            calendar_id=CALENDAR_ID,
            start=datetime(2026, 6, 16, 14, tzinfo=ET),
            end=datetime(2026, 6, 16, 14, 30, tzinfo=ET),
            summary="Intro call",
            attendee_email="borrower@example.invalid",
        )
        assert event.event_id == "evt_abc123"
        assert event.attendee_email == "borrower@example.invalid"
        assert event.status == "confirmed"
        assert event.start == datetime(2026, 6, 16, 14, tzinfo=ET)

    def test_sends_the_invitation(self):
        client, service = _client(insert=_event_payload())
        client.create_event(
            calendar_id=CALENDAR_ID,
            start=datetime(2026, 6, 16, 14, tzinfo=ET),
            end=datetime(2026, 6, 16, 14, 30, tzinfo=ET),
            summary="Intro call",
            attendee_email="borrower@example.invalid",
        )
        kwargs = service.events.return_value.insert.call_args.kwargs
        assert kwargs["sendUpdates"] == "all", (
            "without this Google creates the event silently and the borrower "
            "is never told"
        )
        assert kwargs["body"]["attendees"] == [{"email": "borrower@example.invalid"}]
        assert kwargs["body"]["start"]["timeZone"] == "America/New_York"


class TestGetEvent:
    def test_returns_the_event(self):
        client, _ = _client(event=_event_payload())
        assert client.get_event(calendar_id=CALENDAR_ID, event_id="evt_abc123").summary == "Intro call"

    def test_missing_event_is_none_not_an_error(self):
        client, _ = _client(get_error=_HttpError(404))
        assert client.get_event(calendar_id=CALENDAR_ID, event_id="gone") is None

    def test_other_failures_propagate(self):
        client, _ = _client(get_error=_HttpError(500))
        with pytest.raises(_HttpError):
            client.get_event(calendar_id=CALENDAR_ID, event_id="evt_abc123")

    def test_cancelled_status_is_reported(self):
        client, _ = _client(event=_event_payload(status="cancelled"))
        assert client.get_event(calendar_id=CALENDAR_ID, event_id="evt_abc123").status == "cancelled"

    def test_all_day_event_boundaries_parse(self):
        client, _ = _client(event=_event_payload(
            start={"date": "2026-06-16"}, end={"date": "2026-06-17"},
        ))
        event = client.get_event(calendar_id=CALENDAR_ID, event_id="evt_abc123")
        assert event.start == datetime(2026, 6, 16, 0, 0, tzinfo=ET)

    def test_event_without_attendees_parses(self):
        client, _ = _client(event=_event_payload(attendees=[]))
        assert client.get_event(calendar_id=CALENDAR_ID, event_id="evt_abc123").attendee_email is None


class TestCancelEvent:
    def test_notifies_the_invitee(self):
        client, service = _client()
        client.cancel_event(calendar_id=CALENDAR_ID, event_id="evt_abc123")
        assert service.events.return_value.delete.call_args.kwargs["sendUpdates"] == "all"

    @pytest.mark.parametrize("status", [404, 410])
    def test_already_gone_is_silent(self, status):
        client, _ = _client(delete_error=_HttpError(status))
        client.cancel_event(calendar_id=CALENDAR_ID, event_id="evt_abc123")

    def test_other_failures_propagate(self):
        client, _ = _client(delete_error=_HttpError(500))
        with pytest.raises(_HttpError):
            client.cancel_event(calendar_id=CALENDAR_ID, event_id="evt_abc123")


class TestProtocolConformance:
    def test_satisfies_the_client_protocol(self):
        from src.services.calendar.client import CalendarClient

        client, _ = _client()
        assert isinstance(client, CalendarClient)

    def test_from_settings_requires_a_subject(self):
        from unittest.mock import patch

        with patch("config.settings.get_settings") as settings:
            settings.return_value.cora_gmail_service_account_key_path = "/tmp/key.json"
            settings.return_value.fa_max_calendar_subject = None
            with pytest.raises(ValueError, match="FA_MAX_CALENDAR_SUBJECT"):
                GoogleCalendarClient.from_settings()
