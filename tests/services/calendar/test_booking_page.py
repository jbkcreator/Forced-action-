"""The borrower-facing booking page.

Exercised through the real router with the calendar faked, so routing,
escaping and the refusal paths are covered without a live calendar.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.booking_router import router
from src.api.deps import get_db
from src.services.calendar import FakeCalendar, Slot

SLUG = "Ab3xY9zQw1"
ATTENDEE = "borrower@example.invalid"
CALENDAR_ID = "leads@example.invalid"


def _future_slot(days: int = 2, hour: int = 14) -> Slot:
    start = (datetime.now(timezone.utc) + timedelta(days=days)).replace(
        hour=hour, minute=0, second=0, microsecond=0
    )
    return Slot(start=start, end=start + timedelta(minutes=30))


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: MagicMock()
    return TestClient(app)


@pytest.fixture(autouse=True)
def _calendar_configured():
    # rincr is patched where the router bound it, not at its source: the
    # router imported the name, so patching src.core.redis_client leaves the
    # bound reference pointing at the real counter — which is shared, and
    # would trip the rate limit partway through the suite.
    with patch("src.api.booking_router.get_calendar_client", return_value=FakeCalendar()), \
         patch("src.api.booking_router.get_calendar_id", return_value=CALENDAR_ID), \
         patch("src.api.booking_router.record_click"), \
         patch("src.api.booking_router.has_live_booking", return_value=False), \
         patch("src.api.booking_router.rincr", return_value=1):
        yield


def _already_booked():
    return patch("src.api.booking_router.has_live_booking", return_value=True)


def _link(link_id: int = 7):
    return MagicMock(id=link_id, property_id=None, buyer_entity_id=None)


def _slots(*slots):
    return patch("src.api.booking_router.get_slots", return_value=list(slots))


class TestPage:
    def test_renders_the_picker_with_slots(self, client):
        slot = _future_slot()
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), _slots(slot):
            response = client.get(f"/book/{SLUG}")

        assert response.status_code == 200
        assert "Book a call" in response.text
        assert slot.start.isoformat() in response.text
        assert 'class="slot"' in response.text

    def test_unknown_slug_shows_a_notice_not_an_error(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=None):
            response = client.get(f"/book/{SLUG}")

        assert response.status_code == 200, "a dead link is a message, not a 500"
        assert "no longer active" in response.text

    def test_no_availability_shows_a_notice(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), _slots():
            response = client.get(f"/book/{SLUG}")

        assert response.status_code == 200
        assert "no open times" in response.text

    def test_calendar_failure_degrades_instead_of_leaking(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.get_slots", side_effect=RuntimeError("freebusy exploded")
        ):
            response = client.get(f"/book/{SLUG}")

        assert response.status_code == 200
        # Apostrophes are escaped on the way out, so match a clean substring.
        assert "show times right now" in response.text
        assert "freebusy exploded" not in response.text, "internals must not reach the visitor"

    def test_click_is_attributed_before_anything_is_typed(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link(42)), _slots(
            _future_slot()
        ), patch("src.api.booking_router.record_click") as record:
            client.get(f"/book/{SLUG}")

        assert record.call_args.kwargs["tracked_link_id"] == 42

    def test_raw_ip_is_never_recorded(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), _slots(
            _future_slot()
        ), patch("src.api.booking_router.record_click") as record:
            client.get(f"/book/{SLUG}")

        ip_hash = record.call_args.kwargs["ip_hash"]
        assert ip_hash is None or (len(ip_hash) == 64 and "." not in ip_hash)

    def test_slug_is_escaped_into_the_page(self, client):
        # No slash in the payload: a "/" would split the path and the route
        # would simply not match, making this assertion pass for the wrong
        # reason. This one really does reach the rendered page.
        hostile = 'x"><img src=x onerror=alert(1)>'
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), _slots(
            _future_slot()
        ):
            response = client.get(f"/book/{hostile}")

        assert response.status_code == 200, "the payload must actually reach the page"
        assert "<img src=x onerror=alert(1)>" not in response.text
        assert "&lt;img" in response.text, "it should appear, escaped"

    def test_a_link_that_already_booked_shows_a_notice(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), \
             _already_booked(), _slots(_future_slot()):
            response = client.get(f"/book/{SLUG}")

        assert response.status_code == 200
        assert "already have a call booked" in response.text
        assert 'class="slot"' not in response.text, "no picker for a spent link"

    def test_the_page_reads_availability_from_cache(self, client):
        """A shared link is opened repeatedly; each view must not call Google."""
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.get_slots", return_value=[_future_slot()]
        ) as slots_call:
            client.get(f"/book/{SLUG}")

        assert slots_call.call_args.kwargs["use_cache"] is True

    def test_page_is_not_indexable(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), _slots(
            _future_slot()
        ):
            response = client.get(f"/book/{SLUG}")
        assert 'name="robots" content="noindex, nofollow"' in response.text


class TestSubmit:
    def _payload(self, slot=None, email=ATTENDEE):
        slot = slot or _future_slot()
        return {
            "starts_at": slot.start.isoformat(),
            "ends_at": slot.end.isoformat(),
            "name": "Maria",
            "email": email,
        }

    def test_books_and_reports_the_reference(self, client):
        booked = MagicMock(booked=True, booking_ref="REF123", reason=None)
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book", return_value=booked
        ):
            response = client.post(f"/api/book/{SLUG}", json=self._payload())

        assert response.status_code == 200
        assert response.json() == {
            "booked": True, "booking_ref": "REF123", "email": ATTENDEE
        }

    def test_taken_slot_is_an_answer_not_an_error(self, client):
        refused = MagicMock(booked=False, booking_ref=None, reason="slot_taken")
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book", return_value=refused
        ):
            response = client.post(f"/api/book/{SLUG}", json=self._payload())

        assert response.status_code == 200
        assert response.json() == {"booked": False, "reason": "slot_taken"}

    def test_suppressed_recipient_is_refused(self, client):
        refused = MagicMock(booked=False, booking_ref=None, reason="suppressed")
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book", return_value=refused
        ):
            response = client.post(f"/api/book/{SLUG}", json=self._payload())

        assert response.json()["booked"] is False
        assert response.json()["reason"] == "suppressed"

    def test_a_second_booking_from_one_link_is_refused(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), \
             _already_booked(), patch("src.api.booking_router.book") as book_call:
            response = client.post(f"/api/book/{SLUG}", json=self._payload())

        assert response.json() == {"booked": False, "reason": "already_booked"}
        book_call.assert_not_called(), "the guard must run before anything is written"

    def test_malformed_email_is_rejected_before_booking(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book"
        ) as book_call:
            response = client.post(
                f"/api/book/{SLUG}", json=self._payload(email="not-an-email")
            )

        assert response.status_code == 400
        book_call.assert_not_called()

    def test_unknown_slug_is_rejected_before_booking(self, client):
        with patch("src.api.booking_router.resolve_slug", return_value=None), patch(
            "src.api.booking_router.book"
        ) as book_call:
            response = client.post(f"/api/book/{SLUG}", json=self._payload())

        assert response.status_code == 404
        book_call.assert_not_called()

    def test_naive_timestamp_is_rejected(self, client):
        payload = self._payload()
        payload["starts_at"] = "2026-06-16T14:00:00"
        payload["ends_at"] = "2026-06-16T14:30:00"
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book"
        ) as book_call:
            response = client.post(f"/api/book/{SLUG}", json=payload)

        assert response.status_code == 400
        book_call.assert_not_called()

    def test_inverted_range_is_rejected(self, client):
        slot = _future_slot()
        payload = self._payload(slot)
        payload["starts_at"], payload["ends_at"] = payload["ends_at"], payload["starts_at"]
        with patch("src.api.booking_router.resolve_slug", return_value=_link()), patch(
            "src.api.booking_router.book"
        ) as book_call:
            response = client.post(f"/api/book/{SLUG}", json=payload)

        assert response.status_code == 400
        book_call.assert_not_called()

    def test_missing_name_is_rejected_by_validation(self, client):
        payload = self._payload()
        payload["name"] = ""
        with patch("src.api.booking_router.resolve_slug", return_value=_link()):
            response = client.post(f"/api/book/{SLUG}", json=payload)
        assert response.status_code == 422


class TestRateLimiting:
    def test_view_is_rate_limited(self, client):
        with patch("src.api.booking_router.rincr", return_value=999), patch(
            "src.api.booking_router.resolve_slug", return_value=_link()
        ):
            response = client.get(f"/book/{SLUG}")
        assert response.status_code == 429

    def test_booking_is_rate_limited_before_it_writes(self, client):
        with patch("src.api.booking_router.rincr", return_value=999), patch(
            "src.api.booking_router.book"
        ) as book_call:
            response = client.post(
                f"/api/book/{SLUG}",
                json={
                    "starts_at": _future_slot().start.isoformat(),
                    "ends_at": _future_slot().end.isoformat(),
                    "name": "Maria",
                    "email": ATTENDEE,
                },
            )
        assert response.status_code == 429
        book_call.assert_not_called()
