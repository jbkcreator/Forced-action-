"""A failed caller check on an AI-booked call frees Josh's GHL slot (behind LENDING_GHL_RELEASE_SLOT_ENABLED)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending import booking_webhook, ghl_appointments
from src.lending.booking_messages import handle_booking_confirmed
from src.lending.ghl_appointments import FakeAppointmentCanceller, GhlAppointmentCanceller, get_appointment_canceller
from src.lending.ghl_sms import GhlAccount

PHONE = "+18135550147"
SLOT = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)
BOOKED = datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc)
SECRET_KEY = "pit-super-secret-key"


# ── the live canceller (HTTP is faked) ───────────────────────────────────────

def canceller(status=200, error=None):
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if error:
            raise error
        return SimpleNamespace(status_code=status)

    return GhlAppointmentCanceller(GhlAccount(SECRET_KEY, "loc1"), request), calls


def test_it_cancels_the_appointment_with_the_right_call():
    live, calls = canceller()
    assert live("appt-9") is True
    method, url, kwargs = calls[0]
    assert method == "PUT" and url.endswith("/calendars/events/appointments/appt-9")
    assert kwargs["json"] == {"appointmentStatus": "cancelled"}
    assert kwargs["headers"]["Authorization"] == f"Bearer {SECRET_KEY}"


@pytest.mark.parametrize("status", [400, 404, 500])
def test_a_refusal_is_false_and_logged_without_the_key(status, caplog):
    live, _ = canceller(status=status)
    with caplog.at_level(logging.INFO):
        assert live("appt-9") is False
    assert SECRET_KEY not in caplog.text and f"HTTP {status}" in caplog.text


def test_a_network_error_is_false_not_an_exception(caplog):
    live, _ = canceller(error=ConnectionError(f"boom {SECRET_KEY}"))
    with caplog.at_level(logging.INFO):
        assert live("appt-9") is False
    assert SECRET_KEY not in caplog.text and "ConnectionError" in caplog.text


def test_the_factory_is_off_by_default_and_needs_the_account(monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_ghl_release_slot_enabled", False, raising=False)
    assert get_appointment_canceller() is None
    monkeypatch.setattr(get_settings(), "lending_ghl_release_slot_enabled", True, raising=False)
    monkeypatch.setattr(ghl_appointments, "lending_ghl_account", lambda: None)
    assert get_appointment_canceller() is None
    monkeypatch.setattr(ghl_appointments, "lending_ghl_account", lambda: GhlAccount(SECRET_KEY, "loc1"))
    assert isinstance(get_appointment_canceller(), GhlAppointmentCanceller)


# ── through the nurture webhook ──────────────────────────────────────────────

@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr("s3cret"), raising=False)
    app = FastAPI()
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def book(db, ref="r1", booked_by="ai", event="appt-1"):
    handle_booking_confirmed(db, {"booking_ref": ref, "provider_event_id": event, "phone": PHONE, "first_name": "Marcus",
                                  "slot_start_utc": SLOT, "booked_by": booked_by}, now=BOOKED)


def nurture(client):
    return client.post("/webhooks/lending/ghl-nurture", headers={"X-Webhook-Secret": "s3cret"},
                       json={"stage_name": "Nurture", "phone": PHONE})


def test_off_by_default_nothing_is_cancelled_and_the_response_is_unchanged(client, db):
    book(db)
    assert nurture(client).json() == {"status": "queued"}


def test_a_failed_check_releases_the_slot_once(client, db, monkeypatch):
    fake = FakeAppointmentCanceller()
    monkeypatch.setattr(booking_webhook, "get_appointment_canceller", lambda: fake)
    book(db)
    assert nurture(client).json() == {"status": "queued", "slot_released": True}
    assert nurture(client).json() == {"status": "duplicate"}
    assert fake.cancelled == ["appt-1"]


def test_a_caller_booked_call_or_an_unknown_phone_never_cancels(client, db, monkeypatch):
    fake = FakeAppointmentCanceller()
    monkeypatch.setattr(booking_webhook, "get_appointment_canceller", lambda: fake)
    book(db, booked_by="dana@heu.ai")
    assert nurture(client).json() == {"status": "no_ai_booking"}
    assert fake.cancelled == []


def test_a_refused_release_is_reported_to_slack_and_the_message_still_went(client, db, monkeypatch):
    posts = []
    monkeypatch.setattr(booking_webhook, "get_appointment_canceller", lambda: FakeAppointmentCanceller(succeed=False))
    monkeypatch.setattr(booking_webhook, "slack_poster", lambda: posts.append)
    book(db)
    assert nurture(client).json() == {"status": "queued", "slot_released": False}
    assert len(posts) == 1 and "appt-1" in posts[0] and "cancel it by hand" in posts[0]
    assert db.execute(text("SELECT status FROM lending.booking_messages WHERE kind = 'gate_fail'")).scalar() == "pending"
