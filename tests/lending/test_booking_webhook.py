"""WP-GL-10: a GHL appointment cancelled / rescheduled cancels the booking's pending messages."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed

SECRET = "test-ghl-secret"
SLOT = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    person = lending_db.execute(text("INSERT INTO fa_max_persons (source, full_name, phone) VALUES ('test', 'Jane Doe', "
                                     "'+18135550111') RETURNING person_id::text")).scalar()
    handle_booking_confirmed(lending_db, {"booking_ref": "ref-1", "provider_event_id": "appt-1", "person_id": person,
                                          "slot_start_utc": SLOT, "booked_by": "dana@heu.ai"}, now=NOW)
    return lending_db


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending.booking_webhook import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def statuses(db):
    return {r[0]: r[1] for r in db.execute(text("SELECT kind, status FROM lending.booking_messages"))}


def post(client, body, secret=SECRET):
    headers = {"X-Webhook-Secret": secret} if secret else {}
    return client.post("/webhooks/lending/ghl-appointment", headers=headers, json=body)


def test_a_cancelled_appointment_cancels_all_pending_messages(client, db):
    response = post(client, {"appointmentId": "appt-1", "appointmentStatus": "cancelled"})
    assert response.status_code == 200 and response.json() == {"status": "cancelled", "cancelled": 3}
    assert set(statuses(db).values()) == {"cancelled"}


def test_a_rescheduled_appointment_cancels_the_old_messages(client, db):
    response = post(client, {"appointment": {"id": "appt-1", "appointmentStatus": "rescheduled"}})
    assert response.json()["cancelled"] == 3
    assert db.execute(text("SELECT DISTINCT cancel_reason FROM lending.booking_messages")).scalar() == "booking_rescheduled"


def test_an_unrelated_status_or_unknown_appointment_changes_nothing(client, db):
    assert post(client, {"appointmentId": "appt-1", "appointmentStatus": "showed"}).json()["status"] == "noop"
    assert post(client, {"appointmentId": "other", "appointmentStatus": "cancelled"}).json()["cancelled"] == 0
    assert post(client, {"type": "ping"}).json()["reason"] == "no_appointment_id"
    assert set(statuses(db).values()) == {"pending"}


def test_the_booking_ref_is_not_an_appointment_id(client, db):
    assert post(client, {"appointmentId": "ref-1", "appointmentStatus": "cancelled"}).json()["cancelled"] == 0


def test_a_wrong_or_missing_secret_is_rejected(client, db):
    assert post(client, {"appointmentId": "appt-1", "appointmentStatus": "cancelled"}, secret="nope").status_code == 401
    assert post(client, {"appointmentId": "appt-1", "appointmentStatus": "cancelled"}, secret=None).status_code == 401
    assert set(statuses(db).values()) == {"pending"}
