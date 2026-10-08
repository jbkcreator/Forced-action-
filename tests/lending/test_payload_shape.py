"""The payload-shape logger records key paths and types only, and the webhooks accept GHL's default nested bodies."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending import payload_shape
from src.lending.payload_shape import key_paths, log_shape

SECRET = "s3cret"
SECRET_VALUE = "+18135550147"


def test_key_paths_lists_dotted_paths_and_types_in_order():
    body = {"contact": {"phone": SECRET_VALUE, "tags": ["a", "b"]}, "appointmentId": "x", "n": 3, "empty": []}
    assert list(key_paths(body)) == ["appointmentId:str", "contact.phone:str", "contact.tags[]:str", "empty:list", "n:int"]


def test_values_are_never_in_the_paths():
    assert SECRET_VALUE not in " ".join(key_paths({"contact": {"phone": SECRET_VALUE}, "body": "my rate question"}))


def test_log_shape_is_off_by_default_and_logs_paths_not_values_when_on(monkeypatch, caplog):
    from config.settings import get_settings
    settings = get_settings()
    with caplog.at_level(logging.INFO, logger="src.lending.payload_shape"):
        monkeypatch.setattr(settings, "lending_ghl_log_payload_shape", False, raising=False)
        log_shape("ghl-reply", {"body": "secret text"})
        assert caplog.records == []
        monkeypatch.setattr(settings, "lending_ghl_log_payload_shape", True, raising=False)
        log_shape("ghl-reply", {"body": "secret text", "contact": {"phone": SECRET_VALUE}})
    message = caplog.records[0].getMessage()
    assert "ghl-reply" in message and "contact.phone:str" in message
    assert "secret text" not in message and SECRET_VALUE not in message


def test_log_shape_never_raises(monkeypatch):
    monkeypatch.setattr(payload_shape, "get_settings", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    log_shape("x", {"a": 1})


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending import booking_webhook
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_the_appointment_webhook_accepts_ghls_default_nested_calendar_body(client, db):
    body = {"first_name": "Marcus", "phone": "(813) 555-0147", "email": "m@example.com",
            "calendar": {"appointmentId": "appt-nested", "appoinmentStatus": "confirmed",
                         "startTime": "2099-10-07T10:00:00-04:00"}}
    response = client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": SECRET}, json=body)
    assert response.json() == {"status": "scheduled", "inserted": 3, "skip_reason": None}
    row = db.execute(text("SELECT first_name, contact_phone, slot_start_utc FROM lending.booking_messages LIMIT 1")).one()
    assert row == ("Marcus", "+18135550147", datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc))
    cancel = {"calendar": {"appointmentId": "appt-nested", "status": "cancelled"}}
    assert client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": SECRET}, json=cancel).json()["cancelled"] == 3
