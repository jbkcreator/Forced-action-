"""Go-live gaps found cross-checking WP-GL-10 against Josh's Oct 1/Oct 4 answers: a GHL appointment created by
hand schedules the reminders (the B8 fallback), and a reschedule request is handed to Josh in Slack."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed
from src.lending.consent import has_text_consent
from src.lending.reply_guard import ReplyEvent, asks_to_reschedule, classify, format_slack

SECRET = "test-ghl-secret"
PHONE = "+18135550147"
SLOT = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)


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


def appointment(client, status="confirmed", **extra):
    body = {"appointmentId": "appt-9", "appointmentStatus": status, "startTime": "2099-10-07T10:00:00-04:00",
            "contact": {"id": "c1", "firstName": "Marcus", "phone": "(813) 555-0147", "email": "m@example.com"}, **extra}
    return client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": SECRET}, json=body)


def rows(db):
    return db.execute(text("SELECT booking_ref, kind, status, slot_start_utc, property_address, booked_by "
                           "FROM lending.booking_messages ORDER BY kind")).all()


def test_an_appointment_created_by_hand_schedules_the_messages_and_the_task(client, db):
    response = appointment(client)
    assert response.json() == {"status": "scheduled", "inserted": 3, "skip_reason": None}
    assert {(r[0], r[1]) for r in rows(db)} == {("ghl-appt-9", k) for k in ("confirmation", "night_before", "ninety_min")}
    assert {r[3] for r in rows(db)} == {SLOT}
    assert db.execute(text("SELECT assignee FROM lending.confirmation_tasks")).scalar() == "jbkantor@gmail.com"


def test_a_ghl_appointment_never_grants_text_consent(client, db):
    appointment(client)
    assert has_text_consent(db, PHONE) is False


def test_a_redelivered_appointment_event_changes_nothing(client, db):
    appointment(client)
    assert appointment(client, status="new").json()["status"] == "duplicate"
    assert len(rows(db)) == 3


def test_an_appointment_the_booking_flow_already_scheduled_is_not_scheduled_twice(client, db):
    handle_booking_confirmed(db, {"booking_ref": "r1", "provider_event_id": "appt-9", "phone": PHONE, "first_name": "Marcus",
                                  "slot_start_utc": SLOT, "booked_by": "dana@heu.ai", "property_address": "412 Oak Ave"},
                             now=datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc))
    assert appointment(client).json()["status"] == "duplicate"
    assert {r[0] for r in rows(db)} == {"r1"}


def test_a_new_time_in_ghl_re_plans_the_flows_booking_and_keeps_its_address(client, db):
    handle_booking_confirmed(db, {"booking_ref": "r1", "provider_event_id": "appt-9", "phone": PHONE, "first_name": "Marcus",
                                  "slot_start_utc": SLOT, "booked_by": "dana@heu.ai", "property_address": "412 Oak Ave"},
                             now=datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc))
    assert appointment(client, startTime="2099-10-08T11:00:00-04:00").json()["status"] == "scheduled"
    assert {(r[0], r[3], r[4], r[5]) for r in rows(db)} == {("r1", datetime(2099, 10, 8, 15, 0, tzinfo=timezone.utc),
                                                           "412 Oak Ave", "dana@heu.ai")}


@pytest.mark.parametrize("override", [{"startTime": None}, {"startTime": "garbage"}, {"startTime": "2099-10-07T10:00:00"}])
def test_an_appointment_without_a_usable_start_time_schedules_nothing(client, db, override):
    assert appointment(client, **override).json() == {"status": "noop", "reason": "no_start_time"}
    assert rows(db) == []


def test_an_appointment_with_no_contact_method_is_recorded_as_skipped(client, db):
    response = appointment(client, contact={"id": "c1"})
    assert response.json()["status"] == "skipped" and response.json()["skip_reason"] == "no_person"


def test_cancelling_the_appointment_still_cancels_what_the_fallback_scheduled(client, db):
    appointment(client)
    assert appointment(client, status="cancelled").json() == {"status": "cancelled", "cancelled": 3}


@pytest.mark.parametrize("message", ["Can we reschedule?", "I can't make it Thursday", "cant make it, another time?",
                                     "Need a different day please", "Can we move the call to Friday", "change the time to 3"])
def test_a_reschedule_request_is_recognised(message):
    assert asks_to_reschedule(message) is True


@pytest.mark.parametrize("message", ["Thursday works", "Yes see you then", "What's your rate?", "STOP"])
def test_ordinary_replies_are_not_reschedule_requests(message):
    assert asks_to_reschedule(message) is False


def event(**kw):
    base = dict(message_id="m1", direction="inbound", body="Can we reschedule?", contact_id="c1", first_name="Marcus",
                phone=PHONE, handoff=False)
    return ReplyEvent(**{**base, **kw})


def test_a_reschedule_request_goes_to_slack_and_a_rate_question_still_wins():
    assert classify(event()) == "reschedule_request"
    assert classify(event(body="can we reschedule and what is the rate")) == "rate_terms_handoff"
    assert classify(event(direction="outbound", body="Sure, which day works?")) is None
    assert "Reschedule request" in format_slack("reschedule_request", event()) and "…0147" in format_slack("reschedule_request", event())
