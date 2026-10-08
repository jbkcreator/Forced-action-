"""A caller's check fails for an AI-booked call (Josh Oct 4, "approved as written"): reminders are cancelled and the
contact gets one short message, by text if they consented and by email otherwise, through the usual gates."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed, handle_booking_gate_failed, render_email, render_text
from src.lending.consent import record_consent
from src.lending.reminder_worker import process_due

PHONE = "+18135550147"
SLOT = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)
BOOKED = datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc)      # 11:00 ET, window open
NUMBER = "+18135550100"
WORDING = ("Hi Marcus, this is Next Deal Lending. We're not able to hold the call we mentioned. "
           "Thanks for your interest, and we'll be in touch if anything changes.")


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, ref="r1", booked_by="ai", **extra):
    return handle_booking_confirmed(db, {"booking_ref": ref, "provider_event_id": f"appt-{ref}", "phone": PHONE,
                                         "first_name": "Marcus", "email": "m@example.com", "slot_start_utc": SLOT,
                                         "booked_by": booked_by, **extra}, now=BOOKED)


def statuses(db, ref="r1"):
    rows = db.execute(text("SELECT kind, status FROM lending.booking_messages WHERE booking_ref = :r"), {"r": ref})
    return {r[0]: r[1] for r in rows}


def mark_confirmation_sent(db):
    db.execute(text("UPDATE lending.booking_messages SET status = 'sent' WHERE kind = 'confirmation'"))


class Sender:
    def __init__(self):
        self.sent = []

    def __call__(self, phone, body, first_name, *, deadline):
        self.sent.append((phone, body))
        return "msg-1"


def cycle(db, sender, now=BOOKED):
    return process_due(db, text_sender=sender, text_enabled=True, email_enabled=False, number=NUMBER, now=now)


def test_the_wording_is_the_approved_text_with_no_stop_language_or_address():
    sms = render_text("gate_fail", first_name="Marcus", slot_start_utc=SLOT, property_address="412 Oak Ave, Tampa")
    assert sms == WORDING and "STOP" not in sms and "Oak" not in sms
    subject, body = render_email("gate_fail", first_name="Marcus", slot_start_utc=SLOT)
    assert subject == "Your call with Next Deal Lending" and "not able to hold the call we mentioned" in body


def test_a_failed_check_cancels_the_reminders_and_queues_one_message(db):
    book(db)
    mark_confirmation_sent(db)
    assert handle_booking_gate_failed(db, "r1", now=BOOKED) == "queued"
    assert statuses(db) == {"confirmation": "sent", "night_before": "cancelled", "ninety_min": "cancelled",
                            "gate_fail": "pending"}
    assert db.execute(text("SELECT cancelled_at IS NOT NULL FROM lending.confirmation_tasks")).scalar() is True


def test_a_repeated_failure_queues_nothing_more(db):
    book(db)
    handle_booking_gate_failed(db, "r1", now=BOOKED)
    assert handle_booking_gate_failed(db, "r1", now=BOOKED) == "duplicate"
    assert db.execute(text("SELECT count(*) FROM lending.booking_messages WHERE kind = 'gate_fail'")).scalar() == 1


def test_an_unknown_booking_and_a_caller_booked_call_send_nothing(db):
    assert handle_booking_gate_failed(db, "nope", now=BOOKED) == "unknown_booking"
    book(db, ref="r2", booked_by="dana@heu.ai")
    assert handle_booking_gate_failed(db, "r2", now=BOOKED) == "not_ai_booked"
    assert statuses(db, "r2") == {"confirmation": "pending", "night_before": "pending", "ninety_min": "pending"}


def test_a_consented_contact_is_texted_the_approved_wording(db):
    book(db)
    record_consent(db, PHONE, "inbound_text")
    mark_confirmation_sent(db)
    handle_booking_gate_failed(db, "r1", now=BOOKED)
    sender = Sender()
    assert cycle(db, sender) == {"sent": 1}
    assert sender.sent == [(PHONE, WORDING)]
    assert statuses(db)["gate_fail"] == "sent"


def test_without_consent_it_goes_to_email_and_email_is_off_so_it_is_skipped_visibly(db):
    book(db)
    mark_confirmation_sent(db)
    handle_booking_gate_failed(db, "r1", now=BOOKED)
    sender = Sender()
    assert cycle(db, sender) == {"skipped_email_not_enabled": 1}
    assert sender.sent == []


def test_a_suppressed_contact_is_not_messaged(db):
    book(db)
    record_consent(db, PHONE, "inbound_text")
    mark_confirmation_sent(db)
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"),
               {"p": PHONE})
    handle_booking_gate_failed(db, "r1", now=BOOKED)
    sender = Sender()
    assert cycle(db, sender) == {"skipped_suppressed": 1}
    assert sender.sent == []


def test_a_message_due_in_quiet_hours_waits_for_the_window(db):
    book(db)
    record_consent(db, PHONE, "inbound_text")
    mark_confirmation_sent(db)
    late = datetime(2099, 10, 2, 3, 0, tzinfo=timezone.utc)                       # 11 pm ET
    handle_booking_gate_failed(db, "r1", now=late)
    sender = Sender()
    assert cycle(db, sender, now=late) == {"deferred_quiet_hours": 1}
    assert sender.sent == []


def test_a_table_migrated_before_gate_fail_existed_accepts_it_after_the_migration_reruns(lending_db):
    apply_to(lending_db.connection())
    lending_db.execute(text("ALTER TABLE lending.booking_messages DROP CONSTRAINT booking_messages_kind_check"))
    lending_db.execute(text("ALTER TABLE lending.booking_messages ADD CONSTRAINT booking_messages_kind_check "
                            "CHECK (kind IN ('confirmation', 'night_before', 'ninety_min'))"))
    apply_to(lending_db.connection())
    book(lending_db)
    assert handle_booking_gate_failed(lending_db, "r1", now=BOOKED) == "queued"


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending import booking_webhook
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr("s3cret"), raising=False)
    app = FastAPI()
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_the_endpoint_queues_once_and_checks_the_secret(client, db):
    book(db)

    def post(body, secret="s3cret"):
        return client.post("/webhooks/lending/booking-gate-failed", headers={"X-Webhook-Secret": secret}, json=body)

    assert post({"booking_ref": "r1"}, secret="nope").status_code == 401
    assert post({}).status_code == 422
    assert post({"booking_ref": "r1"}).json() == {"status": "queued"}
    assert post({"booking_ref": "r1"}).json() == {"status": "duplicate"}
    assert post({"booking_ref": "zzz"}).json() == {"status": "unknown_booking"}
