"""PR #328 re-review: a repeat delivery after a cancel must not revive the task, a late "rescheduled" cancel must not
wipe a freshly re-planned booking, and a booking skipped for want of a phone or email can be completed."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import cancel_by_provider_event, handle_booking_confirmed
from src.lending.consent import has_text_consent
from src.tasks import lending_confirmation_tasks as digest

PHONE = "+18135550147"
BOOKED = datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc)
SLOT_A = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)
SLOT_B = datetime(2099, 10, 8, 15, 0, tzinfo=timezone.utc)
LIST_NOW = datetime(2099, 10, 6, 13, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, slot=SLOT_A, **extra):
    payload = {"booking_ref": "r1", "provider_event_id": "appt-1", "phone": PHONE, "first_name": "Marcus",
               "slot_start_utc": slot, "booked_by": "dana@heu.ai", "text_consent": True, **extra}
    return handle_booking_confirmed(db, payload, now=BOOKED)


def statuses(db):
    return {r[0]: r[1] for r in db.execute(text("SELECT kind, status FROM lending.booking_messages"))}


def listed(db, today=date(2099, 10, 6)):
    return digest.open_tasks(db, today, LIST_NOW)


# 1. the task is revived only when the messages are re-planned
def test_a_same_slot_redelivery_after_a_cancel_does_not_revive_the_task(db):
    book(db)
    cancel_by_provider_event(db, "appt-1", "booking_cancelled")
    assert book(db).inserted == 0
    assert listed(db) == []
    assert db.execute(text("SELECT cancelled_at IS NOT NULL FROM lending.confirmation_tasks")).scalar() is True


def test_a_new_slot_after_a_cancel_does_revive_it(db):
    book(db)
    cancel_by_provider_event(db, "appt-1", "booking_cancelled")
    assert book(db, slot=SLOT_B).inserted == 3
    assert db.execute(text("SELECT cancelled_at IS NULL FROM lending.confirmation_tasks")).scalar() is True


# 2. a late cancel after a re-plan
def test_a_rescheduled_cancel_that_arrives_after_the_replan_spares_the_new_rows(db):
    book(db)
    book(db, slot=SLOT_B)
    assert cancel_by_provider_event(db, "appt-1", "booking_rescheduled", spare_replanned_seconds=120) == 0
    assert set(statuses(db).values()) == {"pending"}
    assert db.execute(text("SELECT cancelled_at IS NULL FROM lending.confirmation_tasks")).scalar() is True


def test_a_rescheduled_cancel_that_arrives_first_still_works_and_the_replan_follows(db):
    book(db)
    assert cancel_by_provider_event(db, "appt-1", "booking_rescheduled", spare_replanned_seconds=120) == 3
    book(db, slot=SLOT_B)
    assert set(statuses(db).values()) == {"pending"}


def test_the_grace_window_expires_and_a_real_cancel_is_never_spared(db):
    book(db)
    book(db, slot=SLOT_B)
    assert cancel_by_provider_event(db, "appt-1", "booking_cancelled") == 3                  # no grace for a cancel
    book(db, slot=datetime(2099, 10, 9, 15, 0, tzinfo=timezone.utc))
    db.execute(text("UPDATE lending.booking_messages SET replanned_at = now() - interval '10 minutes'"))
    assert cancel_by_provider_event(db, "appt-1", "booking_rescheduled", spare_replanned_seconds=120) == 3


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


def appointment(client, status):
    return client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": "s3cret"},
                       json={"appointmentId": "appt-1", "appointmentStatus": status})


def test_through_the_webhook_a_late_rescheduled_event_is_spared_but_a_cancelled_one_is_not(client, db):
    book(db)
    book(db, slot=SLOT_B)
    assert appointment(client, "rescheduled").json() == {"status": "rescheduled", "cancelled": 0}
    assert set(statuses(db).values()) == {"pending"}
    assert appointment(client, "cancelled").json() == {"status": "cancelled", "cancelled": 3}


# 3. a booking skipped for want of a phone or email can be completed
def test_a_corrected_redelivery_with_a_phone_completes_a_skipped_booking_and_records_consent(db):
    first = book(db, phone=None)
    assert first.skip_reason == "no_person" and set(statuses(db).values()) == {"skipped"}
    assert has_text_consent(db, PHONE) is False
    second = book(db)
    assert second.skip_reason is None and second.inserted == 3
    assert set(statuses(db).values()) == {"pending"}
    assert db.execute(text("SELECT DISTINCT contact_phone FROM lending.booking_messages")).scalar() == PHONE
    assert has_text_consent(db, PHONE) is True
    assert book(db).inserted == 0


def test_completing_a_skipped_booking_does_not_re_grant_revoked_consent_on_a_later_redelivery(db):
    from src.lending.consent import revoke_consent
    book(db, phone=None)
    book(db)
    revoke_consent(db, PHONE)
    book(db)
    assert has_text_consent(db, PHONE) is False
