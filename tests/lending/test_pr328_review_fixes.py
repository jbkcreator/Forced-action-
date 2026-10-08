"""PR #328 review: consent is not re-granted on redelivery, cancelled tasks leave the list, a reschedule is
re-planned, an unconfigured Slack is a 503, and borrower text cannot inject Slack markup."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import cancel_by_provider_event, handle_booking_confirmed
from src.lending.confirmation_tasks import complete_confirmation_task
from src.lending.consent import has_text_consent, revoke_consent
from src.lending.reply_guard import ReplyEvent, format_slack
from src.tasks import lending_confirmation_tasks as digest

SECRET = "test-ghl-secret"
PHONE = "+18135550147"
BOOKED_AT = datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)
SLOT_A = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)
SLOT_B = datetime(2026, 10, 8, 15, 0, tzinfo=timezone.utc)
TODAY = date(2026, 10, 6)
LIST_NOW = datetime(2026, 10, 6, 13, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, slot=SLOT_A, **extra):
    payload = {"booking_ref": "ref-1", "provider_event_id": "appt-1", "phone": PHONE, "first_name": "Marcus",
               "slot_start_utc": slot, "booked_by": "dana@heu.ai", "text_consent": True, **extra}
    return handle_booking_confirmed(db, payload, now=BOOKED_AT)


def rows(db):
    return {r[0]: r[1:] for r in db.execute(text(
        "SELECT kind, status, slot_start_utc, attempts FROM lending.booking_messages ORDER BY kind"))}


# 1. consent
def test_a_redelivered_booking_does_not_re_grant_revoked_consent(db):
    book(db)
    assert has_text_consent(db, PHONE) is True
    revoke_consent(db, PHONE)
    assert has_text_consent(db, PHONE) is False
    assert book(db).inserted == 0
    assert has_text_consent(db, PHONE) is False


def test_a_reschedule_does_not_re_grant_revoked_consent_either(db):
    book(db)
    revoke_consent(db, PHONE)
    book(db, slot=SLOT_B)
    assert has_text_consent(db, PHONE) is False


# 2. confirmation tasks
def listed(db):
    return digest.open_tasks(db, TODAY, LIST_NOW)


def test_a_cancelled_booking_leaves_the_morning_list(db):
    book(db)
    assert len(listed(db)) == 1
    cancel_by_provider_event(db, "appt-1", "booking_cancelled")
    assert listed(db) == []


def test_a_completed_task_leaves_the_list_once(db):
    book(db)
    assert complete_confirmation_task(db, "ref-1") is True
    assert complete_confirmation_task(db, "ref-1") is False
    assert listed(db) == []


def test_completing_a_cancelled_task_does_nothing(db):
    book(db)
    cancel_by_provider_event(db, "appt-1", "booking_cancelled")
    assert complete_confirmation_task(db, "ref-1") is False


# 3. reschedule under the same booking_ref
def test_a_reschedule_re_plans_the_messages_and_revives_the_task(db):
    book(db)
    db.execute(text("UPDATE lending.booking_messages SET status = 'sent' WHERE kind = 'confirmation'"))
    cancel_by_provider_event(db, "appt-1", "booking_rescheduled")
    assert listed(db) == []
    assert book(db, slot=SLOT_B).inserted == 3
    assert {k: (v[0], v[1]) for k, v in rows(db).items()} == {k: ("pending", SLOT_B) for k in ("confirmation", "night_before", "ninety_min")}
    assert db.execute(text("SELECT due_date FROM lending.confirmation_tasks")).scalar() == date(2026, 10, 7)
    assert db.execute(text("SELECT cancelled_at FROM lending.confirmation_tasks")).scalar() is None


def test_a_redelivery_of_the_same_slot_after_a_cancel_stays_cancelled(db):
    book(db)
    cancel_by_provider_event(db, "appt-1", "booking_cancelled")
    assert book(db).inserted == 0
    assert {v[0] for v in rows(db).values()} == {"cancelled"}
    assert listed(db) == []


def test_a_reschedule_leaves_a_row_that_is_being_sent_right_now(db):
    book(db)
    db.execute(text("UPDATE lending.booking_messages SET status = 'sending' WHERE kind = 'confirmation'"))
    assert book(db, slot=SLOT_B).inserted == 2
    assert rows(db)["confirmation"][0] == "sending"


# 4. webhook with Slack unconfigured; 5. Slack escaping
@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.lending import booking_webhook, reply_webhook
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(reply_webhook.router)
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_an_unconfigured_slack_is_a_503_so_ghl_retries(client, monkeypatch):
    from src.lending import reply_webhook
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: None)
    response = client.post("/webhooks/lending/ghl-reply", headers={"X-Webhook-Secret": SECRET},
                           json={"messageId": "m1", "body": "What's your rate?"})
    assert response.status_code == 503
    monkeypatch.setattr(reply_webhook, "slack_poster", lambda: (lambda message: None))
    assert client.post("/webhooks/lending/ghl-reply", headers={"X-Webhook-Secret": SECRET},
                       json={"messageId": "m1", "body": "What's your rate?"}).json() == {"status": "posted"}


def test_the_complete_endpoint_takes_a_task_off_the_list(client, db):
    book(db)
    post = lambda ref, secret=SECRET: client.post("/webhooks/lending/confirmation-task-complete",
                                                  headers={"X-Webhook-Secret": secret}, json={"booking_ref": ref})
    assert post("ref-1", secret="nope").status_code == 401
    assert post("").status_code == 422
    assert post("ref-1").json() == {"status": "completed"}
    assert post("ref-1").json() == {"status": "no_open_task"}
    assert listed(db) == []


def test_borrower_text_cannot_inject_slack_markup():
    event = ReplyEvent(message_id="m", direction="inbound", body="rate? <!channel> <https://evil.example|click> & more",
                       contact_id="c<1>", first_name="<!here>", phone=PHONE, handoff=False)
    message = format_slack("rate_terms_handoff", event)
    assert "<!channel>" not in message and "<!here>" not in message and "<https://" not in message
    assert "&lt;!channel&gt;" in message and "&amp; more" in message and "c&lt;1&gt;" in message
