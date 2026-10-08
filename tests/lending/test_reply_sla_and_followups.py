"""The one-business-hour reply check, a GHL reschedule that carries a new time, and the small follow-ups
(suppression logged as a warning, a Fake text sender behind the port)."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed
from src.lending.consent import record_consent
from src.lending.reminder_worker import FakeTextSender, process_due
from src.lending.reply_guard import ReplyEvent, classify, handle_reply_event, parse_event
from src.tasks import lending_reply_sla as sla

PHONE = "+18135550147"
SLOT = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)
BOOKED = datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


# ── business minutes ─────────────────────────────────────────────────────────

TUE_10 = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)         # Tue 10:00 ET


@pytest.mark.parametrize("start,end,expected", [
    (TUE_10, TUE_10 + timedelta(minutes=45), 45),
    (TUE_10, TUE_10 + timedelta(hours=2), 120),
    (datetime(2026, 10, 6, 22, 45, tzinfo=timezone.utc), datetime(2026, 10, 7, 14, 30, tzinfo=timezone.utc), 30 + 90),   # 6:45 PM Tue -> 10:30 AM Wed
    (datetime(2026, 10, 10, 16, 0, tzinfo=timezone.utc), datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc), 60),        # Sat noon -> Mon 10:00
    (datetime(2026, 10, 7, 3, 0, tzinfo=timezone.utc), datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc), 0),            # all outside hours
    (TUE_10, TUE_10, 0),
])
def test_business_minutes_only_counts_josh_hours(start, end, expected):
    assert sla.business_minutes(start, end) == expected


def test_business_minutes_is_exactly_sixty_at_the_boundary():
    assert sla.business_minutes(TUE_10, TUE_10 + timedelta(minutes=60)) == 60
    assert sla.business_minutes(TUE_10, TUE_10 + timedelta(minutes=59)) == 59


# ── response tracking and the overdue check ──────────────────────────────────

def handoff(db, message_id="m1", contact="c1", kind="rate_terms_handoff", posted_at=TUE_10, body="What's your rate?"):
    db.execute(text("INSERT INTO lending.reply_handoffs (message_id, kind, contact_id, phone_hash, posted_at) "
                    "VALUES (:m, :k, :c, 'abc', :p)"), {"m": message_id, "k": kind, "c": contact, "p": posted_at})


def test_a_handoff_unanswered_for_an_hour_is_listed_once_and_then_marked(db):
    handoff(db)
    now = TUE_10 + timedelta(minutes=61)
    rows = sla.overdue_handoffs(db, now)
    assert [r["message_id"] for r in rows] == ["m1"]
    assert "unanswered after 60 business minutes" in sla.format_alert(rows) and "c1" in sla.format_alert(rows)
    db.execute(sla._MARK_ALERTED, {"ids": ["m1"]})
    assert sla.overdue_handoffs(db, now) == []


def test_a_handoff_inside_the_hour_or_posted_overnight_is_not_yet_overdue(db):
    handoff(db, "m1", posted_at=TUE_10)
    assert sla.overdue_handoffs(db, TUE_10 + timedelta(minutes=59)) == []
    handoff(db, "m2", contact="c2", posted_at=datetime(2026, 10, 7, 2, 0, tzinfo=timezone.utc))      # 10 PM ET
    ids = lambda moment: [r["message_id"] for r in sla.overdue_handoffs(db, moment)]
    assert "m2" not in ids(datetime(2026, 10, 7, 13, 30, tzinfo=timezone.utc))      # 9:30 AM: 30 business minutes
    assert "m2" in ids(datetime(2026, 10, 7, 14, 1, tzinfo=timezone.utc))           # 10:01 AM: 61 business minutes


def out(**kw):
    base = dict(message_id="o1", direction="outbound", body="Happy to walk you through it on a call", contact_id="c1",
                first_name="Marcus", phone=PHONE, handoff=False, from_user=True)
    return ReplyEvent(**{**base, **kw})


def test_a_person_replying_marks_the_handoff_answered(db):
    handoff(db)
    assert handle_reply_event(db, out(), poster=None) == "ignored"
    assert sla.overdue_handoffs(db, TUE_10 + timedelta(hours=3)) == []


def test_an_ai_or_workflow_message_does_not_mark_it_answered(db):
    handoff(db)
    assert handle_reply_event(db, out(from_user=False, message_id="o2"), poster=None) == "ignored"
    assert len(sla.overdue_handoffs(db, TUE_10 + timedelta(hours=3))) == 1


def test_a_person_replying_to_a_different_contact_changes_nothing(db):
    handoff(db)
    handle_reply_event(db, out(contact_id="other"), poster=None)
    assert len(sla.overdue_handoffs(db, TUE_10 + timedelta(hours=3))) == 1


def test_josh_may_quote_numbers_only_the_ai_is_held_to_it():
    assert classify(out(body="Rates start around 9.5%")) is None
    assert classify(out(body="Rates start around 9.5%", from_user=False)) == "ai_quoted_numbers"


def test_parse_event_reads_the_user_id_as_a_person_sent_it():
    assert parse_event({"messageId": "m", "body": "hi", "direction": "outbound", "userId": "u1"}).from_user is True
    assert parse_event({"messageId": "m", "body": "hi", "direction": "outbound"}).from_user is False


def test_the_check_needs_slack_configured(monkeypatch):
    monkeypatch.setattr(sla, "slack_poster", lambda: None)
    assert sla.main(now=TUE_10) == 1


# ── a GHL reschedule that carries a new time ─────────────────────────────────

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


def appointment(client, status, **extra):
    body = {"appointmentId": "appt-r1", "appointmentStatus": status, **extra}
    return client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": "s3cret"}, json=body)


def book(db):
    handle_booking_confirmed(db, {"booking_ref": "r1", "provider_event_id": "appt-r1", "phone": PHONE, "first_name": "Marcus",
                                  "slot_start_utc": SLOT, "booked_by": "dana@heu.ai"}, now=BOOKED)


def test_a_rescheduled_event_with_a_new_time_re_plans_the_messages(client, db):
    book(db)
    response = appointment(client, "rescheduled", startTime="2099-10-08T11:00:00-04:00")
    assert response.json()["status"] == "scheduled"
    assert {r[0] for r in db.execute(text("SELECT slot_start_utc FROM lending.booking_messages"))} == {datetime(2099, 10, 8, 15, 0, tzinfo=timezone.utc)}
    assert {r[0] for r in db.execute(text("SELECT status FROM lending.booking_messages"))} == {"pending"}


def test_a_rescheduled_event_without_a_time_still_just_cancels(client, db):
    book(db)
    assert appointment(client, "rescheduled").json() == {"status": "rescheduled", "cancelled": 3}


# ── follow-ups ───────────────────────────────────────────────────────────────

def test_a_suppressed_contact_is_logged_as_a_warning(db, caplog):
    handle_booking_confirmed(db, {"booking_ref": "r9", "phone": PHONE, "first_name": "Marcus", "slot_start_utc": SLOT,
                                  "booked_by": "dana@heu.ai"}, now=BOOKED)
    record_consent(db, PHONE, "inbound_text")
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    with caplog.at_level(logging.INFO, logger="src.lending.reminder_worker"):
        process_due(db, text_sender=FakeTextSender(), text_enabled=True, email_enabled=False, number="+18135550100", now=BOOKED)
    assert any(r.levelno == logging.WARNING and "skipped_suppressed" in r.getMessage() for r in caplog.records)


def test_the_fake_text_sender_sits_behind_the_port(db):
    handle_booking_confirmed(db, {"booking_ref": "r8", "phone": PHONE, "first_name": "Marcus", "slot_start_utc": SLOT,
                                  "booked_by": "dana@heu.ai", "text_consent": True}, now=BOOKED)
    sender = FakeTextSender()
    assert process_due(db, text_sender=sender, text_enabled=True, email_enabled=False, number="+18135550100", now=BOOKED) == {"sent": 1}
    assert sender.sent and sender.sent[0][0] == PHONE
