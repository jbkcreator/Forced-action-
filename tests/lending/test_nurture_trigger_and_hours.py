"""The failed-check trigger (a contact enters the GHL Nurture stage before an AI-booked call) and Josh's reply
hours on the Slack handoff (Mon-Fri 9:00 AM - 7:15 PM ET, Oct 4 email)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from migrations.apply_lending_booking_messages import apply_to
from src.lending.booking_messages import handle_booking_confirmed, handle_nurture_entry
from src.lending.reply_guard import ReplyEvent, format_slack, in_reply_hours

PHONE = "+18135550147"
SLOT = datetime(2099, 10, 7, 14, 0, tzinfo=timezone.utc)
BOOKED = datetime(2099, 10, 1, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(lending_db):
    apply_to(lending_db.connection())
    return lending_db


def book(db, ref="r1", booked_by="ai", phone=PHONE, slot=SLOT):
    handle_booking_confirmed(db, {"booking_ref": ref, "provider_event_id": f"appt-{ref}", "phone": phone,
                                  "first_name": "Marcus", "slot_start_utc": slot, "booked_by": booked_by}, now=BOOKED)


def kinds(db):
    return {r[0]: r[1] for r in db.execute(text("SELECT kind, status FROM lending.booking_messages"))}


def test_nurture_before_an_ai_booked_call_runs_the_failed_check_handling(db):
    book(db)
    assert handle_nurture_entry(db, "(813) 555-0147", now=BOOKED) == "queued"
    assert kinds(db)["gate_fail"] == "pending" and kinds(db)["night_before"] == "cancelled"


def test_a_repeated_nurture_event_does_nothing_more(db):
    book(db)
    handle_nurture_entry(db, PHONE, now=BOOKED)
    assert handle_nurture_entry(db, PHONE, now=BOOKED) == "duplicate"


def test_a_caller_booked_call_a_past_call_and_an_unknown_phone_are_left_alone(db):
    book(db, ref="r1", booked_by="dana@heu.ai")
    assert handle_nurture_entry(db, PHONE, now=BOOKED) == "no_ai_booking"
    assert "gate_fail" not in kinds(db)
    book(db, ref="r2", phone="+18135550222", booked_by="ai")
    after_call = datetime(2099, 10, 8, 15, 0, tzinfo=timezone.utc)
    assert handle_nurture_entry(db, "+18135550222", now=after_call) == "no_ai_booking"
    assert handle_nurture_entry(db, "+18135559999", now=BOOKED) == "no_ai_booking"
    assert handle_nurture_entry(db, "", now=BOOKED) == "no_ai_booking"


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


def stage(client, body, secret="s3cret"):
    return client.post("/webhooks/lending/ghl-nurture", headers={"X-Webhook-Secret": secret}, json=body)


def test_the_webhook_acts_only_on_the_nurture_stage(client, db):
    book(db, slot=datetime(2099, 12, 7, 14, 0, tzinfo=timezone.utc))
    assert stage(client, {"stage_name": "Booked", "phone": PHONE}).json() == {"status": "noop", "reason": "stage_not_handled"}
    assert stage(client, {"stage_name": "Nurture"}).json() == {"status": "noop", "reason": "no_phone"}
    assert "gate_fail" not in kinds(db)
    assert stage(client, {"stage_name": "nurture", "contact": {"phone": "(813) 555-0147"}}).json() == {"status": "queued"}
    assert kinds(db)["gate_fail"] == "pending"


def test_the_webhook_checks_the_secret(client, db):
    assert stage(client, {"stage_name": "Nurture", "phone": PHONE}, secret="nope").status_code == 401


def event(**kw):
    base = dict(message_id="m1", direction="inbound", body="What's your rate?", contact_id="c1", first_name="Marcus",
                phone=PHONE, handoff=False)
    return ReplyEvent(**{**base, **kw})


TUE_NOON = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)        # Tue 12:00 ET
TUE_7_14 = datetime(2026, 10, 6, 23, 14, tzinfo=timezone.utc)       # 7:14 PM ET
TUE_7_15 = datetime(2026, 10, 6, 23, 15, tzinfo=timezone.utc)       # 7:15 PM ET
TUE_8_59 = datetime(2026, 10, 6, 12, 59, tzinfo=timezone.utc)       # 8:59 AM ET
TUE_9_00 = datetime(2026, 10, 6, 13, 0, tzinfo=timezone.utc)        # 9:00 AM ET
SAT_NOON = datetime(2026, 10, 10, 16, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("moment,inside", [(TUE_NOON, True), (TUE_9_00, True), (TUE_8_59, False), (TUE_7_14, True),
                                           (TUE_7_15, False), (SAT_NOON, False)])
def test_reply_hours_boundaries(moment, inside):
    assert in_reply_hours(moment) is inside


def test_a_rate_question_outside_hours_says_it_waits_for_morning_and_inside_hours_does_not():
    assert "first thing next business morning" in format_slack("rate_terms_handoff", event(), now=SAT_NOON)
    assert "first thing next business morning" not in format_slack("rate_terms_handoff", event(), now=TUE_NOON)
    assert "first thing next business morning" in format_slack("reschedule_request", event(), now=TUE_7_15)


def test_the_quoted_number_alert_is_not_held_for_business_hours():
    alert = format_slack("ai_quoted_numbers", event(direction="outbound", body="Rates start at 9%"), now=SAT_NOON)
    assert "first thing next business morning" not in alert
