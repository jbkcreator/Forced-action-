"""Split T-06: after-hours voicemail / answering-service intake -> logged call, pin, reply draft for Approve."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from config.lending_after_hours import DRAFT_TOOL_NAME
from migrations.apply_lending_contact_pins import apply_to as apply_pins
from src.lending.after_hours_intake import (active_pins, fallback_reply, model_drafter, parse_intake,
                                            queue_reply_draft, record_intake, reply_body)
from src.lending.ghl_consent_events import InvalidEvent

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
NOW = datetime(2026, 10, 21, 1, 15, tzinfo=timezone.utc)  # 9:15 pm ET
BODY = {"phone": "(813) 555-0199", "first_name": "Mike Rivera", "call_id": "vm-1",
        "message": "Hi, it's Mike. I have a duplex in Tampa under contract, closing on the 30th. Call me back.",
        "received_at": "2026-10-21T01:10:00Z"}


@pytest.fixture
def db(lending_db):
    apply_pins(lending_db.connection())
    lending_db.execute(text("DELETE FROM lending.contacts WHERE phone = '+18135550199'"))
    return lending_db


class FakeQueue:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def enqueue(self, **kwargs: Any) -> int:
        self.calls.append(kwargs)
        return len(self.calls)


def _drafter(reply):
    return lambda first_name, transcript: reply


def test_parse_reads_ghl_contact_fields_and_keeps_first_name():
    event = parse_intake({"contact": {"phone": "8135550199", "name": "Mike Rivera"}, "transcription": "hello",
                          "messageId": "m-9"}, now=NOW)
    assert (event.phone, event.first_name, event.message, event.ref) == ("+18135550199", "Mike", "hello", "m-9")
    assert event.received_at == NOW and event.call_key == "after_hours:ghl_voicemail:m-9"


def test_parse_rejects_a_missing_phone_and_a_bad_time():
    with pytest.raises(InvalidEvent):
        parse_intake({"message": "hi"}, now=NOW)
    with pytest.raises(InvalidEvent):
        parse_intake({"phone": "8135550199", "received_at": "last night"}, now=NOW)


def test_unknown_caller_gets_a_contact_a_logged_call_and_a_pin(db):
    event, result = record_intake(db, BODY, now=NOW)
    assert result.recorded and result.draft_allowed and result.contact_id
    call = db.execute(text("SELECT direction, disposition, campaign_tag, phone FROM lending.call_dispositions "
                           "WHERE dialer_call_id = :k"), {"k": event.call_key}).one()
    assert tuple(call) == ("inbound", "LEFT_VOICEMAIL", "inbound_voicemail", "+18135550199")
    pin = db.execute(text("SELECT pinned_at, pinned_until FROM lending.contacts WHERE id = :id"),
                     {"id": result.contact_id}).one()
    assert pin.pinned_at == NOW and pin.pinned_until == NOW + timedelta(hours=24)


def test_a_redelivered_event_logs_nothing_and_drafts_nothing(db):
    record_intake(db, BODY, now=NOW)
    event, result = record_intake(db, BODY, now=NOW + timedelta(minutes=1))
    assert not result.recorded and not result.draft_allowed
    assert queue_reply_draft(FakeQueue(), event, result, _drafter("hi")) is None


def test_a_do_not_contact_caller_is_logged_and_pinned_but_gets_no_draft(db):
    db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES ('+18135550199', true)"))
    _, result = record_intake(db, BODY, now=NOW)
    assert result.recorded and not result.draft_allowed


def test_pin_ends_at_the_first_outbound_attempt_or_after_24_hours(db):
    _, result = record_intake(db, BODY, now=NOW)
    assert [row["id"] for row in active_pins(db, NOW + timedelta(hours=8))] == [result.contact_id]
    assert active_pins(db, NOW + timedelta(hours=25)) == []
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_ended_at, raw_event) "
                    "VALUES ('cdr-1', 'outbound', '+18135550199', :at, '{}')"), {"at": NOW + timedelta(hours=8)})
    assert active_pins(db, NOW + timedelta(hours=9)) == []


def test_the_draft_is_queued_for_approval_with_the_model_reply(db):
    event, result = record_intake(db, BODY, now=NOW)
    queue = FakeQueue()
    reply = "Hi Mike, we got your message about the Tampa duplex. We will call you back this morning."
    assert queue_reply_draft(queue, event, result, _drafter(reply)) == 1
    call = queue.calls[0]
    assert call["tool_name"] == DRAFT_TOOL_NAME and call["channel"] == "ghl_sms"
    assert call["payload"] == {"lead_ref": f"contact:{result.contact_id}", "to_phone": "+18135550199", "body": reply}
    assert call["idempotency_key"] == event.call_key and call["recipient_phone"] == "+18135550199"


@pytest.mark.parametrize("drafted", [None, "", "x" * 400, "Hi Mike, rates start at 9.5% for your duplex."])
def test_unusable_model_replies_fall_back_to_the_fixed_text(drafted):
    event = parse_intake(BODY, now=NOW)
    assert reply_body(event, _drafter(drafted)) == fallback_reply("Mike")


def test_model_drafter_skips_the_call_without_a_transcript_and_survives_a_failure():
    class Boom:
        def create(self, **_):
            raise RuntimeError("down")
    assert model_drafter(Boom(), "m")("Mike", None) is None
    assert model_drafter(Boom(), "m")("Mike", "call me") is None


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api import lending_ghl_router
    from src.api.deps import get_db
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(settings, "lending_after_hours_intake_enabled", True, raising=False)
    queued: list = []
    monkeypatch.setattr(lending_ghl_router, "_queue_after_hours_draft", lambda event, result: queued.append(event))
    app = FastAPI()
    app.include_router(lending_ghl_router.router)
    app.dependency_overrides[get_db] = lambda: db
    test_client = TestClient(app)
    test_client.queued = queued
    return test_client


def test_endpoint_records_and_queues_the_draft_in_the_background(client):
    r = client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY)
    assert r.status_code == 200 and r.json() == {"recorded": True, "pinned": True, "draft_queued": True}
    assert len(client.queued) == 1
    again = client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY)
    assert again.json()["recorded"] is False and len(client.queued) == 1


def test_endpoint_rejects_a_bad_secret_and_a_bad_body(client):
    assert client.post("/webhooks/lending/after-hours-intake", json=BODY).status_code == 401
    assert client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json={"message": "x"}).status_code == 422


def test_endpoint_is_closed_while_switched_off(client, monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_after_hours_intake_enabled", False, raising=False)
    assert client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY).status_code == 503


def test_morning_post_lists_pins_with_last_four_only(db):
    from src.tasks.lending_pinned_callbacks import format_slack
    record_intake(db, BODY, now=NOW)
    message = format_slack(active_pins(db, NOW + timedelta(hours=11)))
    assert "Call these first (1)" in message and "…0199" in message and "8135550199" not in message
    assert format_slack([]) is None
