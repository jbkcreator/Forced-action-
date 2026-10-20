"""Split T-06: after-hours voicemail / answering-service intake -> logged call + pin; 08:55 ET reply drafts."""
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
from src.lending.after_hours_intake import (MorningReply, PinnedCall, draft_morning_replies, fallback_reply,
                                            model_drafter, parse_intake, pins_to_call, record_intake, reply_body)
from src.lending.ghl_consent_events import InvalidEvent

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
PHONE = "+18135550199"
NOW = datetime(2026, 10, 21, 1, 15, tzinfo=timezone.utc)  # 9:15 pm ET
MORNING = NOW + timedelta(hours=11, minutes=40)  # 8:55 am ET
BODY = {"phone": "(813) 555-0199", "first_name": "Mike Rivera", "call_id": "vm-1",
        "message": "Hi, it's Mike. I have a duplex in Tampa under contract, closing on the 30th. Call me back.",
        "received_at": "2026-10-21T01:10:00Z"}


@pytest.fixture
def db(lending_db):
    apply_pins(lending_db.connection())
    lending_db.execute(text("DELETE FROM lending.contacts WHERE phone = :p"), {"p": PHONE})
    lending_db.execute(text("DELETE FROM lending.text_consents WHERE phone = :p"), {"p": PHONE})
    return lending_db


def _consent(db) -> None:
    db.execute(text("INSERT INTO lending.text_consents (phone, source) VALUES (:p, 'web_form')"), {"p": PHONE})


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
    assert (event.phone, event.first_name, event.message, event.ref) == (PHONE, "Mike", "hello", "m-9")
    assert event.received_at == NOW and event.call_key == "after_hours:ghl_voicemail:m-9"


def test_parse_rejects_a_missing_phone_and_a_bad_time():
    with pytest.raises(InvalidEvent):
        parse_intake({"message": "hi"}, now=NOW)
    with pytest.raises(InvalidEvent):
        parse_intake({"phone": "8135550199", "received_at": "last night"}, now=NOW)


def test_unknown_caller_gets_a_contact_an_inbound_call_row_and_a_pin(db):
    assert record_intake(db, BODY, now=NOW)
    call = db.execute(text("SELECT direction, disposition, disposition_raw FROM lending.call_dispositions "
                           "WHERE dialer_call_id = 'after_hours:ghl_voicemail:vm-1'")).one()
    assert tuple(call) == ("inbound", None, "after_hours_voicemail")
    pin = db.execute(text("SELECT pinned_at, pinned_until FROM lending.contacts WHERE phone = :p"), {"p": PHONE}).one()
    assert pin.pinned_at == NOW and pin.pinned_until == NOW + timedelta(hours=24)


def test_a_redelivered_event_changes_nothing(db):
    record_intake(db, BODY, now=NOW)
    assert not record_intake(db, BODY, now=NOW + timedelta(minutes=1))
    pin = db.execute(text("SELECT pinned_at FROM lending.contacts WHERE phone = :p"), {"p": PHONE}).scalar()
    assert pin == NOW and len(pins_to_call(db, MORNING)) == 1


def test_pin_ends_at_the_first_outbound_attempt_or_after_24_hours(db):
    record_intake(db, BODY, now=NOW)
    [pin] = pins_to_call(db, MORNING)
    assert pin.event.first_name == "Mike" and pin.event.message == BODY["message"]
    assert pins_to_call(db, NOW + timedelta(hours=25)) == []
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_ended_at, raw_event) "
                    "VALUES ('cdr-1', 'outbound', :p, :at, '{}')"), {"p": PHONE, "at": MORNING + timedelta(minutes=10)})
    assert pins_to_call(db, MORNING + timedelta(minutes=20)) == []


def test_a_do_not_contact_caller_is_logged_but_never_listed(db):
    db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
    assert record_intake(db, BODY, now=NOW)
    assert pins_to_call(db, MORNING) == []


def test_morning_drafts_a_reply_only_for_a_caller_with_text_consent(db):
    record_intake(db, BODY, now=NOW)
    pins = pins_to_call(db, MORNING)
    queue = FakeQueue()
    assert [r.action_id for r in draft_morning_replies(db, pins, queue, _drafter("hi"))] == [None]
    _consent(db)
    reply = "Hi Mike, we got your message about the Tampa duplex. We will call you back this morning."
    [result] = draft_morning_replies(db, pins, queue, _drafter(reply))
    call = queue.calls[0]
    assert result.action_id == 1 and call["tool_name"] == DRAFT_TOOL_NAME and call["channel"] == "ghl_sms"
    assert call["payload"] == {"lead_ref": pins[0].lead_ref, "to_phone": PHONE, "body": reply}
    assert call["idempotency_key"] == "after_hours:ghl_voicemail:vm-1" and call["recipient_phone"] == PHONE


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


def test_morning_post_shows_status_and_last_four_only():
    from src.tasks.lending_pinned_callbacks import format_slack
    pin = PinnedCall(12, parse_intake(BODY, now=NOW))
    message = format_slack([MorningReply(pin, 7), MorningReply(pin, None)])
    assert "Call these first (2)" in message and "…0199" in message and "8135550199" not in message
    assert "waiting for Approve" in message and "call only" in message
    assert format_slack([]) is None


@pytest.fixture
def client(db, monkeypatch):
    from config.settings import get_settings
    from src.api import lending_ghl_router
    from src.api.deps import get_db
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(settings, "lending_after_hours_intake_enabled", True, raising=False)
    app = FastAPI()
    app.include_router(lending_ghl_router.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_endpoint_records_once(client):
    r = client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY)
    assert r.status_code == 200 and r.json() == {"recorded": True}
    assert client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY).json() == {"recorded": False}


def test_endpoint_rejects_a_bad_secret_and_a_bad_body_and_is_closed_while_off(client, monkeypatch):
    assert client.post("/webhooks/lending/after-hours-intake", json=BODY).status_code == 401
    assert client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json={"message": "x"}).status_code == 422
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_after_hours_intake_enabled", False, raising=False)
    assert client.post("/webhooks/lending/after-hours-intake", headers=HEADERS, json=BODY).status_code == 503
