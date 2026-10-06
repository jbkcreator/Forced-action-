"""GHL email sender for booking reminders, and the worker's email outcomes. No network, no database."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config.lending_reminders import MAX_SEND_ATTEMPTS
from config.settings import get_settings
from src.lending import reminder_worker
from src.lending.ghl_email import GhlEmailSender, get_email_sender
from src.lending.ghl_sms import GhlAccount, GhlSmsError

ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")
FROM = "hello@nextdeallending.com"
TO = "jane@example.com"
PHONE = "+18135558601"


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class Recorder:
    def __init__(self, *responses):
        self.calls, self._responses = [], list(responses)

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self._responses.pop(0)


def test_upserts_by_email_and_phone_then_sends_an_html_email_from_the_verified_address():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    message_id = GhlEmailSender(ACCOUNT, FROM, request=request)(TO, "Your call", "Hi Jane,\nSee you <soon>.",
                                                                phone=PHONE, first_name="Jane")
    assert message_id == "m1"
    (_, u1, k1), (_, u2, k2) = request.calls
    assert u1.endswith("/contacts/upsert")
    assert k1["json"] == {"locationId": "loc-ndl", "email": TO, "phone": PHONE, "firstName": "Jane"}
    assert u2.endswith("/conversations/messages")
    assert k2["json"] == {"type": "Email", "contactId": "ct1", "subject": "Your call",
                          "html": "Hi Jane,<br>See you &lt;soon&gt;.", "emailFrom": FROM}
    assert k2["headers"]["Version"] == "2021-04-15"


def test_upsert_without_phone_or_name_sends_email_only():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    GhlEmailSender(ACCOUNT, FROM, request=request)(TO, "s", "b")
    assert request.calls[0][2]["json"] == {"locationId": "loc-ndl", "email": TO}


@pytest.mark.parametrize("responses,fragment,ambiguous", [
    ([Resp(422, {"message": f"bad {TO}"})], "contact upsert failed: HTTP 422", False),
    ([Resp(200, {"contact": {}})], "contact upsert returned no contact id", False),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(400, {"message": f"no {TO}"})], "email send failed: HTTP 400", False),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(502, {})], "email send failed: HTTP 502", True),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {})], "email send returned no message id", True),
])
def test_failures_say_whether_anything_may_have_gone_out(responses, fragment, ambiguous):
    with pytest.raises(GhlSmsError) as err:
        GhlEmailSender(ACCOUNT, FROM, request=Recorder(*responses))(TO, "s", "b")
    assert fragment in str(err.value) and TO not in str(err.value)
    assert err.value.ambiguous is ambiguous


def test_nothing_is_sent_after_the_deadline():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}))
    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    with pytest.raises(GhlSmsError) as err:
        GhlEmailSender(ACCOUNT, FROM, request=request)(TO, "s", "b", deadline=past)
    assert not err.value.ambiguous and len(request.calls) == 1


def test_no_sender_without_a_sending_address(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(s, "lending_ghl_email_from", None, raising=False)
    monkeypatch.setattr("src.lending.ghl_email.lending_ghl_account", lambda: ACCOUNT)
    assert get_email_sender() is None


def test_no_sender_without_a_ghl_account(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(s, "lending_ghl_email_from", FROM, raising=False)
    monkeypatch.setattr("src.lending.ghl_email.lending_ghl_account", lambda: None)
    assert get_email_sender() is None


def test_sender_built_when_both_are_set(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(s, "lending_ghl_email_from", f"  {FROM} ", raising=False)
    monkeypatch.setattr("src.lending.ghl_email.lending_ghl_account", lambda: ACCOUNT)
    assert isinstance(get_email_sender(), GhlEmailSender)


SLOT = datetime(2026, 10, 7, 14, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 5, 16, 0, tzinfo=timezone.utc)


def _row(attempts=0):
    return {"id": 9, "kind": "confirmation", "first_name": "Jane", "contact_phone": PHONE, "contact_email": TO,
            "property_address": "412 Oak Ave", "slot_start_utc": SLOT, "attempts": attempts, "send_at": NOW}


@pytest.fixture
def writes(monkeypatch):
    recorded = []
    monkeypatch.setattr(reminder_worker, "_record", lambda db, rid, status, reason=None, channel=None, message_id=None:
                        recorded.append(("record", status, reason, channel, message_id)))
    monkeypatch.setattr(reminder_worker, "_requeue", lambda db, row, send_at, reason, *, count_attempt:
                        recorded.append(("requeue", reason, count_attempt)))
    return recorded


def _send(sender, row):
    return reminder_worker._send_email(None, row, now=NOW, sender=sender, enabled=True, number="+18135550100",
                                       started=[False])


def test_email_sent_records_the_provider_message_id(writes):
    calls = []

    def sender(to, subject, body, *, phone, first_name, deadline):
        calls.append((to, phone, first_name, deadline))
        return "m1"
    assert _send(sender, _row()) == "sent"
    assert calls == [(TO, PHONE, "Jane", SLOT)]
    assert writes == [("record", "sent", None, "email", "m1")]


def test_ambiguous_email_send_is_never_resent(writes):
    def sender(*a, **k):
        raise GhlSmsError("timeout", ambiguous=True)
    assert _send(sender, _row()) == "send_unknown"
    assert writes == [("record", "send_unknown", "ambiguous_send_error", "email", None)]


def test_rejected_email_is_retried_then_failed(writes):
    def sender(*a, **k):
        raise GhlSmsError("HTTP 400")
    assert _send(sender, _row(attempts=0)) == "retry"
    assert _send(sender, _row(attempts=MAX_SEND_ATTEMPTS)) == "failed"
    assert writes == [("requeue", "send_failed_retry", True), ("record", "failed", "send_failed", "email", None)]
