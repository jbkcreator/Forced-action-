"""Cold leads: never emailed, texted only after text consent. Checked inside the GHL senders. No network, no database."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.lending import cold_leads
from src.lending.cold_leads import ColdSendBlock, SendChannel, cold_send_decision
from src.lending.ghl_email import GhlEmailSender
from src.lending.ghl_sms import GhlAccount, GhlSendRefused, GhlSmsSender

ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")
PHONE = "+18135558601"
TO = "jane@example.com"


class FakeLeadDb:
    """Answers the cold-lead query and the text-consent query; records the phone each one was asked about."""

    def __init__(self, *, cold: bool, consent: bool = False, fail: bool = False) -> None:
        self.cold, self.consent, self.fail = cold, consent, fail
        self.asked: list[str] = []

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        if self.fail:
            raise RuntimeError(f"connection lost {PHONE}")
        sql = str(statement)
        self.asked.append(params["p"])
        if "call_dispositions" in sql and "text_consents" not in sql:  # the "said not interested" lookup
            return SimpleNamespace(scalar=lambda: False)
        answer = self.cold if "dialer_load_records" in sql else self.consent if "text_consents" in sql else None
        assert answer is not None, sql
        return SimpleNamespace(scalar=lambda: answer)


@pytest.fixture
def lead_db(monkeypatch):
    def install(**kwargs) -> FakeLeadDb:
        db = FakeLeadDb(**kwargs)
        monkeypatch.setattr(cold_leads, "_session_factory", db)
        return db
    return install


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        return self._body or {}


class Recorder:
    def __init__(self, *responses):
        self.calls, self._responses = [], list(responses)

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self._responses.pop(0)


def _sent_ok():
    return Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))


# The decision

def test_a_warm_or_unknown_number_is_allowed_on_every_channel(lead_db):
    lead_db(cold=False)
    for channel in SendChannel:
        assert cold_send_decision(PHONE, channel) == cold_leads.ColdSendDecision()


def test_a_cold_lead_without_text_consent_gets_no_text(lead_db):
    lead_db(cold=True, consent=False)
    decision = cold_send_decision(PHONE, SendChannel.SMS)
    assert decision.block is ColdSendBlock.COLD_LEAD_NO_TEXT_CONSENT


def test_a_cold_lead_with_text_consent_may_be_texted_and_is_marked_cold(lead_db):
    lead_db(cold=True, consent=True)
    assert cold_send_decision(PHONE, SendChannel.SMS) == cold_leads.ColdSendDecision(cold=True, lift_dnd=("SMS",))


def test_a_cold_lead_is_never_emailed_even_with_text_consent(lead_db):
    lead_db(cold=True, consent=True)
    assert cold_send_decision(PHONE, SendChannel.EMAIL).block is ColdSendBlock.COLD_LEAD_NO_EMAIL


def test_the_lookup_uses_the_normalized_phone(lead_db):
    db = lead_db(cold=True, consent=True)
    cold_send_decision("(813) 555-8601", SendChannel.SMS)
    assert set(db.asked) == {PHONE}


def test_an_unreadable_lookup_refuses_the_send_without_logging_the_phone(lead_db, caplog):
    lead_db(cold=False, fail=True)
    decision = cold_send_decision(PHONE, SendChannel.SMS)
    assert decision.block is ColdSendBlock.COLD_CHECK_UNAVAILABLE
    assert PHONE not in caplog.text


def test_an_email_without_a_phone_cannot_be_matched_and_is_allowed(lead_db):
    db = lead_db(cold=True)
    assert cold_send_decision(None, SendChannel.EMAIL).block is None
    assert db.asked == []


# The SMS sender

def test_sms_to_a_cold_lead_without_consent_is_refused_before_anything_reaches_ghl(lead_db):
    lead_db(cold=True, consent=False)
    request = _sent_ok()
    with pytest.raises(GhlSendRefused) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam")
    assert request.calls == []
    assert err.value.reason == "cold_lead_no_text_consent" and err.value.ambiguous is False
    assert PHONE not in str(err.value)


def test_sms_to_a_consented_cold_lead_lifts_only_the_sms_do_not_disturb(lead_db):
    lead_db(cold=True, consent=True)
    from config.lending_compliance import COLD_LEAD_DND_MESSAGE
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}),
                       Resp(200, {"contact": {"id": "ct1", "dndSettings": {
                           "SMS": {"status": "active", "message": COLD_LEAD_DND_MESSAGE}}}}),
                       Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    assert GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam") == "m1"
    assert "dndSettings" not in request.calls[0][2]["json"]
    assert request.calls[2][2]["json"]["dndSettings"] == {"SMS": {"status": "inactive"}}


def test_sms_to_a_warm_lead_leaves_do_not_disturb_untouched(lead_db):
    lead_db(cold=False)
    request = _sent_ok()
    GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert "dndSettings" not in request.calls[0][2]["json"]


def test_internal_alerts_to_the_team_phone_skip_the_cold_check(lead_db):
    db = lead_db(cold=True, consent=False)
    request = _sent_ok()
    assert GhlSmsSender(ACCOUNT, "+18135550100", request=request, internal_alert=True)(PHONE, "alarm") == "m1"
    assert db.asked == []


# The email sender

def test_email_to_a_cold_lead_is_refused_before_anything_reaches_ghl(lead_db):
    lead_db(cold=True, consent=True)
    request = _sent_ok()
    with pytest.raises(GhlSendRefused) as err:
        GhlEmailSender(ACCOUNT, "hello@nextdeallending.com", request=request)(TO, "s", "b", phone=PHONE)
    assert request.calls == []
    assert err.value.reason == "cold_lead_no_email"


def test_email_to_a_warm_lead_is_sent(lead_db):
    lead_db(cold=False)
    request = _sent_ok()
    assert GhlEmailSender(ACCOUNT, "hello@nextdeallending.com", request=request)(TO, "s", "b", phone=PHONE) == "m1"
