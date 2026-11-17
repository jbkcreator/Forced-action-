"""A cold lead that later comes in through the website form or LendingFlow is warm again: the do-not-disturb the
cold push set is lifted (SMS only with text consent) and the ndl-cold tag removed, but a genuine opt-out always
wins. No network, no database."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from config.lending_compliance import COLD_LEAD_DND_MESSAGE, GHL_TAG_COLD_LEAD
from src.lending import cold_leads
from src.lending.cold_leads import SendChannel, cold_send_decision, reconcile_warm_contact, warm_dnd_reconciliation
from src.lending.ghl_email import GhlEmailSender
from src.lending.ghl_sms import GhlAccount, GhlSmsSender

ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")
PHONE = "+18135558601"
MESSAGING = ("SMS", "Email", "GMB", "FB", "WhatsApp")


class LeadState:
    """One phone's history. ``inbound``: it came in through the form or LendingFlow; ``loaded``: it was loaded into
    the dialer cold; ``opted_out``: suppression list, STOP or do-not-contact; ``consent``: recorded text consent."""

    def __init__(self, *, loaded=True, inbound=True, opted_out=False, consent=False, fail=False, opted_out_email=None):
        self.loaded, self.inbound, self.opted_out, self.consent, self.fail = loaded, inbound, opted_out, consent, fail
        self.opted_out_email = opted_out_email  # an email-only opt-out (suppression row with no phone)

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        if self.fail:
            raise RuntimeError("connection lost")
        sql = str(statement)
        if "warm_state" in sql:  # 0 = leave as is, 1 = lift messaging except SMS, 2 = lift every messaging channel
            email_out = self.opted_out_email is not None and (params or {}).get("e") == self.opted_out_email
            if not (self.loaded and self.inbound) or self.opted_out or email_out:
                answer = 0
            else:
                answer = 2 if self.consent else 1
        elif "dialer_load_records" in sql:  # the cold check: loaded and not inbound
            answer = self.loaded and not self.inbound
        else:
            raise AssertionError(sql)
        return SimpleNamespace(scalar=lambda: answer)


@pytest.fixture
def state(monkeypatch):
    def install(**kwargs) -> LeadState:
        db = LeadState(**kwargs)
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


def _lift_then_send(dnd):
    """upsert -> read the contact's do-not-disturb -> lift -> send."""
    return Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(200, {"contact": {"id": "ct1", "dndSettings": dnd}}),
                    Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))


# Which channels may be lifted

def test_a_warm_lead_first_reached_cold_gets_every_messaging_channel_back_with_text_consent(state):
    db = state(consent=True)
    assert warm_dnd_reconciliation(db, PHONE, text_consent=False) == MESSAGING


def test_without_text_consent_sms_stays_blocked_and_the_other_channels_are_lifted(state):
    db = state(consent=False)
    assert warm_dnd_reconciliation(db, PHONE, text_consent=False) == ("Email", "GMB", "FB", "WhatsApp")


def test_consent_given_on_the_inbound_form_counts_for_sms(state):
    db = state(consent=False)
    assert "SMS" in warm_dnd_reconciliation(db, PHONE, text_consent=True)


@pytest.mark.parametrize("kwargs", [dict(opted_out=True, consent=True), dict(loaded=False), dict(inbound=False)])
def test_an_opt_out_a_number_never_loaded_cold_or_a_still_cold_lead_gets_nothing_lifted(state, kwargs):
    db = state(**kwargs)
    assert warm_dnd_reconciliation(db, PHONE, text_consent=True) == ()


# The senders, at send time

def test_sms_to_a_warmed_up_lead_with_consent_lifts_the_sms_do_not_disturb(state):
    state(consent=True)
    request = _lift_then_send(_cold_dnd())
    assert GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam") == "m1"
    assert "dndSettings" not in request.calls[0][2]["json"]
    assert request.calls[1][0] == "GET" and request.calls[1][1].endswith("/contacts/ct1")
    assert request.calls[2][2]["json"]["dndSettings"] == {"SMS": {"status": "inactive"}}


def test_sms_to_a_warmed_up_lead_that_opted_out_lifts_nothing(state):
    state(consent=True, opted_out=True)
    assert cold_send_decision(PHONE, SendChannel.SMS).lift_dnd == ()


def test_email_to_a_warmed_up_lead_lifts_the_email_do_not_disturb(state):
    state(consent=False)
    request = _lift_then_send(_cold_dnd())
    assert GhlEmailSender(ACCOUNT, "hello@nextdeallending.com", request=request)(
        "jane@example.com", "s", "b", phone=PHONE) == "m1"
    assert request.calls[2][2]["json"]["dndSettings"] == {"Email": {"status": "inactive"}}


def test_a_lead_never_loaded_cold_sends_without_touching_do_not_disturb(state):
    state(loaded=False)
    request = _sent_ok()
    GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert "dndSettings" not in request.calls[0][2]["json"]


# The inbound push: reconcile the GHL contact

class FakeCall:
    def __init__(self, dnd, fail_on=None):
        self.calls, self._dnd, self._fail_on = [], dnd, fail_on

    def __call__(self, step, method, path, **kwargs):
        self.calls.append((step, method, path, kwargs.get("json")))
        if self._fail_on == method:
            raise RuntimeError("HTTP 500")
        if method == "GET":
            return {"contact": {"id": "ct1", "dndSettings": self._dnd, "tags": [GHL_TAG_COLD_LEAD]}}
        return {"contact": {"id": "ct1"}}


def _cold_dnd(**overrides):
    dnd = {c: {"status": "active", "message": COLD_LEAD_DND_MESSAGE} for c in MESSAGING}
    dnd.update(overrides)
    return dnd


def test_a_consented_inbound_push_lifts_our_cold_do_not_disturb_and_removes_the_cold_tag(state):
    state(consent=False)
    call = FakeCall(_cold_dnd())
    reconcile_warm_contact(call, ACCOUNT.location_id, "ct1", PHONE, text_consent=True, log_prefix="[t]")
    lift = next(c for c in call.calls if c[1] == "POST")
    assert lift[2] == "/contacts/upsert"
    assert lift[3]["dndSettings"] == {c: {"status": "inactive"} for c in MESSAGING}
    assert ("remove cold tag", "DELETE", "/contacts/ct1/tags", {"tags": [GHL_TAG_COLD_LEAD]}) in call.calls


def test_a_do_not_disturb_set_by_an_opt_out_is_never_lifted(state):
    state(consent=True)
    call = FakeCall(_cold_dnd(SMS={"status": "active", "message": "Lending opt-out"}))
    reconcile_warm_contact(call, ACCOUNT.location_id, "ct1", PHONE, text_consent=True, log_prefix="[t]")
    lift = next(c for c in call.calls if c[1] == "POST")
    assert "SMS" not in lift[3]["dndSettings"] and "Email" in lift[3]["dndSettings"]


def test_an_opted_out_or_never_cold_lead_is_not_touched_in_ghl(state):
    state(opted_out=True, consent=True)
    call = FakeCall(_cold_dnd())
    reconcile_warm_contact(call, ACCOUNT.location_id, "ct1", PHONE, text_consent=True, log_prefix="[t]")
    assert call.calls == []


def test_a_failed_ghl_call_never_fails_the_push(state, caplog):
    state(consent=True)
    reconcile_warm_contact(FakeCall(_cold_dnd(), fail_on="GET"), ACCOUNT.location_id, "ct1", PHONE,
                           text_consent=True, log_prefix="[t]")
    assert "skipped" in caplog.text and PHONE not in caplog.text


# End to end through the real sinks: cold import, then a consented inbound submission

def _ghl(monkeypatch, dnd):
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs.get("json")))
        if method == "GET" and "/contacts/ct1" in url:
            return Resp(200, {"contact": {"id": "ct1", "dndSettings": dnd, "tags": [GHL_TAG_COLD_LEAD]}})
        return Resp(200, {"contact": {"id": "ct1"}, "new": False, "opportunities": []})

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", request)
    return calls


@pytest.fixture
def quiet_settings(monkeypatch):
    from config.settings import get_settings
    settings = get_settings()
    for field in ("lending_ghl_pipeline_id", "lending_ghl_stage_new_lead", "lending_ghl_cf_intro_name"):
        monkeypatch.setattr(settings, field, None, raising=False)


def test_web_form_after_a_cold_import_reopens_messaging(state, monkeypatch, quiet_settings):
    from src.lending.web_lead_ghl import GhlLeadSink
    state(consent=False)
    calls = _ghl(monkeypatch, _cold_dnd())
    lead = {"name": "Sam Lee", "phone": PHONE, "email": None, "sms_consent": True, "call_consent": True,
            "deal_drop_optin": False, "suppressed": False}
    try:
        GhlLeadSink(ACCOUNT).push(lead)
    except Exception:
        pass  # later steps (card, note) may need more fakes; the reconciliation runs right after the tags
    lifts = [c for c in calls if c[0] == "POST" and c[1].endswith("/contacts/upsert") and c[2].get("dndSettings")]
    assert lifts and lifts[0][2]["dndSettings"]["SMS"] == {"status": "inactive"}
    assert any(c[0] == "DELETE" and c[2] == {"tags": [GHL_TAG_COLD_LEAD]} for c in calls)


def test_lendingflow_lead_after_a_cold_import_reopens_messaging(state, monkeypatch, quiet_settings):
    from src.lending.lendingflow_ghl import LendingFlowGhlSink
    state(consent=False)
    calls = _ghl(monkeypatch, _cold_dnd())
    lead = {"phone": PHONE, "first_name": "Sam", "consent_status": "present", "vendor_lead_id": "LF-1"}
    try:
        LendingFlowGhlSink(ACCOUNT).push(lead)
    except Exception:
        pass
    lifts = [c for c in calls if c[0] == "POST" and c[1].endswith("/contacts/upsert") and c[2].get("dndSettings")]
    assert lifts and "SMS" in lifts[0][2]["dndSettings"]
    assert any(c[0] == "DELETE" and c[2] == {"tags": [GHL_TAG_COLD_LEAD]} for c in calls)


# Finding 1: an opt-out made in GHL that has not reached our suppression list yet

@pytest.mark.parametrize("sender, args, channel", [
    ("sms", (PHONE, "hello", "Sam"), "SMS"),
    ("email", ("jane@example.com", "s", "b"), "Email"),
])
def test_a_genuine_ghl_do_not_disturb_awaiting_local_sync_is_never_lifted_and_nothing_is_sent(
        state, sender, args, channel):
    from src.lending.ghl_sms import GhlSendRefused
    state(consent=True)
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}),
                       Resp(200, {"contact": {"id": "ct1", "dndSettings": _cold_dnd(**{
                           channel: {"status": "active", "message": "Opted out by reply"}})}}))
    send = (GhlSmsSender(ACCOUNT, "+18135550100", request=request) if sender == "sms"
            else GhlEmailSender(ACCOUNT, "hello@nextdeallending.com", request=request))
    kwargs = {} if sender == "sms" else {"phone": PHONE}
    with pytest.raises(GhlSendRefused, match="ghl_dnd_not_cold_push"):
        send(*args, **kwargs)
    assert [c[0] for c in request.calls] == ["POST", "GET"]  # no lift, no send
    assert not any("dndSettings" in (c[2].get("json") or {}) for c in request.calls)


def test_an_unreadable_ghl_contact_refuses_the_send(state):
    from src.lending.ghl_sms import GhlSendRefused
    state(consent=True)
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(500))
    with pytest.raises(GhlSendRefused, match="dnd_ownership_unreadable"):
        GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam")
    assert len(request.calls) == 2


def test_a_channel_without_active_do_not_disturb_sends_with_no_lift(state):
    state(consent=True)
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(200, {"contact": {"id": "ct1", "dndSettings": {}}}),
                       Resp(201, {"messageId": "m1"}))
    assert GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam") == "m1"
    assert [c[0] for c in request.calls] == ["POST", "GET", "POST"]


# Finding 2: an email-only opt-out

def test_an_email_only_opt_out_gets_nothing_lifted(state):
    db = state(consent=True, opted_out_email="jane@example.com")
    assert warm_dnd_reconciliation(db, PHONE, text_consent=True, email="Jane@Example.com ") == ()
    assert warm_dnd_reconciliation(db, PHONE, text_consent=True, email=None) == MESSAGING


def test_reconciliation_checks_the_email_as_well_as_the_phone(state):
    state(consent=True, opted_out_email="jane@example.com")
    call = FakeCall(_cold_dnd())
    reconcile_warm_contact(call, ACCOUNT.location_id, "ct1", PHONE, text_consent=True, log_prefix="[t]",
                           email="jane@example.com")
    assert call.calls == []


def test_an_email_suppressed_contact_submitting_the_website_form_has_nothing_lifted(
        state, monkeypatch, quiet_settings):
    from src.lending.web_lead_ghl import GhlLeadSink
    state(consent=True, opted_out_email="jane@example.com")
    calls = _ghl(monkeypatch, _cold_dnd())
    lead = {"name": "Jane Roe", "phone": PHONE, "email": "jane@example.com", "sms_consent": True,
            "call_consent": True, "deal_drop_optin": False, "suppressed": True}
    try:
        GhlLeadSink(ACCOUNT).push(lead)
    except Exception:
        pass
    assert not [c for c in calls if (c[2] or {}).get("dndSettings")]
    assert not any(c[0] == "DELETE" and c[2] == {"tags": [GHL_TAG_COLD_LEAD]} for c in calls)
