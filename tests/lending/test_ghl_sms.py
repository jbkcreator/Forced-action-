"""WP-GL-9 GHL sender: request shapes and error handling, with no network."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
import requests
from pydantic import SecretStr

from config.settings import get_settings
from src.lending import ghl_sms
from src.lending.ghl_sms import GhlAccount, GhlSmsError, GhlSmsSender, get_sender, lending_ghl_account, texting_number

PHONE = "+18135558601"
ACCOUNT = GhlAccount(api_key="k-secret", location_id="loc-ndl")


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


def test_sender_upserts_the_contact_then_sends_an_sms_from_the_texting_number():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1", "conversationId": "cv1"}))
    message_id = GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", "Sam")
    assert message_id == "m1"
    (m1, u1, k1), (m2, u2, k2) = request.calls
    assert (m1, u1.endswith("/contacts/upsert")) == ("POST", True)
    assert k1["json"] == {"locationId": "loc-ndl", "phone": PHONE, "firstName": "Sam"}
    assert (m2, u2.endswith("/conversations/messages")) == ("POST", True)
    assert k2["json"] == {"type": "SMS", "contactId": "ct1", "message": "hello", "fromNumber": "+18135550100"}
    assert k2["headers"]["Authorization"] == "Bearer k-secret"


def test_upsert_omits_first_name_when_unknown():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert "firstName" not in request.calls[0][2]["json"]


@pytest.mark.parametrize("responses,fragment", [
    ([Resp(422, {"message": f"bad {PHONE}"})], "contact upsert failed: HTTP 422"),
    ([Resp(200, {"contact": {}})], "contact upsert returned no contact id"),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(400, {"message": f"DND {PHONE}"})], "message send failed: HTTP 400"),
    ([Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {})], "message send returned no message id"),
])
def test_failures_raise_without_leaking_the_response_body(responses, fragment):
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=Recorder(*responses))(PHONE, "hello", None)
    assert fragment in str(err.value) and PHONE not in str(err.value)


def test_a_network_error_becomes_a_ghl_sms_error():
    def boom(*a, **k):
        raise ConnectionError(f"reset {PHONE}")
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=boom)(PHONE, "hello", None)
    assert PHONE not in str(err.value)


def _clear(monkeypatch, *names):
    s = get_settings()
    for name in names:
        monkeypatch.setattr(s, name, None, raising=False)
    return s


ALL = ("lending_ghl_api_key", "lending_ghl_location_id", "lending_ghl_sms_from_number", "ghl_api_key", "ghl_location_id")


def test_no_account_when_nothing_is_configured(monkeypatch):
    _clear(monkeypatch, *ALL)
    assert lending_ghl_account() is None and get_sender() is None


def test_it_falls_back_to_the_bay_street_account_until_next_deal_lending_exists(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    assert lending_ghl_account() == GhlAccount("bay-key", "loc-bay")


def test_the_next_deal_lending_settings_win_once_both_are_set(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    monkeypatch.setattr(s, "lending_ghl_api_key", SecretStr("ndl-key"), raising=False)
    monkeypatch.setattr(s, "lending_ghl_location_id", "loc-ndl", raising=False)
    assert lending_ghl_account() == GhlAccount("ndl-key", "loc-ndl")


def test_a_sender_needs_an_account_and_the_texting_number(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    assert get_sender() is None and texting_number() is None
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "(813) 555-0100", raising=False)
    assert isinstance(get_sender(), GhlSmsSender) and texting_number() == "+18135550100"


def test_dnd_never_falls_back_to_the_shared_account(monkeypatch):
    """The DND sync uses only the Next Deal Lending sub-account (src.lending.ghl_account): with only the
    shared Bay Street credentials set it writes nothing."""
    from src.lending import ghl_dnd
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    calls = []
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", lambda *a, **k: calls.append(k) or Resp(200, {}))
    assert ghl_dnd.set_ghl_dnd(PHONE) is False and calls == []


def test_dnd_uses_the_next_deal_lending_account(monkeypatch):
    from src.lending import ghl_dnd
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "lending_ghl_api_key", SecretStr("ndl-key"), raising=False)
    monkeypatch.setattr(s, "lending_ghl_location_id", "loc-ndl", raising=False)
    seen = {}

    def fake(method, url, **kw):
        seen.update(kw)
        return Resp(200, {})
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert seen["json"]["locationId"] == "loc-ndl" and seen["headers"]["Authorization"] == "Bearer ndl-key"
    assert seen["headers"]["Version"] == "2021-07-28"


@pytest.fixture(autouse=True)
def _reset_warning_flags(monkeypatch):
    monkeypatch.setattr(ghl_sms, "_warned_fallback", False)
    monkeypatch.setattr(ghl_sms, "_warned_partial", False)


def _bay(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    return s


@pytest.mark.parametrize("field,value,missing", [
    ("lending_ghl_api_key", SecretStr("ndl-secret-key"), "LENDING_GHL_LOCATION_ID"),
    ("lending_ghl_location_id", "loc-ndl-secret", "LENDING_GHL_API_KEY"),
])
def test_a_half_configured_next_deal_lending_account_fails_closed(monkeypatch, caplog, field, value, missing):
    from src.lending import ghl_dnd
    s = _bay(monkeypatch)
    monkeypatch.setattr(s, field, value, raising=False)
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "(813) 555-0100", raising=False)
    with caplog.at_level(logging.INFO):
        assert lending_ghl_account() is None and get_sender() is None
        assert lending_ghl_account() is None
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and missing in errors[0].getMessage()
    assert "secret" not in caplog.text and "bay-key" not in caplog.text
    assert ghl_dnd.set_ghl_dnd(PHONE) is False and ghl_dnd.get_ghl_dnd() is None


def test_empty_strings_count_as_unset(monkeypatch):
    s = _bay(monkeypatch)
    monkeypatch.setattr(s, "lending_ghl_api_key", SecretStr(""), raising=False)
    monkeypatch.setattr(s, "lending_ghl_location_id", "", raising=False)
    assert lending_ghl_account() == GhlAccount("bay-key", "loc-bay")
    monkeypatch.setattr(s, "ghl_api_key", SecretStr(""), raising=False)
    assert lending_ghl_account() is None


def test_the_fallback_warning_is_logged_once(monkeypatch, caplog):
    _bay(monkeypatch)
    with caplog.at_level(logging.INFO):
        lending_ghl_account()
        lending_ghl_account()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "shared GHL_*" in r.getMessage()]
    assert len(warnings) == 1 and "bay-key" not in caplog.text


def test_an_invalid_texting_number_disables_texting_without_logging_it(monkeypatch, caplog):
    s = _bay(monkeypatch)
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "555-nope-99", raising=False)
    with caplog.at_level(logging.INFO):
        assert texting_number() is None and get_sender() is None
    assert "555-nope-99" not in caplog.text and any(r.levelno == logging.WARNING for r in caplog.records)


class Boom:
    def __init__(self, exc):
        self.exc, self.calls = exc, 0

    def __call__(self, *a, **k):
        self.calls += 1
        raise self.exc


@pytest.mark.parametrize("exc", [ConnectionError("reset"), requests.Timeout("slow")])
def test_the_message_send_is_attempted_exactly_once(monkeypatch, exc):
    def http(method, url, **kw):
        calls.append(url)
        if url.endswith("/contacts/upsert"):
            return Resp(200, {"contact": {"id": "ct1"}})
        raise exc

    calls = []
    monkeypatch.setattr("requests.request", http)
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100")(PHONE, "hello", None)
    assert [u.rsplit("/", 1)[-1] for u in calls] == ["upsert", "messages"] and PHONE not in str(err.value)
    assert err.value.ambiguous is True


def test_default_send_uses_a_timeout_and_a_429_is_an_error_not_a_retry(monkeypatch):
    seen = []

    def fake(method, url, **kw):
        seen.append((url, kw))
        return Resp(200, {"contact": {"id": "ct1"}}) if url.endswith("/contacts/upsert") else Resp(429, {})
    monkeypatch.setattr("requests.request", fake)
    with pytest.raises(GhlSmsError, match="HTTP 429"):
        GhlSmsSender(ACCOUNT, "+18135550100")(PHONE, "hello", None)
    assert len(seen) == 2 and all(kw["timeout"] == 15 for _, kw in seen)


def test_the_upsert_is_a_single_attempt_and_never_the_retrying_helper(monkeypatch):
    def retrying_helper(*a, **k):
        raise AssertionError("the retrying helper must not be used for texts")

    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", retrying_helper)
    calls = []

    def http(method, url, **kw):
        calls.append(url)
        return Resp(200, {"contact": {"id": "ct1"}}) if url.endswith("/contacts/upsert") else Resp(201, {"messageId": "m1"})
    monkeypatch.setattr("requests.request", http)
    assert GhlSmsSender(ACCOUNT, "+18135550100")(PHONE, "hello", None) == "m1"
    assert [u.rsplit("/", 1)[-1] for u in calls] == ["upsert", "messages"]


def test_an_upsert_timeout_is_one_call_and_not_ambiguous(monkeypatch):
    calls = []

    def http(method, url, **kw):
        calls.append(url)
        raise requests.Timeout("slow")
    monkeypatch.setattr("requests.request", http)
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100")(PHONE, "hello", None)
    assert len(calls) == 1 and err.value.ambiguous is False


def test_a_send_past_its_deadline_is_refused_after_the_upsert_and_never_requested():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    late = datetime.now(timezone.utc) - timedelta(seconds=1)
    with pytest.raises(GhlSmsError, match="deadline passed before send") as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None, deadline=late)
    assert err.value.ambiguous is False and len(request.calls) == 1
    assert request.calls[0][1].endswith("/contacts/upsert")  # the message request was never made


def test_a_send_inside_its_deadline_goes_out():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    soon = datetime.now(timezone.utc) + timedelta(seconds=30)
    assert GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None, deadline=soon) == "m1"


def test_api_versions_per_endpoint():
    request = Recorder(Resp(200, {"contact": {"id": "ct1"}}), Resp(201, {"messageId": "m1"}))
    GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert request.calls[0][2]["headers"]["Version"] == "2021-07-28"
    assert request.calls[1][2]["headers"]["Version"] == "2021-04-15"


def test_set_ghl_dnd_failure_paths(monkeypatch):
    from src.lending import ghl_dnd
    _clear(monkeypatch, *ALL)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False
    _bay(monkeypatch)
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", lambda *a, **k: Resp(422, {}))
    assert ghl_dnd.set_ghl_dnd(PHONE) is False

    def boom(*a, **k):
        raise ConnectionError(f"reset {PHONE}")
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", boom)
    assert ghl_dnd.set_ghl_dnd(PHONE) is False


def test_send_test_cli_reports_a_failed_send_without_a_traceback(monkeypatch):
    s = _bay(monkeypatch)
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "(813) 555-0100", raising=False)
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", Boom(ConnectionError("x")))
    assert ghl_sms.main(["--send-test", "(813) 555-8601"]) == 1


def _timeout(*a, **k):
    raise requests.Timeout("slow")


class UpsertThenSend:
    """First call (the contact upsert) succeeds; the second (the send) does ``send``."""

    def __init__(self, send):
        self.send, self.calls = send, 0

    def __call__(self, *a, **k):
        self.calls += 1
        if self.calls == 1:
            return Resp(200, {"contact": {"id": "ct1"}})
        return self.send() if callable(self.send) else self.send


@pytest.mark.parametrize("request_,ambiguous", [
    (UpsertThenSend(_timeout), True),                                   # send timeout / connection error
    (UpsertThenSend(lambda: (_ for _ in ()).throw(ConnectionError("reset"))), True),
    (UpsertThenSend(Resp(500, {})), True),                              # send 5xx
    (UpsertThenSend(Resp(503, {})), True),
    (UpsertThenSend(Resp(201, {})), True),                              # 2xx without a message id
    (UpsertThenSend(Resp(200, None)), True),                            # 2xx with an unreadable body
    (UpsertThenSend(Resp(422, {"message": "bad"})), False),             # 4xx: GHL rejected it
    (UpsertThenSend(Resp(429, {})), False),                             # rate limited: not accepted
    (UpsertThenSend(Resp(400, {})), False),
])
def test_only_a_send_that_may_have_been_accepted_is_ambiguous(request_, ambiguous):
    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request_)(PHONE, "hello", None)
    assert err.value.ambiguous is ambiguous and request_.calls == 2


@pytest.mark.parametrize("first", [_timeout, Resp(500, {}), Resp(422, {}), Resp(200, {"contact": {}})])
def test_a_contact_upsert_failure_is_never_ambiguous(first):
    calls = []

    def request(*a, **k):
        calls.append(1)
        return first(*a, **k) if callable(first) else first

    with pytest.raises(GhlSmsError) as err:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request)(PHONE, "hello", None)
    assert err.value.ambiguous is False and len(calls) == 1  # the send was never attempted


def test_ghl_sms_error_is_not_ambiguous_by_default():
    assert GhlSmsError("x").ambiguous is False and GhlSmsError("x", ambiguous=True).ambiguous is True
