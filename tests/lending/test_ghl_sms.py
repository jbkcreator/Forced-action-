"""WP-GL-9 GHL sender: request shapes and error handling, with no network."""
from __future__ import annotations

import pytest
from pydantic import SecretStr

from config.settings import get_settings
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
    assert lending_ghl_account() == GhlAccount("bay-key", "loc-bay")  # key alone is not enough: both or neither
    monkeypatch.setattr(s, "lending_ghl_location_id", "loc-ndl", raising=False)
    assert lending_ghl_account() == GhlAccount("ndl-key", "loc-ndl")


def test_a_sender_needs_an_account_and_the_texting_number(monkeypatch):
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    assert get_sender() is None and texting_number() is None
    monkeypatch.setattr(s, "lending_ghl_sms_from_number", "(813) 555-0100", raising=False)
    assert isinstance(get_sender(), GhlSmsSender) and texting_number() == "+18135550100"


def test_dnd_uses_the_same_account_as_the_sender(monkeypatch):
    from src.lending import ghl_dnd
    s = _clear(monkeypatch, *ALL)
    monkeypatch.setattr(s, "ghl_api_key", SecretStr("bay-key"), raising=False)
    monkeypatch.setattr(s, "ghl_location_id", "loc-bay", raising=False)
    seen = {}

    def fake(method, url, **kw):
        seen.update(kw)
        return Resp(200, {})
    monkeypatch.setattr("src.services.ghl_webhook._ghl_request", fake)
    assert ghl_dnd.set_ghl_dnd(PHONE) is True
    assert seen["json"]["locationId"] == "loc-bay" and seen["headers"]["Authorization"] == "Bearer bay-key"
    assert seen["headers"]["Version"] == "2021-07-28"
