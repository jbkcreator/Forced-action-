from types import SimpleNamespace
from unittest.mock import MagicMock

from pydantic import SecretStr

from src.services import telnyx_sms


def _settings():
    return SimpleNamespace(
        telnyx_sms_api_key=SecretStr("k"),
        telnyx_messaging_profile_id="settings-profile",
        telnyx_from_number="+15550000001",
    )


def _post(monkeypatch):
    monkeypatch.setattr(telnyx_sms, "get_settings", _settings)
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"data": {"id": "m1", "status": "queued"}}
    post = MagicMock(return_value=resp)
    monkeypatch.setattr(telnyx_sms.requests, "post", post)
    return post


def test_send_message_uses_overrides(monkeypatch):
    post = _post(monkeypatch)
    telnyx_sms.send_message("+15551112222", "hi", from_number="+15559990000", messaging_profile_id="override-profile")
    sent = post.call_args.kwargs["json"]
    assert sent["from"] == "+15559990000"
    assert sent["messaging_profile_id"] == "override-profile"


def test_send_message_defaults_unchanged(monkeypatch):
    post = _post(monkeypatch)
    telnyx_sms.send_message("+15551112222", "hi")
    sent = post.call_args.kwargs["json"]
    assert sent["from"] == "+15550000001"
    assert sent["messaging_profile_id"] == "settings-profile"


def test_overrides_do_not_need_settings_values(monkeypatch):
    post = _post(monkeypatch)
    monkeypatch.setattr(
        telnyx_sms, "get_settings",
        lambda: SimpleNamespace(telnyx_sms_api_key=SecretStr("k"), telnyx_messaging_profile_id=None, telnyx_from_number=None),
    )
    telnyx_sms.send_message("+15551112222", "hi", from_number="+15559990000", messaging_profile_id="p")
    assert post.called
