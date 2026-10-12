"""Cora over HTTP on the lending Slack app: the events endpoint, the shared interactivity endpoint's
routing, and the per-worker runtime. No Slack, Claude or database is touched."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import cora_events_webhook, cora_runtime
from src.lending import soft_approval_webhook as interactivity

SECRET = "test-signing-secret"


def _signed(body: bytes, *, secret: str = SECRET, retry: str | None = None) -> dict[str, str]:
    timestamp = str(int(time.time()))
    digest = hmac.new(secret.encode(), b"v0:" + timestamp.encode() + b":" + body, hashlib.sha256).hexdigest()
    headers = {"X-Slack-Request-Timestamp": timestamp, "X-Slack-Signature": f"v0={digest}"}
    if retry:
        headers["X-Slack-Retry-Num"] = retry
    return headers


def _settings(**overrides) -> SimpleNamespace:
    values = {"lending_slack_signing_secret": SecretStr(SECRET), "lending_soft_approval_enabled": True}
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def handled(monkeypatch) -> list[tuple]:
    calls: list[tuple] = []
    record = lambda *args: calls.append(args)  # noqa: E731 - tiny recorder
    monkeypatch.setattr(cora_events_webhook, "handle_slack_request", record)
    monkeypatch.setattr(interactivity, "handle_slack_request", record)
    return calls


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setattr(cora_events_webhook, "get_settings", lambda: _settings())
    monkeypatch.setattr(interactivity, "get_settings", lambda: _settings())
    app = FastAPI()
    app.include_router(cora_events_webhook.router)
    app.include_router(interactivity.router)
    return TestClient(app)


# -- events -----------------------------------------------------------------------------------------

EVENTS = "/webhooks/lending/slack-events"


def _event(text_value: str = "how many leads?") -> bytes:
    return json.dumps({"type": "event_callback", "event_id": "Ev123", "event": {
        "type": "message", "channel": "C_CORA", "user": "U_JOSH", "text": text_value, "ts": "1.1"}}).encode()


def test_events_closed_while_the_signing_secret_is_unset(client, monkeypatch, handled) -> None:
    monkeypatch.setattr(cora_events_webhook, "get_settings", lambda: _settings(lending_slack_signing_secret=None))
    body = _event()
    assert client.post(EVENTS, content=body, headers=_signed(body)).status_code == 503
    assert handled == []


def test_events_reject_a_bad_or_missing_signature(client, handled) -> None:
    body = _event()
    assert client.post(EVENTS, content=body, headers=_signed(body, secret="wrong")).status_code == 401
    assert client.post(EVENTS, content=body).status_code == 401
    assert handled == []


def test_url_verification_challenge_is_answered(client, handled) -> None:
    body = json.dumps({"type": "url_verification", "challenge": "abc123"}).encode()
    response = client.post(EVENTS, content=body, headers=_signed(body))
    assert response.status_code == 200 and response.json() == {"challenge": "abc123"}
    assert handled == []


def test_message_event_is_acknowledged_and_handed_to_cora(client, handled) -> None:
    body = _event()
    response = client.post(EVENTS, content=body, headers=_signed(body))
    assert response.status_code == 200 and response.json() == {}
    assert handled == [("events_api", {"event": {"type": "message", "channel": "C_CORA", "user": "U_JOSH",
                                                  "text": "how many leads?", "ts": "1.1"}}, "Ev123")]


def test_slack_retries_are_acknowledged_but_not_handled_again(client, handled) -> None:
    body = _event()
    assert client.post(EVENTS, content=body, headers=_signed(body, retry="1")).status_code == 200
    assert handled == []


def test_malformed_or_unknown_payloads(client, handled) -> None:
    for body in (b"not json", b"[1, 2]"):
        assert client.post(EVENTS, content=body, headers=_signed(body)).status_code == 400
    body = json.dumps({"type": "app_rate_limited"}).encode()
    assert client.post(EVENTS, content=body, headers=_signed(body)).json() == {}
    assert handled == []


# -- shared interactivity endpoint -----------------------------------------------------------------

CLICKS = "/webhooks/lending/slack-interactivity"


def _click(action_id: str, value: str = "7") -> bytes:
    payload = {"type": "block_actions", "trigger_id": "trig", "user": {"id": "U_JOSH"}, "channel": {"id": "C_CORA"},
               "message": {"ts": "9.9"}, "actions": [{"action_id": action_id, "value": value, "action_ts": "1700.1"}]}
    return urlencode({"payload": json.dumps(payload)}).encode()


@pytest.mark.parametrize("action_id", ["agent_core_approve", "agent_core_revise", "agent_core_reject"])
def test_cora_card_clicks_go_to_cora(client, handled, monkeypatch, action_id) -> None:
    opened: list = []
    monkeypatch.setattr(interactivity, "_open_form", opened.append)
    body = _click(action_id)
    assert client.post(CLICKS, content=body, headers=_signed(body)).json() == {}
    assert len(handled) == 1
    request_type, payload, envelope = handled[0]
    assert request_type == "interactive" and payload["actions"][0]["action_id"] == action_id and envelope == "1700.1"
    assert opened == []  # the soft-approval form is not opened for a Cora click


def test_cora_clicks_still_work_when_soft_approval_is_off(client, handled, monkeypatch) -> None:
    monkeypatch.setattr(interactivity, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
    body = _click("agent_core_approve")
    assert client.post(CLICKS, content=body, headers=_signed(body)).json() == {}
    assert len(handled) == 1


def test_soft_approval_clicks_still_reach_the_soft_approval_form(client, handled, monkeypatch) -> None:
    opened: list = []
    monkeypatch.setattr(interactivity, "_open_form", opened.append)
    body = _click("soft_approval_open", value="call-1")
    assert client.post(CLICKS, content=body, headers=_signed(body)).status_code == 200
    assert handled == [] and len(opened) == 1


def test_interactivity_keeps_its_signature_and_disabled_behaviour(client, handled, monkeypatch) -> None:
    body = _click("agent_core_approve")
    assert client.post(CLICKS, content=body, headers=_signed(body, secret="wrong")).status_code == 401
    monkeypatch.setattr(interactivity, "get_settings", lambda: _settings(lending_soft_approval_enabled=False))
    body = urlencode({"payload": "not json"}).encode()
    assert client.post(CLICKS, content=body, headers=_signed(body)).json() == {}  # disabled: ignored, as before
    assert handled == []


# -- runtime ----------------------------------------------------------------------------------------

@pytest.fixture
def fresh_runtime():
    cora_runtime.reset()
    yield
    cora_runtime.reset()


def test_unconfigured_cora_is_not_rebuilt_on_every_message(fresh_runtime, monkeypatch) -> None:
    builds: list = []
    monkeypatch.setattr(cora_runtime, "build_live_services", lambda settings: builds.append(1))
    cora_runtime.handle_slack_request("events_api", {"event": {}}, "Ev1")
    cora_runtime.handle_slack_request("events_api", {"event": {}}, "Ev2")
    assert len(builds) == 1  # remembered as unavailable; no auth.test per message


def test_configured_cora_is_built_once_and_handles_requests(fresh_runtime, monkeypatch) -> None:
    received: list = []
    session = SimpleNamespace(handle_request=lambda *args: received.append(args))
    builds: list = []

    def build(settings):
        builds.append(1)
        return SimpleNamespace(session=session)

    monkeypatch.setattr(cora_runtime, "build_live_services", build)
    cora_runtime.handle_slack_request("events_api", {"event": {"text": "a"}}, "Ev1")
    cora_runtime.handle_slack_request("interactive", {"type": "block_actions"}, "1700.1")
    assert len(builds) == 1
    assert [call[0] for call in received] == ["events_api", "interactive"]


def test_a_crash_in_handling_never_escapes(fresh_runtime, monkeypatch) -> None:
    def broken(*_):
        raise RuntimeError("boom")

    monkeypatch.setattr(cora_runtime, "build_live_services",
                        lambda settings: SimpleNamespace(session=SimpleNamespace(handle_request=broken)))
    cora_runtime.handle_slack_request("events_api", {"event": {}}, "Ev1")
