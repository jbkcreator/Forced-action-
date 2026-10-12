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
    values = {"lending_slack_signing_secret": SecretStr(SECRET), "lending_soft_approval_enabled": True,
              "redis_url": None}
    values.update(overrides)
    return SimpleNamespace(**values)


class FakeRedis:
    """Just enough of redis-py for the receipt: SET key value NX EX."""

    def __init__(self, fail: bool = False) -> None:
        self.keys: dict[str, object] = {}
        self.fail = fail

    def set(self, key, value, nx=False, ex=None):
        if self.fail:
            raise ConnectionError("redis down")
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True


@pytest.fixture
def redis(monkeypatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr("src.core.redis_client.get_redis", lambda: fake)
    monkeypatch.setattr(cora_events_webhook, "_own_client", None)
    monkeypatch.setattr(cora_events_webhook, "_next_connect_at", 0.0)
    return fake


@pytest.fixture
def handled(monkeypatch, redis) -> list[tuple]:
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


def test_retry_of_an_accepted_event_is_dropped(client, handled) -> None:
    body = _event()
    assert client.post(EVENTS, content=body, headers=_signed(body)).status_code == 200
    assert client.post(EVENTS, content=body, headers=_signed(body, retry="1")).status_code == 200
    assert client.post(EVENTS, content=body, headers=_signed(body)).status_code == 200  # a plain repeat too
    assert len(handled) == 1


def test_retry_of_an_event_that_never_arrived_is_processed(client, handled) -> None:
    """The first delivery failed before reaching us (e.g. nginx 502 during a restart); Slack's retry is all we get."""
    body = _event()
    response = client.post(EVENTS, content=body, headers=_signed(body, retry="1"))
    assert response.status_code == 200 and len(handled) == 1 and handled[0][2] == "Ev123"


def test_receipt_is_shared_by_every_worker(handled, monkeypatch) -> None:
    monkeypatch.setattr(cora_events_webhook, "get_settings", lambda: _settings())
    workers = []
    for _ in range(2):
        app = FastAPI()
        app.include_router(cora_events_webhook.router)
        workers.append(TestClient(app))
    body = _event()
    workers[0].post(EVENTS, content=body, headers=_signed(body))
    workers[1].post(EVENTS, content=body, headers=_signed(body, retry="1"))
    assert len(handled) == 1


def test_different_events_are_each_processed(client, handled) -> None:
    for event_id in ("EvA", "EvB"):
        body = json.dumps({"type": "event_callback", "event_id": event_id, "event": {"type": "message"}}).encode()
        client.post(EVENTS, content=body, headers=_signed(body))
    assert [call[2] for call in handled] == ["EvA", "EvB"]


@pytest.mark.parametrize("broken", ["unavailable", "erroring"])
def test_events_are_processed_when_redis_cannot_be_used(client, handled, monkeypatch, broken) -> None:
    fake = None if broken == "unavailable" else FakeRedis(fail=True)
    monkeypatch.setattr("src.core.redis_client.get_redis", lambda: fake)
    body = _event()
    client.post(EVENTS, content=body, headers=_signed(body))
    assert len(handled) == 1  # never lost; a duplicate answer is the accepted risk


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


def test_receipts_reconnect_when_the_shared_client_gave_up(handled, monkeypatch) -> None:
    """get_redis() stays None if Redis was down at its one connection attempt; Cora keeps retrying her own."""
    monkeypatch.setattr("src.core.redis_client.get_redis", lambda: None)
    monkeypatch.setattr(cora_events_webhook, "get_settings", lambda: _settings(redis_url="redis://cache:6379/0"))
    monkeypatch.setattr(cora_events_webhook, "RECONNECT_SECONDS", 0)
    recovered = FakeRedis()
    recovered.ping = lambda: True
    attempts: list = []

    def from_url(url, **kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise ConnectionError("still down")
        return recovered

    import redis as redis_lib

    monkeypatch.setattr(redis_lib.Redis, "from_url", staticmethod(from_url))
    assert cora_events_webhook.accept_event_once("Ev1") is True  # down: processed, not lost
    assert cora_events_webhook.accept_event_once("Ev2") is True  # reconnected: receipt taken
    assert cora_events_webhook.accept_event_once("Ev2") is False  # repeat of an accepted event
    assert len(attempts) == 2 and attempts[0]["socket_timeout"] == 1


def test_reconnect_attempts_are_rate_limited(monkeypatch) -> None:
    monkeypatch.setattr("src.core.redis_client.get_redis", lambda: None)
    monkeypatch.setattr(cora_events_webhook, "get_settings", lambda: _settings(redis_url="redis://cache:6379/0"))
    monkeypatch.setattr(cora_events_webhook, "_own_client", None)
    monkeypatch.setattr(cora_events_webhook, "_next_connect_at", 0.0)
    attempts: list = []

    import redis as redis_lib

    def failing(url, **kwargs):
        attempts.append(1)
        raise ConnectionError("down")

    monkeypatch.setattr(redis_lib.Redis, "from_url", staticmethod(failing))
    for event_id in ("A", "B", "C"):
        assert cora_events_webhook.accept_event_once(event_id) is True
    assert len(attempts) == 1  # one try per RECONNECT_SECONDS, not one per message


def test_only_the_first_action_decides_the_route() -> None:
    first_cora = {"type": "block_actions", "actions": [{"action_id": "agent_core_approve"}, {"action_id": "x"}]}
    later_cora = {"type": "block_actions", "actions": [{"action_id": "x"}, {"action_id": "agent_core_approve"}]}
    assert interactivity.is_cora_click(first_cora)
    assert not interactivity.is_cora_click(later_cora)
    assert not interactivity.is_cora_click({"type": "block_actions", "actions": []})
    assert not interactivity.is_cora_click({"type": "view_submission", "actions": [{"action_id": "agent_core_approve"}]})


# -- runtime ----------------------------------------------------------------------------------------

@pytest.fixture
def fresh_runtime():
    cora_runtime.reset()
    yield
    cora_runtime.reset()


def test_unconfigured_cora_is_not_rebuilt_on_every_message(fresh_runtime, monkeypatch) -> None:
    builds: list = []
    monkeypatch.setattr(cora_runtime, "build_live_services", lambda settings: builds.append(1))
    monkeypatch.setattr(cora_runtime, "missing_settings", lambda config: ["approver_user_ids"])
    cora_runtime.handle_slack_request("events_api", {"event": {}}, "Ev1")
    cora_runtime.handle_slack_request("events_api", {"event": {}}, "Ev2")
    assert len(builds) == 1  # remembered as unavailable; no auth.test per message


def test_a_slack_blip_is_retried_soon_not_after_five_minutes(fresh_runtime, monkeypatch) -> None:
    """auth.test failing once (settings present) must not silence Cora for the long unconfigured window."""
    results = [None, SimpleNamespace(session=SimpleNamespace(handle_request=lambda *args: None))]
    monkeypatch.setattr(cora_runtime, "build_live_services", lambda settings: results.pop(0))
    monkeypatch.setattr(cora_runtime, "missing_settings", lambda config: [])
    monkeypatch.setattr(cora_runtime, "RETRY_UNREACHABLE_SECONDS", 0)
    assert cora_runtime.cora_services() is None
    assert cora_runtime.cora_services() is not None and results == []


def test_unconfigured_and_unreachable_use_different_waits(fresh_runtime, monkeypatch) -> None:
    monkeypatch.setattr(cora_runtime, "build_live_services", lambda settings: None)
    monkeypatch.setattr(cora_runtime, "missing_settings", lambda config: [])
    before = time.monotonic()
    cora_runtime.cora_services()
    assert cora_runtime._unavailable_until - before <= cora_runtime.RETRY_UNREACHABLE_SECONDS + 1
    cora_runtime.reset()
    monkeypatch.setattr(cora_runtime, "missing_settings", lambda config: ["slack_channel_id"])
    cora_runtime.cora_services()
    assert cora_runtime._unavailable_until - before >= cora_runtime.RETRY_UNCONFIGURED_SECONDS - 1


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
