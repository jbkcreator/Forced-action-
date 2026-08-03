"""
Unit tests for POST /api/admin/slack/interact — the single Interactivity
Request URL that /slack/county-launch/interact, /slack/relay-decision, and
/slack/win-story/interact previously each tried to register on their own
(Slack allows exactly one per app, so at most one of the three was ever
actually reachable). This endpoint dispatches on the clicked button's
action_id; the old three routes remain as deprecated aliases delegating to
the same handlers (covered by their own existing test files).
"""
import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient


def _sign(body: bytes, secret: str, ts_offset: int = 0) -> tuple[str, str]:
    ts = str(int(time.time()) + ts_offset)
    sig_base = f"v0:{ts}:{body.decode()}"
    sig = "v0=" + hmac.new(secret.encode(), sig_base.encode(), hashlib.sha256).hexdigest()
    return ts, sig


@pytest.fixture
def app_and_client(monkeypatch):
    # See test_slack_interact_endpoint.py / test_relay_slack_endpoints.py for
    # why this patches admin_router's own `settings` binding directly.
    monkeypatch.setattr(
        "src.api.admin_router.settings.slack_signing_secret",
        MagicMock(get_secret_value=lambda: "test-signing-secret"),
    )
    monkeypatch.setattr("src.api.admin_router.settings.county_launch_approvers", ["U_APPROVER"])
    monkeypatch.setattr("src.api.admin_router.settings.relay_approvers", ["U_APPROVER"])
    monkeypatch.setattr(
        "src.api.admin_router.settings.slack_bot_token",
        MagicMock(get_secret_value=lambda: "xoxb-test"),
    )
    monkeypatch.setattr("src.api.admin_router.settings.county_launch_slack_channel", "#expansion")
    monkeypatch.setattr("src.api.admin_router.settings.relay_slack_channel", "#agent-daily")

    from src.api.main import app
    return app, TestClient(app)


@pytest.fixture
def app_client(app_and_client):
    _, client = app_and_client
    return client


def _post_interact(client, payload: dict, ts_offset: int = 0, bad_sig: bool = False):
    body = urlencode({"payload": json.dumps(payload)}).encode()
    if bad_sig:
        ts, sig = str(int(time.time())), "v0=badsig"
    else:
        ts, sig = _sign(body, "test-signing-secret", ts_offset=ts_offset)
    return client.post(
        "/api/admin/slack/interact",
        content=body,
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "x-slack-request-timestamp": ts,
            "x-slack-signature": sig,
        },
    )


def _mock_db_candidate(app, candidate):
    from src.api.admin_router import get_db

    session = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = candidate
    session.execute.return_value = result

    def override():
        yield session

    app.dependency_overrides[get_db] = override
    return session


def test_dispatches_county_launch_decision(app_and_client, monkeypatch):
    app, client = app_and_client
    candidate = MagicMock()
    candidate.id = 1
    candidate.county_id = "pinellas"
    candidate.status = "queued"
    candidate.approved_by_slack_user = None
    candidate.last_slack_message_ts = "123.456"

    _mock_db_candidate(app, candidate)
    monkeypatch.setattr("src.api.admin_router._update_slack_message", MagicMock())

    payload = {
        "user": {"id": "U_APPROVER"},
        "actions": [{
            "action_id": "county_launch_decision",
            "value": json.dumps({"candidate_id": 1, "action": "approve"}),
        }],
    }
    try:
        resp = _post_interact(client, payload)
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        assert candidate.status == "approved"
    finally:
        app.dependency_overrides.clear()


def test_dispatches_relay_decision(app_client, monkeypatch):
    mock_record_decision = MagicMock(return_value=MagicMock(slack_message_ts=None))
    monkeypatch.setattr("src.services.relay.queue.record_decision", mock_record_decision)

    payload = {
        "user": {"id": "U_APPROVER"},
        "actions": [{"action_id": "approve", "value": json.dumps({"item_id": 1, "action": "approve"})}],
    }
    resp = _post_interact(app_client, payload)

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    mock_record_decision.assert_called_once_with(1, approved=True, decided_by="U_APPROVER")


def test_dispatches_win_story_decision(app_client, monkeypatch):
    row = MagicMock(id=5, is_public=False, proof_text="5 fresh leads claimed")
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = row

    from src.api.admin_router import get_db
    from src.api.main import app as fastapi_app

    def override():
        yield session

    fastapi_app.dependency_overrides[get_db] = override
    monkeypatch.setattr("src.api.admin_router._update_win_story_slack_message", MagicMock())

    payload = {
        "user": {"id": "U_ANYONE"},
        "actions": [{"action_id": "approve_win_story", "value": json.dumps({"asset_id": 5, "action": "approve"})}],
    }
    try:
        resp = _post_interact(app_client, payload)
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
    finally:
        fastapi_app.dependency_overrides.clear()


def test_unrecognized_action_id_returns_ephemeral(app_client):
    payload = {
        "user": {"id": "U_APPROVER"},
        "actions": [{"action_id": "something_new", "value": "{}"}],
    }
    resp = _post_interact(app_client, payload)

    assert resp.status_code == 200
    assert "Unrecognized action" in resp.json()["text"]


def test_invalid_signature_returns_401(app_client):
    payload = {"user": {"id": "U_APPROVER"}, "actions": [{"action_id": "approve", "value": "{}"}]}
    resp = _post_interact(app_client, payload, bad_sig=True)
    assert resp.status_code == 401
