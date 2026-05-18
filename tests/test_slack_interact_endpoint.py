"""Unit tests for POST /api/admin/slack/county-launch/interact."""
import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient


def _make_signed_request(client, payload_dict: dict, secret: str = "test-signing-secret",
                          ts_offset: int = 0):
    """Build a properly signed Slack interaction request."""
    payload_str = json.dumps(payload_dict)
    body = urlencode({"payload": payload_str}).encode()
    ts = str(int(time.time()) + ts_offset)
    sig_base = f"v0:{ts}:{body.decode()}"
    sig = "v0=" + hmac.new(secret.encode(), sig_base.encode(), hashlib.sha256).hexdigest()
    return client.post(
        "/api/admin/slack/county-launch/interact",
        content=body,
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "x-slack-request-timestamp": ts,
            "x-slack-signature": sig,
        },
    )


def _make_payload(user_id: str, candidate_id: int, action: str) -> dict:
    return {
        "user": {"id": user_id},
        "actions": [
            {
                "action_id": "county_launch_decision",
                "value": json.dumps({"candidate_id": candidate_id, "action": action}),
            }
        ],
    }


@pytest.fixture
def app_and_client(monkeypatch):
    monkeypatch.setattr(
        "config.settings.settings.slack_signing_secret",
        MagicMock(get_secret_value=lambda: "test-signing-secret"),
    )
    monkeypatch.setattr(
        "config.settings.settings.county_launch_approvers",
        ["U_APPROVER"],
    )
    monkeypatch.setattr(
        "config.settings.settings.slack_bot_token",
        MagicMock(get_secret_value=lambda: "xoxb-test"),
    )
    monkeypatch.setattr("config.settings.settings.county_launch_slack_channel", "#expansion")

    from src.api.main import app
    return app, TestClient(app)


@pytest.fixture
def app_client(app_and_client):
    _, client = app_and_client
    return client


def _mock_db_candidate(app, candidate):
    """Override get_db dependency to return a mock session with the given candidate."""
    from src.api.admin_router import get_db

    session = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = candidate
    session.execute.return_value = result

    def override():
        yield session

    app.dependency_overrides[get_db] = override
    return session


def test_approve_by_valid_approver(app_and_client, monkeypatch):
    app, client = app_and_client
    candidate = MagicMock()
    candidate.id = 1
    candidate.county_id = "pinellas"
    candidate.status = "queued"
    candidate.approved_by_slack_user = None
    candidate.last_slack_message_ts = "123.456"

    _mock_db_candidate(app, candidate)
    monkeypatch.setattr("src.api.admin_router._update_slack_message", MagicMock())

    try:
        resp = _make_signed_request(client, _make_payload("U_APPROVER", 1, "approve"))
        assert resp.status_code == 200
        assert candidate.status == "approved"
    finally:
        app.dependency_overrides.clear()


def test_non_approver_rejected(app_client):
    resp = _make_signed_request(app_client, _make_payload("U_RANDO", 1, "approve"))
    assert resp.status_code == 200
    data = resp.json()
    assert "Not authorized" in data.get("text", "")


def test_invalid_signature_returns_401(app_client):
    payload_str = json.dumps(_make_payload("U_APPROVER", 1, "approve"))
    body = urlencode({"payload": payload_str}).encode()
    ts = str(int(time.time()))
    resp = app_client.post(
        "/api/admin/slack/county-launch/interact",
        content=body,
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "x-slack-request-timestamp": ts,
            "x-slack-signature": "v0=badsig",
        },
    )
    assert resp.status_code == 401


def test_replay_attack_rejected(app_client):
    payload_dict = _make_payload("U_APPROVER", 1, "approve")
    # ts_offset=-400 makes timestamp 400s old (>300s threshold)
    resp = _make_signed_request(app_client, payload_dict, ts_offset=-400)
    assert resp.status_code == 401


def test_double_click_idempotent(app_and_client, monkeypatch):
    app, client = app_and_client
    candidate = MagicMock()
    candidate.id = 1
    candidate.county_id = "pinellas"
    candidate.status = "approved"
    candidate.approved_by_slack_user = "U_APPROVER"
    candidate.last_slack_message_ts = "123.456"

    _mock_db_candidate(app, candidate)

    try:
        resp = _make_signed_request(client, _make_payload("U_APPROVER", 1, "approve"))
        assert resp.status_code == 200
        data = resp.json()
        assert "Already approved" in data.get("text", "")
    finally:
        app.dependency_overrides.clear()
