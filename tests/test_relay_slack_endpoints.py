"""
Unit tests for POST /api/admin/slack/relay-decision and
POST /api/admin/slack/kill (RELAY-v2.2 R1 — added per the 2026-07-24
impl-audit finding that neither endpoint had coverage).

Mirrors tests/test_slack_interact_endpoint.py's signing/fixture pattern.
Mocks src.services.relay.queue's DB-touching functions and
src.core.redis_client.rset directly (matching tests/test_relay_engine.py's
convention) rather than hitting a real DB/Redis, since slack_relay_decision
and slack_kill_command call these internally instead of taking a
FastAPI-injected session.
"""
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock
from urllib.parse import urlencode

import pytest
from fastapi.testclient import TestClient

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay.queue import QueueItem


def _sign(body: bytes, secret: str, ts_offset: int = 0) -> tuple[str, str]:
    ts = str(int(time.time()) + ts_offset)
    sig_base = f"v0:{ts}:{body.decode()}"
    sig = "v0=" + hmac.new(secret.encode(), sig_base.encode(), hashlib.sha256).hexdigest()
    return ts, sig


def _make_item(item_id=1, status="pending", slack_message_ts=None,
               venture_key=DEFAULT_VENTURE_KEY) -> QueueItem:
    return QueueItem(
        id=item_id, idempotency_key=f"key-{item_id}", batch_id=None, thread_id=None,
        channel="noop", recipient="prospect@example.com",
        payload={"subject": "Hi", "body": "Hello"}, status=status,
        slack_message_ts=slack_message_ts, decided_by=None, decided_at=None,
        error=None, dispatched_at=None, created_at=datetime.now(timezone.utc),
        venture_key=venture_key,
    )


def _interactive_payload(user_id: str, item_id: int, action: str) -> dict:
    return {
        "user": {"id": user_id},
        "actions": [{"action_id": action, "value": json.dumps({"item_id": item_id, "action": action})}],
    }


@pytest.fixture
def app_and_client(monkeypatch):
    # Patches src.api.admin_router's own imported 'settings' name, not
    # config.settings.settings directly — test_instantly_service.py's
    # reload(config.settings) calls elsewhere in the session rebind
    # config.settings.settings to a brand-new object each time, which
    # would desync from whatever object admin_router.py already captured
    # via `from config.settings import settings` at its own first import.
    # Patching admin_router's binding directly is immune to that.
    monkeypatch.setattr(
        "src.api.admin_router.settings.slack_signing_secret",
        MagicMock(get_secret_value=lambda: "test-signing-secret"),
    )
    monkeypatch.setattr("src.api.admin_router.settings.relay_approvers", ["U_APPROVER"])
    monkeypatch.setattr(
        "src.api.admin_router.settings.slack_bot_token",
        MagicMock(get_secret_value=lambda: "xoxb-test"),
    )
    monkeypatch.setattr("src.api.admin_router.settings.relay_slack_channel", "#agent-daily")

    from src.api.main import app
    return app, TestClient(app)


@pytest.fixture
def app_client(app_and_client):
    _, client = app_and_client
    return client


def _post_decision(client, payload: dict, ts_offset: int = 0, bad_sig: bool = False):
    body = urlencode({"payload": json.dumps(payload)}).encode()
    if bad_sig:
        ts, sig = str(int(time.time())), "v0=badsig"
    else:
        ts, sig = _sign(body, "test-signing-secret", ts_offset=ts_offset)
    return client.post(
        "/api/admin/slack/relay-decision",
        content=body,
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "x-slack-request-timestamp": ts,
            "x-slack-signature": sig,
        },
    )


def _post_kill(client, text: str, user_id: str = "U_APPROVER", ts_offset: int = 0, bad_sig: bool = False):
    body = urlencode({"text": text, "user_id": user_id}).encode()
    if bad_sig:
        ts, sig = str(int(time.time())), "v0=badsig"
    else:
        ts, sig = _sign(body, "test-signing-secret", ts_offset=ts_offset)
    return client.post(
        "/api/admin/slack/kill",
        content=body,
        headers={
            "content-type": "application/x-www-form-urlencoded",
            "x-slack-request-timestamp": ts,
            "x-slack-signature": sig,
        },
    )


# ---------------------------------------------------------------------------
# /slack/relay-decision
# ---------------------------------------------------------------------------

def test_approve_flips_row_and_updates_message(app_client, monkeypatch):
    approved_item = _make_item(status="approved", slack_message_ts="123.456")
    mock_record_decision = MagicMock(return_value=approved_item)
    monkeypatch.setattr("src.services.relay.queue.get_item", MagicMock(return_value=_make_item()))
    monkeypatch.setattr("src.services.relay.queue.record_decision", mock_record_decision)
    monkeypatch.setattr("src.api.admin_router._update_relay_slack_message", MagicMock())

    resp = _post_decision(app_client, _interactive_payload("U_APPROVER", 1, "approve"))

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    mock_record_decision.assert_called_once_with(1, approved=True, decided_by="U_APPROVER")


def test_reject_calls_record_decision_with_approved_false(app_client, monkeypatch):
    rejected_item = _make_item(status="rejected", slack_message_ts="123.456")
    mock_record_decision = MagicMock(return_value=rejected_item)
    monkeypatch.setattr("src.services.relay.queue.get_item", MagicMock(return_value=_make_item()))
    monkeypatch.setattr("src.services.relay.queue.record_decision", mock_record_decision)
    monkeypatch.setattr("src.api.admin_router._update_relay_slack_message", MagicMock())

    resp = _post_decision(app_client, _interactive_payload("U_APPROVER", 1, "reject"))

    assert resp.status_code == 200
    mock_record_decision.assert_called_once_with(1, approved=False, decided_by="U_APPROVER")


def test_non_approver_rejected(app_client, monkeypatch):
    mock_record_decision = MagicMock()
    monkeypatch.setattr("src.services.relay.queue.record_decision", mock_record_decision)

    resp = _post_decision(app_client, _interactive_payload("U_RANDO", 1, "approve"))

    assert resp.status_code == 200
    assert "Not authorized" in resp.json()["text"]
    mock_record_decision.assert_not_called()


def test_invalid_signature_returns_401(app_client):
    resp = _post_decision(app_client, _interactive_payload("U_APPROVER", 1, "approve"), bad_sig=True)
    assert resp.status_code == 401


def test_replay_attack_rejected(app_client):
    resp = _post_decision(app_client, _interactive_payload("U_APPROVER", 1, "approve"), ts_offset=-400)
    assert resp.status_code == 401


def test_double_click_returns_already_decided(app_client, monkeypatch):
    # record_decision returns None when the row was not still 'pending' —
    # the WHERE status='pending' guard didn't match a second decision attempt.
    monkeypatch.setattr("src.services.relay.queue.get_item", MagicMock(return_value=_make_item()))
    monkeypatch.setattr("src.services.relay.queue.record_decision", MagicMock(return_value=None))

    resp = _post_decision(app_client, _interactive_payload("U_APPROVER", 1, "approve"))

    assert resp.status_code == 200
    assert "already decided" in resp.json()["text"]


# ---------------------------------------------------------------------------
# /slack/kill
# ---------------------------------------------------------------------------

def test_kill_all_sets_global_override(app_client, monkeypatch):
    mock_rset = MagicMock(return_value=True)
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "ALL")

    assert resp.status_code == 200
    mock_rset.assert_called_once()
    args = mock_rset.call_args[0]
    assert args[0] == "kill_switch_override:global"
    assert args[1] == "red"


def test_kill_relay_sets_relay_global_override(app_client, monkeypatch):
    mock_rset = MagicMock(return_value=True)
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "RELAY")

    assert resp.status_code == 200
    assert mock_rset.call_args[0][0] == "kill_switch_override:relay_global"


def test_kill_cora_sets_cora_global_override(app_client, monkeypatch):
    mock_rset = MagicMock(return_value=True)
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "CORA")

    assert resp.status_code == 200
    assert mock_rset.call_args[0][0] == "kill_switch_override:cora_global"
    assert mock_rset.call_args[0][1] == "red"


def test_kill_non_approver_rejected(app_client, monkeypatch):
    mock_rset = MagicMock()
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "ALL", user_id="U_RANDO")

    assert resp.status_code == 200
    assert "Not authorized" in resp.json()["text"]
    mock_rset.assert_not_called()


def test_kill_invalid_arg_returns_usage(app_client, monkeypatch):
    mock_rset = MagicMock()
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "BOGUS")

    assert resp.status_code == 200
    assert "Usage" in resp.json()["text"]
    mock_rset.assert_not_called()


# ---------------------------------------------------------------------------
# PR #179 review finding #3 — empty RELAY_APPROVERS must fail CLOSED, not
# fail open. Every test above uses app_client's ["U_APPROVER"] fixture, so
# none of them ever covered the actual default (unset) production
# configuration -- these are new, targeted specifically at that gap.
# ---------------------------------------------------------------------------

def test_empty_approvers_rejects_every_user_on_decision(app_client, monkeypatch):
    """The default RELAY_APPROVERS=[] must NOT mean 'anyone is authorized' --
    it must mean nobody is, until the list is explicitly configured."""
    monkeypatch.setattr("src.api.admin_router.settings.relay_approvers", [])
    mock_record_decision = MagicMock()
    monkeypatch.setattr("src.services.relay.queue.record_decision", mock_record_decision)

    resp = _post_decision(app_client, _interactive_payload("U_ANYONE", 1, "approve"))

    assert resp.status_code == 200
    assert "Not authorized" in resp.json()["text"]
    mock_record_decision.assert_not_called()


def test_empty_approvers_rejects_every_user_on_kill(app_client, monkeypatch):
    """Same fail-closed requirement for the kill command -- an unconfigured
    approver list must not let any workspace member halt the fleet."""
    monkeypatch.setattr("src.api.admin_router.settings.relay_approvers", [])
    mock_rset = MagicMock()
    monkeypatch.setattr("src.core.redis_client.rset", mock_rset)

    resp = _post_kill(app_client, "ALL", user_id="U_ANYONE")

    assert resp.status_code == 200
    assert "Not authorized" in resp.json()["text"]
    mock_rset.assert_not_called()


def test_kill_invalid_signature_returns_401(app_client):
    resp = _post_kill(app_client, "ALL", bad_sig=True)
    assert resp.status_code == 401
