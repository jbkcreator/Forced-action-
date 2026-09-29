"""Disposition webhook: auth, filtering, compliance hooks and error handling.

Database-free: the recording step and the compliance hooks are replaced.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from src.lending import webhooks
from src.lending.api import app
from src.lending.db import get_lending_db
from src.lending.dispositions import RecordedCall

TOKEN = "test-token"
PATH = "/webhooks/aircall/disposition"


@pytest.fixture
def db():
    return MagicMock()


@pytest.fixture
def client(monkeypatch, db):
    monkeypatch.setattr(webhooks, "get_settings", lambda: SimpleNamespace(
        lending_aircall_webhook_token=SecretStr(TOKEN), lending_aircall_line_ids="99, 100"))
    app.dependency_overrides[get_lending_db] = lambda: db
    yield TestClient(app)
    app.dependency_overrides.clear()


def _event(etype="call.ended", token=TOKEN, line=99, call_id="c1", **data):
    body = {"event": etype, "token": token,
            "data": {"id": call_id, "number": {"id": line}, **data}}
    return body


def _post(client, body, signature=None):
    raw = json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if signature:
        headers["X-Aircall-Signature"] = signature
    return client.post(PATH, content=raw, headers=headers)


def _recorded(**over):
    base = dict(row_id=1, call_id="c1", etype="call.ended", phone="+18135550142",
                caller_seat="7", disposition=None, opt_out_propagated=False)
    base.update(over)
    return RecordedCall(**base)


@pytest.fixture
def hooks(monkeypatch):
    calls = SimpleNamespace(
        record=MagicMock(return_value=_recorded()),
        attempt=MagicMock(), opt_out=MagicMock(), deliver=MagicMock())
    monkeypatch.setattr(webhooks, "record_aircall_event", calls.record)
    monkeypatch.setattr(webhooks, "on_attempt_recorded", calls.attempt)
    monkeypatch.setattr(webhooks, "propagate_opt_out", calls.opt_out)
    monkeypatch.setattr(webhooks, "deliver_disposition", calls.deliver)
    return calls


# ── auth and parsing ────────────────────────────────────────────────────────

def test_wrong_token_is_401_and_nothing_is_recorded(client, hooks):
    r = _post(client, _event(token="wrong"))
    assert r.status_code == 401
    assert r.json() == {"detail": "invalid signature"}
    hooks.record.assert_not_called()


def test_missing_token_is_401(client, hooks):
    body = _event()
    body.pop("token")
    assert _post(client, body).status_code == 401


def test_valid_body_token_is_accepted(client, hooks):
    assert _post(client, _event()).status_code == 200
    hooks.record.assert_called_once()


def test_valid_hmac_header_is_accepted(client, hooks):
    body = _event(token=None)
    raw = json.dumps(body).encode()
    sig = hmac.new(TOKEN.encode(), raw, hashlib.sha256).hexdigest()
    r = client.post(PATH, content=raw, headers={"X-Aircall-Signature": sig, "Content-Type": "application/json"})
    assert r.status_code == 200


def test_invalid_json_without_valid_auth_is_401(client, hooks):
    r = client.post(PATH, content=b"{not json", headers={"Content-Type": "application/json"})
    assert r.status_code == 401


def test_invalid_json_with_valid_signature_is_400(client, hooks):
    raw = b"{not json"
    sig = hmac.new(TOKEN.encode(), raw, hashlib.sha256).hexdigest()
    r = client.post(PATH, content=raw, headers={"X-Aircall-Signature": sig, "Content-Type": "application/json"})
    assert r.status_code == 400
    assert r.json() == {"detail": "invalid payload"}


def test_missing_call_id_is_400(client, hooks):
    body = _event()
    body["data"].pop("id")
    assert _post(client, body).status_code == 400


def test_unhandled_event_is_200_and_ignored(client, hooks):
    assert _post(client, _event(etype="call.answered")).status_code == 200
    hooks.record.assert_not_called()


def test_event_on_a_non_lending_line_is_ignored(client, hooks):
    assert _post(client, _event(line=555)).status_code == 200
    hooks.record.assert_not_called()


def test_no_configured_lines_ignores_everything(client, hooks, monkeypatch):
    monkeypatch.setattr(webhooks, "get_settings", lambda: SimpleNamespace(
        lending_aircall_webhook_token=SecretStr(TOKEN), lending_aircall_line_ids=""))
    assert _post(client, _event()).status_code == 200
    hooks.record.assert_not_called()


# ── compliance hooks ────────────────────────────────────────────────────────

def test_call_ended_runs_the_attempt_hook(client, hooks, db):
    assert _post(client, _event()).status_code == 200
    hooks.attempt.assert_called_once_with(db, "+18135550142")


def test_call_tagged_does_not_run_the_attempt_hook(client, hooks):
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="CONNECTED")
    _post(client, _event(etype="call.tagged"))
    hooks.attempt.assert_not_called()


def test_dnc_request_propagates_once_with_call_id_and_seat(client, hooks, db):
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="DNC_REQUEST")
    assert _post(client, _event(etype="call.tagged")).status_code == 200
    hooks.opt_out.assert_called_once_with(
        db, phone="+18135550142", source_ref="c1", actor="7")


def test_dnc_tag_propagates_even_when_another_result_tag_won(client, hooks, db):
    hooks.record.return_value = _recorded(
        etype="call.tagged", disposition="CONNECTED", dnc_tagged=True)
    _post(client, _event(etype="call.tagged"))
    hooks.opt_out.assert_called_once()


def test_dnc_already_propagated_is_not_repeated(client, hooks):
    hooks.record.return_value = _recorded(
        etype="call.tagged", disposition="DNC_REQUEST", opt_out_propagated=True)
    _post(client, _event(etype="call.tagged"))
    hooks.opt_out.assert_not_called()


def test_other_dispositions_do_not_propagate(client, hooks):
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="CONNECTED")
    _post(client, _event(etype="call.tagged"))
    hooks.opt_out.assert_not_called()


# ── failures and delivery ───────────────────────────────────────────────────

def test_hook_failure_is_500_so_aircall_retries(client, hooks, db):
    hooks.attempt.side_effect = RuntimeError("boom")
    r = _post(client, _event())
    assert r.status_code == 500
    assert r.json() == {"detail": "processing failed"}
    assert "boom" not in r.text
    db.rollback.assert_called()


def test_database_error_is_503(client, hooks):
    hooks.record.side_effect = OperationalError("stmt", {}, Exception("down"))
    assert _post(client, _event()).status_code == 503


def test_removed_result_still_schedules_delivery(client, hooks):
    hooks.record.return_value = _recorded(etype="call.untagged", disposition=None)
    _post(client, _event(etype="call.untagged"))
    hooks.deliver.assert_called_once_with(1)


def test_dnc_without_a_usable_phone_raises_a_slack_alert(client, hooks, monkeypatch):
    alert = MagicMock()
    monkeypatch.setattr(webhooks, "alert_unpropagated_dnc", alert)
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="DNC_REQUEST", phone=None)
    assert _post(client, _event(etype="call.tagged")).status_code == 200
    alert.assert_called_once_with("c1", "7")
    hooks.opt_out.assert_not_called()


def test_dnc_with_a_phone_raises_no_alert(client, hooks, monkeypatch):
    alert = MagicMock()
    monkeypatch.setattr(webhooks, "alert_unpropagated_dnc", alert)
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="DNC_REQUEST")
    _post(client, _event(etype="call.tagged"))
    alert.assert_not_called()


def test_delivery_is_scheduled_when_a_disposition_exists(client, hooks):
    hooks.record.return_value = _recorded(etype="call.tagged", disposition="CONNECTED")
    _post(client, _event(etype="call.tagged"))
    hooks.deliver.assert_called_once_with(1)


def test_no_delivery_without_a_disposition(client, hooks):
    _post(client, _event())
    hooks.deliver.assert_not_called()


# ── role script ─────────────────────────────────────────────────────────────

def test_role_grants_are_least_privilege():
    from migrations.apply_lending_app_role import statements

    stmts = statements("db", "app_owner")
    writes = [s for s in stmts if s.startswith("GRANT INSERT") or "UPDATE" in s or "DELETE" in s]
    assert sorted(writes) == [
        "GRANT INSERT ON public.email_opt_outs TO lending_app",
        "GRANT INSERT ON public.sms_opt_outs TO lending_app",
    ]
    assert not any("ALL ON ALL TABLES IN SCHEMA public" in s for s in stmts)
    assert 'FOR ROLE "app_owner" IN SCHEMA lending' in " ".join(stmts)
