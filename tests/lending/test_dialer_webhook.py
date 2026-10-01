"""Dialer call webhook: auth, filtering, compliance hooks and error handling.

Database-free: the recording step and the compliance hooks are replaced.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from src.lending import call_pipeline, webhooks
from src.lending.db import get_lending_db
from src.lending.dispositions import RecordedCall

app = FastAPI()
app.include_router(webhooks.router)

SECRET = "test-secret"
PATH = "/webhooks/lending/dialer"


@pytest.fixture
def db():
    return MagicMock()


@pytest.fixture
def client(monkeypatch, db):
    monkeypatch.setattr(webhooks, "get_settings", lambda: SimpleNamespace(lending_dialer_webhook_secret=SecretStr(SECRET)))
    monkeypatch.setattr(call_pipeline, "get_settings", lambda: SimpleNamespace(lending_dialer_campaign_ids="55, 56"))
    app.dependency_overrides[get_lending_db] = lambda: db
    yield TestClient(app)
    app.dependency_overrides.clear()


def _post(client, body, secret=SECRET, query=""):
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Webhook-Secret"] = secret
    return client.post(PATH + query, content=json.dumps(body).encode(), headers=headers)


def _event(call_id="c1", campaign="55", **extra):
    return {"call_id": call_id, "campaign_id": campaign, "phone": "8135550142", **extra}


def _recorded(**over):
    base = dict(row_id=1, call_id="c1", phone="+18135550142", caller_seat="7",
                disposition=None, opt_out_propagated=False, call_ended=True)
    base.update(over)
    return RecordedCall(**base)


@pytest.fixture
def hooks(monkeypatch):
    calls = SimpleNamespace(
        record=MagicMock(return_value=_recorded()), attempt=MagicMock(), opt_out=MagicMock(),
        deliver=MagicMock(), unknown=MagicMock(), dnc_alert=MagicMock(), pending=MagicMock())
    monkeypatch.setattr(call_pipeline, "record_dialer_event", calls.record)
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", calls.attempt)
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", calls.opt_out)
    monkeypatch.setattr(call_pipeline, "deliver_disposition", calls.deliver)
    monkeypatch.setattr(call_pipeline, "alert_unknown_code", calls.unknown)
    monkeypatch.setattr(call_pipeline, "alert_unpropagated_dnc", calls.dnc_alert)
    monkeypatch.setattr(call_pipeline, "alert_dnc_removal_pending", calls.pending)
    return calls


def test_wrong_or_missing_secret_is_401_and_nothing_is_recorded(client, hooks):
    assert _post(client, _event(), secret="wrong").status_code == 401
    assert _post(client, _event(), secret=None).status_code == 401
    hooks.record.assert_not_called()


def test_unset_server_secret_rejects_everything(monkeypatch, client, hooks):
    monkeypatch.setattr(webhooks, "get_settings", lambda: SimpleNamespace(lending_dialer_webhook_secret=None))
    assert _post(client, _event(), secret="anything").status_code == 401


def test_secret_in_the_query_string_is_accepted(client, hooks):
    assert _post(client, _event(), secret=None, query=f"?token={SECRET}").status_code == 200


def test_unparseable_or_id_less_payloads_are_400(client, hooks):
    assert client.post(PATH, content=b"{not json", headers={"X-Webhook-Secret": SECRET}).status_code == 400
    assert _post(client, {"campaign_id": "55"}).status_code == 400
    assert _post(client, ["list"]).status_code == 400


def test_non_lending_campaign_is_ignored_with_200(client, hooks):
    r = _post(client, _event(campaign="999"))
    assert r.status_code == 200
    hooks.record.assert_not_called()


def test_event_without_a_campaign_is_ignored(client, hooks):
    body = _event()
    del body["campaign_id"]
    assert _post(client, body).status_code == 200
    hooks.record.assert_not_called()


def test_ended_call_records_attempt_then_delivers(client, hooks, db):
    assert _post(client, _event()).status_code == 200
    hooks.record.assert_called_once()
    hooks.attempt.assert_called_once_with(db, "+18135550142")
    hooks.deliver.assert_called_once_with(1)


def test_dnc_request_propagates_once_and_marks_the_row(client, hooks, db):
    hooks.record.return_value = _recorded(disposition="DNC_REQUEST", dnc_requested=True)
    _post(client, _event())
    hooks.opt_out.assert_called_once_with(db, phone="+18135550142", source_ref="c1", actor="7")

    hooks.opt_out.reset_mock()
    hooks.record.return_value = _recorded(disposition="DNC_REQUEST", dnc_requested=True, opt_out_propagated=True)
    _post(client, _event())
    hooks.opt_out.assert_not_called()


def test_dnc_request_without_a_phone_alerts_slack(client, hooks):
    hooks.record.return_value = _recorded(phone=None, disposition="DNC_REQUEST", dnc_requested=True)
    _post(client, _event())
    hooks.opt_out.assert_not_called()
    hooks.dnc_alert.assert_called_once()


def test_unknown_code_is_alerted(client, hooks):
    hooks.record.return_value = _recorded(unknown_code="Hot Lead")
    _post(client, _event())
    hooks.unknown.assert_called_once()


def test_database_outage_is_503_and_other_failures_500_so_the_dialer_redelivers(client, hooks):
    hooks.record.side_effect = OperationalError("x", {}, Exception("down"))
    assert _post(client, _event()).status_code == 503
    hooks.record.side_effect = RuntimeError("boom")
    assert _post(client, _event()).status_code == 500


def test_dnc_whose_dialer_removal_is_unconfirmed_alerts_slack(client, hooks, db):
    hooks.record.return_value = _recorded(disposition="DNC_REQUEST", dnc_requested=True)
    hooks.opt_out.return_value = 42
    db.execute.return_value.scalar.return_value = "dialer_pending"
    _post(client, _event())
    hooks.pending.assert_called_once_with("c1", "7")


def test_dnc_removed_from_the_dialer_sends_no_pending_alert(client, hooks, db):
    hooks.record.return_value = _recorded(disposition="DNC_REQUEST", dnc_requested=True)
    hooks.opt_out.return_value = 42
    db.execute.return_value.scalar.return_value = "complete"
    _post(client, _event())
    hooks.pending.assert_not_called()


def test_follow_up_schedules_delivery_and_alerts_through_the_given_runner():
    from src.lending import call_pipeline as cp

    scheduled = []
    rec = _recorded(unknown_code="Hot Lead", dnc_requested=True, phone=None, dnc_removal_pending=True)
    cp.follow_up(rec, lambda fn, *args: scheduled.append((fn.__name__, args)))
    names = [n for n, _ in scheduled]
    assert names == ["deliver_disposition", "alert_unknown_code", "alert_unpropagated_dnc", "alert_dnc_removal_pending"]
