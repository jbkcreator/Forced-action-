"""Disposition webhook end to end against a real database.

Only the FA-table opt-out write and the Sheet/Slack delivery are replaced.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from src.lending import webhooks
from src.lending.api import app
from src.lending.db import get_lending_db

PATH = "/webhooks/aircall/disposition"


@pytest.fixture
def env(monkeypatch, lending_db):
    monkeypatch.setattr(webhooks, "get_settings", lambda: SimpleNamespace(
        lending_aircall_webhook_token=SecretStr("tok"), lending_aircall_line_ids="99"))
    opt_out, deliver = MagicMock(), MagicMock()
    monkeypatch.setattr(webhooks, "propagate_opt_out", opt_out)
    monkeypatch.setattr(webhooks, "deliver_disposition", deliver)
    app.dependency_overrides[get_lending_db] = lambda: lending_db
    yield SimpleNamespace(client=TestClient(app), db=lending_db, opt_out=opt_out, deliver=deliver)
    app.dependency_overrides.clear()


def _send(env, etype, tags=None, call_id="e2e-1", **extra):
    data = {"id": call_id, "direction": "outbound", "raw_digits": "813-555-0177",
            "started_at": 1790000000, "ended_at": 1790000060, "duration": 30,
            "user": {"id": 5, "name": "Caller"}, "number": {"id": 99}, **extra}
    if tags is not None:
        data["tags"] = [{"name": t} for t in tags]
    return env.client.post(PATH, content=json.dumps({"event": etype, "token": "tok", "data": data}),
                           headers={"Content-Type": "application/json"})


def _row(env, call_id="e2e-1"):
    return env.db.execute(
        text("SELECT * FROM lending.call_dispositions WHERE aircall_call_id = :c"), {"c": call_id}
    ).mappings().one()


def test_call_then_tag_is_one_row_and_schedules_delivery(env):
    assert _send(env, "call.ended").status_code == 200
    assert _row(env)["disposition"] is None
    env.deliver.assert_not_called()

    assert _send(env, "call.tagged", tags=["QUALIFIED_APPOINTMENT"]).status_code == 200
    row = _row(env)
    assert row["disposition"] == "QUALIFIED_APPOINTMENT"
    assert row["phone"] == "+18135550177"
    env.deliver.assert_called_once_with(row["id"])
    env.opt_out.assert_not_called()


def test_dnc_request_propagates_once_even_when_the_event_is_replayed(env):
    _send(env, "call.ended")
    for _ in range(2):
        assert _send(env, "call.tagged", tags=["DNC_REQUEST"]).status_code == 200
    row = _row(env)
    assert row["opt_out_propagated_at"] is not None
    env.opt_out.assert_called_once_with(
        env.db, phone="+18135550177", source_ref="e2e-1", actor="5")


def test_failed_opt_out_returns_500_and_a_redelivery_retries_it(env):
    _send(env, "call.ended")
    env.opt_out.side_effect = RuntimeError("fa tables unavailable")
    assert _send(env, "call.tagged", tags=["DNC_REQUEST"]).status_code == 500
    assert _row(env)["opt_out_propagated_at"] is None
    assert _row(env)["disposition"] == "DNC_REQUEST"  # the call itself is still saved

    env.opt_out.side_effect = None
    assert _send(env, "call.tagged", tags=["DNC_REQUEST"]).status_code == 200
    assert _row(env)["opt_out_propagated_at"] is not None


def test_event_from_another_line_leaves_no_row(env):
    assert _send(env, "call.ended", number={"id": 555}).status_code == 200
    count = env.db.execute(text("SELECT count(*) FROM lending.call_dispositions")).scalar()
    assert count == 0


# ── delivery bookkeeping and retry, real SQL ────────────────────────────────

def _delivery_env(monkeypatch, db):
    from contextlib import contextmanager

    from src.lending import disposition_delivery as dd

    monkeypatch.setattr(dd, "get_settings", lambda: SimpleNamespace(
        lending_disposition_sheet_id="s", lending_disposition_sheet_tab="Dispositions",
        lending_dial_tasks_channel="C1", lending_slack_bot_token=SecretStr("x"),
        lending_sheets_service_account_key_path="/k"))

    @contextmanager
    def factory():
        yield db

    slack = MagicMock()
    slack.chat_postMessage.return_value = {"ts": "9.9"}
    values = MagicMock()
    values.get.return_value.execute.return_value = {"values": [["Call ID"]]}
    sheets = MagicMock()
    sheets.spreadsheets.return_value.values.return_value = values
    return dd, factory, slack, sheets, values


def test_delivery_records_what_was_sent_and_does_not_resend(env, monkeypatch):
    from src.lending.dispositions import record_aircall_event

    dd, factory, slack, sheets, values = _delivery_env(monkeypatch, env.db)
    rec = record_aircall_event(env.db, "call.tagged", {
        "id": "d1", "raw_digits": "813-555-0177", "number": {"id": 99},
        "tags": [{"name": "CONNECTED"}], "user": {"id": 5, "name": "Caller"}})

    dd.deliver_disposition(rec.row_id, session_factory=factory, slack_client=slack, sheets_service=sheets)
    row = _row(env, "d1")
    assert row["slack_posted_disposition"] == "CONNECTED" and row["slack_ts"] == "9.9"
    assert row["sheet_synced_disposition"] == "CONNECTED"
    assert row["slack_posted_at"] is not None and row["sheet_synced_at"] is not None

    dd.deliver_disposition(rec.row_id, session_factory=factory, slack_client=slack, sheets_service=sheets)
    slack.chat_postMessage.assert_called_once()

    record_aircall_event(env.db, "call.tagged", {
        "id": "d1", "number": {"id": 99}, "tags": [{"name": "CONNECTED"}, {"name": "DNC_REQUEST"}]})
    dd.deliver_disposition(rec.row_id, session_factory=factory, slack_client=slack, sheets_service=sheets)
    assert slack.chat_update.call_args.kwargs["ts"] == "9.9"
    assert _row(env, "d1")["slack_posted_disposition"] == "DNC_REQUEST"


def test_removed_result_is_picked_up_by_retry_and_cleared_in_the_database(env, monkeypatch):
    from contextlib import contextmanager

    from src.tasks import lending_disposition_delivery_retry as retry

    @contextmanager
    def factory():
        yield env.db

    monkeypatch.setattr(retry, "lending_session", factory)
    _send(env, "call.tagged", tags=["CONNECTED"], call_id="rm1")
    env.db.execute(text(
        "UPDATE lending.call_dispositions SET sheet_synced_disposition = 'CONNECTED', "
        "slack_posted_disposition = 'CONNECTED', slack_ts = '1.1' WHERE aircall_call_id = 'rm1'"))
    assert _send(env, "call.untagged", tags=[], call_id="rm1").status_code == 200
    row = _row(env, "rm1")
    assert row["disposition"] is None
    env.db.execute(text(
        "UPDATE lending.call_dispositions SET disposition_at = now() - interval '10 minutes' "
        "WHERE aircall_call_id = 'rm1'"))
    assert retry.behind_row_ids() == [row["id"]]
    env.deliver.assert_called_with(row["id"])  # scheduled straight away, not only by the retry


def test_retry_selects_only_calls_that_are_behind_and_old_enough(env, monkeypatch):
    from contextlib import contextmanager

    from src.lending.dispositions import record_aircall_event
    from src.tasks import lending_disposition_delivery_retry as retry

    @contextmanager
    def factory():
        yield env.db

    monkeypatch.setattr(retry, "lending_session", factory)
    ids = {}
    for name in ("behind_old", "behind_new", "synced_old", "no_result"):
        tags = None if name == "no_result" else [{"name": "CONNECTED"}]
        data = {"id": name, "number": {"id": 99}, "raw_digits": "813-555-0177"}
        if tags:
            data["tags"] = tags
        ids[name] = record_aircall_event(env.db, "call.tagged", data).row_id
    env.db.execute(text(
        "UPDATE lending.call_dispositions SET disposition_at = now() - interval '10 minutes' "
        "WHERE aircall_call_id IN ('behind_old', 'synced_old')"))
    env.db.execute(text(
        "UPDATE lending.call_dispositions SET sheet_synced_disposition = 'CONNECTED', "
        "slack_posted_disposition = 'CONNECTED' WHERE aircall_call_id = 'synced_old'"))

    assert retry.behind_row_ids() == [ids["behind_old"]]
