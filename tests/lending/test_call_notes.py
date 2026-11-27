"""R1 1.6: the Slack notes form for connected calls with no recording (SQLite with an attached ``lending`` schema)."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from config.lending_dialer_queue import NOTES_CALLBACK_ID, NOTES_OPEN_ACTION_ID
from src.lending import call_extraction_store
from src.lending.call_extraction import CallExtraction
from src.lending.dialer import call_notes
from src.lending.dialer.call_notes import (
    build_notes_view, handle_notes_interaction, notes_button, parse_notes, save_notes,
)

NOW = datetime(2026, 11, 25, 19, 0, tzinfo=timezone.utc)
_DDL = (
    "CREATE TABLE lending.call_dispositions (dialer_call_id TEXT PRIMARY KEY, phone TEXT)",
    "CREATE TABLE lending.call_extractions (dialer_call_id TEXT PRIMARY KEY, phone TEXT, fields TEXT NOT NULL,"
    " extracted_at TIMESTAMP NOT NULL, source TEXT NOT NULL DEFAULT 'transcript', entered_by TEXT)",
)


@pytest.fixture
def db():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS lending")

    with eng.begin() as conn:
        for statement in _DDL:
            conn.execute(text(statement))
        conn.execute(text("INSERT INTO lending.call_dispositions VALUES ('c1', '+18135550142')"))
    with Session(eng) as session:
        yield session
    eng.dispose()


@pytest.fixture
def slack(monkeypatch, db):
    state = SimpleNamespace(enabled=True, opened=[], ephemeral=[])
    monkeypatch.setattr(call_notes, "get_settings", lambda: SimpleNamespace(
        lending_caller_scorecard_enabled=state.enabled, lending_david_slack_user_id="U_DAVID",
        lending_cora_approver_user_ids="U_JOSH, U_ADMIN"))
    client = SimpleNamespace(views_open=lambda **kw: state.opened.append(kw),
                             chat_postEphemeral=lambda **kw: state.ephemeral.append(kw))
    monkeypatch.setattr(call_notes, "slack_client", lambda: client)

    @contextmanager
    def session():
        yield db
        db.commit()

    monkeypatch.setattr(call_notes, "lending_session", session)
    return state


def state(**fields):
    values = {}
    for name, value in fields.items():
        if name in ("deal_status", "credit_band", "has_liquidity"):
            values[name] = {"value": {"type": "static_select", "selected_option": {"value": value} if value else None}}
        elif name == "deadline":
            values[name] = {"value": {"type": "datepicker", "selected_date": value}}
        else:
            values[name] = {"value": {"type": "plain_text_input", "value": value}}
    return values


def submission(user="U_DAVID", call="c1", **fields):
    return {"type": "view_submission", "user": {"id": user}, "view": {
        "callback_id": NOTES_CALLBACK_ID,
        "private_metadata": json.dumps({"call_id": call, "channel": "C1", "message_ts": "1.1"}),
        "state": {"values": state(**fields)}}}


def row(db, call):
    return db.execute(text("SELECT source, entered_by FROM lending.call_extractions WHERE dialer_call_id = :c"),
                      {"c": call}).first()


def test_parse_notes_requires_next_action():
    ext, errors = parse_notes(state(next_action=""))
    assert ext is None and "next_action" in errors


def test_parse_notes_reads_every_field():
    ext, errors = parse_notes(state(
        next_action="send docs", deadline="2026-12-01", decision_maker="the owner", objection="rate too high",
        deal_status="under_contract", credit_band="below_640", completed_projects_3y="3", has_liquidity="yes",
        property_address="1 Main St", target_market="Tampa", referral_names="Ann\nBo\n",
        missing_file_items="W9\n\nbank statement"))
    assert errors == {}
    assert ext == CallExtraction(
        next_action="send docs", deadline=date(2026, 12, 1), decision_maker="the owner", objection="rate too high",
        deal_status="under_contract", credit_band="below_640", completed_projects_3y=3, has_liquidity=True,
        property_address="1 Main St", target_market="Tampa", referral_names=["Ann", "Bo"],
        missing_file_items=["W9", "bank statement"])


def test_parse_notes_rejects_a_bad_project_count():
    ext, errors = parse_notes(state(next_action="x", completed_projects_3y="many"))
    assert ext is None and "completed_projects_3y" in errors


def test_parse_notes_unknown_liquidity_is_none():
    ext, _ = parse_notes(state(next_action="x", has_liquidity="unknown"))
    assert ext.has_liquidity is None


def test_notes_saved_as_caller_notes(db):
    ext, _ = parse_notes(state(next_action="send docs"))
    assert save_notes(db, dialer_call_id="c1", extraction=ext, user_id="U_DAVID", now=NOW) == "saved"
    assert row(db, "c1") == ("caller_notes", "U_DAVID")
    stored = db.execute(text("SELECT phone, fields FROM lending.call_extractions")).first()
    assert stored[0] == "+18135550142" and json.loads(stored[1])["next_action"] == "send docs"


def test_newer_notes_replace_older_notes(db):
    first, _ = parse_notes(state(next_action="one"))
    second, _ = parse_notes(state(next_action="two"))
    save_notes(db, dialer_call_id="c1", extraction=first, user_id="U_DAVID", now=NOW)
    assert save_notes(db, dialer_call_id="c1", extraction=second, user_id="U_JOSH", now=NOW) == "saved"
    assert row(db, "c1") == ("caller_notes", "U_JOSH")


def test_notes_refused_when_call_has_transcript_extraction(db):
    db.execute(text("INSERT INTO lending.call_extractions (dialer_call_id, fields, extracted_at)"
                    " VALUES ('c1', '{\"next_action\": \"from transcript\"}', :t)"), {"t": NOW})
    ext, _ = parse_notes(state(next_action="mine"))
    assert save_notes(db, dialer_call_id="c1", extraction=ext, user_id="U_DAVID", now=NOW) == "has_transcript"
    assert row(db, "c1")[0] == "transcript"
    assert "from transcript" in db.execute(text("SELECT fields FROM lending.call_extractions")).scalar()


def test_notes_for_unknown_call(db):
    ext, _ = parse_notes(state(next_action="x"))
    assert save_notes(db, dialer_call_id="nope", extraction=ext, user_id="U_DAVID", now=NOW) == "unknown_call"


def test_transcript_extraction_takes_over_a_notes_row():
    class Recorder:
        def execute(self, sql, params):
            self.sql = str(sql)
    recorder = Recorder()
    call_extraction_store.save_call_extraction(recorder, dialer_call_id="c1", phone=None,
                                               extraction=CallExtraction(), extracted_at=NOW)
    assert "source = 'transcript'" in recorder.sql and "entered_by = NULL" in recorder.sql


def test_submission_saves_and_closes_the_form(slack, db):
    assert handle_notes_interaction(submission(next_action="send docs")) == {}
    assert row(db, "c1") == ("caller_notes", "U_DAVID")


def test_notes_refused_for_user_not_allowed(slack, db):
    resp = handle_notes_interaction(submission(user="U_OTHER", next_action="x"))
    assert resp["response_action"] == "errors" and row(db, "c1") is None


def test_submission_errors_are_shown_under_their_field(slack, db):
    resp = handle_notes_interaction(submission(next_action=""))
    assert resp["response_action"] == "errors" and "next_action" in resp["errors"]


def test_submission_for_a_call_with_a_transcript_says_so(slack, db):
    db.execute(text("INSERT INTO lending.call_extractions (dialer_call_id, fields, extracted_at)"
                    " VALUES ('c1', '{}', :t)"), {"t": NOW})
    resp = handle_notes_interaction(submission(next_action="x"))
    assert resp["response_action"] == "errors" and "transcript" in " ".join(resp["errors"].values())


def test_open_button_opens_the_form_for_an_allowed_user(slack):
    payload = {"type": "block_actions", "user": {"id": "U_JOSH"}, "trigger_id": "T1",
               "channel": {"id": "C1"}, "message": {"ts": "1.1"},
               "actions": [{"action_id": NOTES_OPEN_ACTION_ID, "value": "c1"}]}
    assert handle_notes_interaction(payload) == {}
    view = slack.opened[0]["view"]
    assert view["callback_id"] == NOTES_CALLBACK_ID and json.loads(view["private_metadata"])["call_id"] == "c1"


def test_open_button_refused_for_a_user_not_allowed(slack):
    payload = {"type": "block_actions", "user": {"id": "U_OTHER"}, "trigger_id": "T1", "channel": {"id": "C1"},
               "actions": [{"action_id": NOTES_OPEN_ACTION_ID, "value": "c1"}]}
    assert handle_notes_interaction(payload) == {}
    assert slack.opened == [] and len(slack.ephemeral) == 1


def test_switch_off_does_nothing(slack, db):
    slack.enabled = False
    assert handle_notes_interaction(submission(next_action="x")) == {}
    assert row(db, "c1") is None


def test_other_payloads_are_not_ours(slack):
    assert handle_notes_interaction({"type": "block_actions", "actions": [{"action_id": "x"}]}) is None
    assert handle_notes_interaction({"type": "view_submission", "view": {"callback_id": "other"}}) is None


def test_button_and_form_shape():
    button = notes_button("c1")
    assert button["action_id"] == NOTES_OPEN_ACTION_ID and button["value"] == "c1"
    view = build_notes_view(dialer_call_id="c1", channel="C1", message_ts="1.1")
    ids = [b["block_id"] for b in view["blocks"]]
    assert "next_action" in ids and "kill_reason_verbatim" not in ids
    required = [b["block_id"] for b in view["blocks"] if not b.get("optional")]
    assert required == ["next_action"]


# ---- the lending app's interactivity route dispatches to the notes form ------------------------------------------

def _client(monkeypatch, *, soft_approval: bool):
    from urllib.parse import urlencode

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.lending import soft_approval_webhook as webhook

    monkeypatch.setattr(webhook, "get_settings", lambda: SimpleNamespace(
        lending_slack_signing_secret=SimpleNamespace(get_secret_value=lambda: "s"),
        lending_soft_approval_enabled=soft_approval))
    monkeypatch.setattr(webhook, "verify_slack_signature", lambda *a: True)
    app = FastAPI()
    app.include_router(webhook.router)
    client = TestClient(app)
    return lambda payload: client.post("/webhooks/lending/slack-interactivity",
                                       content=urlencode({"payload": json.dumps(payload)}).encode()).json()


def test_route_returns_the_notes_forms_response_even_with_soft_approval_off(monkeypatch):
    post = _client(monkeypatch, soft_approval=False)
    from src.lending import soft_approval_webhook as webhook
    monkeypatch.setattr(webhook, "handle_notes_interaction",
                        lambda payload: {"response_action": "errors", "errors": {"next_action": "x"}})
    assert post({"type": "view_submission"})["response_action"] == "errors"


def test_route_leaves_other_payloads_to_soft_approval(monkeypatch):
    post = _client(monkeypatch, soft_approval=False)
    assert post({"type": "block_actions", "actions": [{"action_id": "soft_approval_open"}]}) == {}
