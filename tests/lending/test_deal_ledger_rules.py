"""R1 0.8: ledger rules, the lifecycle endpoint, and the guard that every loan-file write path is classified. No database."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from src.api import lending_deal_ledger_router as ledger_router
from src.lending import state_machine
from src.lending.audit import deal_ledger

REPO = Path(__file__).resolve().parents[2]


# ── Actors ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("actor,source,expected", [
    ("cora", "ghl", "cora"),
    ("human:josh", "api", "human:josh"),
    ("system:router", "api", "system:router"),
    ("slack:U123", "slack", "human:U123"),
    ("slack:U123", "ghl", "human:U123"),
    ("josh", "api", "human:josh"),
    ("Josh Smith", "manual", "human:Josh_Smith"),
    ("ghl", "ghl", "system:ghl"),
    ("tester", "test", "system:test"),
    (None, "ingest", "system:ingest"),
    ("", "api", "system:api"),
])
def test_every_caller_string_becomes_a_tagged_actor(actor, source, expected):
    assert deal_ledger.tag_actor(actor, source=source) == expected


def test_a_tagged_actor_always_passes_the_database_pattern():
    pattern = re.compile(r"^(cora|human:[^ ]+|system:[^ ]+)$")
    for raw in ("cora", "josh", "slack:U1", "Josh Smith", "system:x", None, ""):
        assert pattern.match(deal_ledger.tag_actor(raw, source="api"))


def test_record_refuses_an_untagged_actor_or_an_unknown_type_before_touching_the_database():
    for kwargs in ({"event_type": "stage_transition", "actor": "josh"}, {"event_type": "stage_transition", "actor": ""},
                   {"event_type": "made_up", "actor": "cora"}):
        with pytest.raises(ValueError):
            deal_ledger.record(None, 1, **kwargs)


def test_the_event_types_cover_every_kind_the_design_names():
    assert {"stage_transition", "stage_rejected", "closed", "quote_generated", "field_update", "payment_recorded",
            "cora_draft", "human_edit", "card_approved", "file_created", "status_change", "lender_submission",
            "card_rejected", "doc_quarantined", "doc_released", "doc_type_tagged", "doc_block_override",
            "package_blocked", "package_submitted", "package_dismissed", "doc_superseded",
            "guarantor_added"} == deal_ledger.EVENT_TYPES


# ── Diffs and display ──────────────────────────────────────────────────────────

def test_diff_names_only_changed_fields_and_treats_a_missing_field_as_none():
    assert deal_ledger.diff_fields({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 5, "d": None, "c": None}) == {
        "b": {"before": 2, "after": 5}, "c": {"before": 3, "after": None}}


def test_diff_limited_to_named_fields():
    assert deal_ledger.diff_fields({"a": 1, "b": 1}, {"a": 2, "b": 2}, fields=["b"]) == {"b": {"before": 1, "after": 2}}


def test_an_event_reads_as_one_line():
    line = deal_ledger.describe({
        "occurred_at": datetime(2026, 11, 24, 10, 2, tzinfo=timezone.utc), "event_type": "stage_transition", "actor": "cora",
        "rule_version": 14, "diff": {"stage": {"before": "INTAKE", "after": "FIT"}}})
    assert line == "2026-11-24 10:02  stage_transition  cora                     rules v14   stage: INTAKE -> FIT"


def test_an_event_with_no_rule_version_says_so():
    assert "rules -" in deal_ledger.describe({"occurred_at": "x", "event_type": "file_created", "actor": "system:ingest",
                                              "rule_version": None, "diff": {}})


def test_an_auto_approved_card_is_a_system_actor_and_a_person_is_a_person():
    assert deal_ledger._decider(AUTO_GRADUATED_ACTOR) == ("system:standing_send", True)
    assert deal_ledger._decider("U0JOSH") == ("human:U0JOSH", False)
    assert deal_ledger._decider(None) == ("system:cora_card", False)


# ── Field edits are checked before any query ───────────────────────────────────

def test_only_listed_fields_can_be_edited():
    with pytest.raises(ValueError, match="not editable"):
        state_machine.update_fields(None, 1, {"stage": "FUNDED"}, actor="josh")
    with pytest.raises(ValueError, match="not editable"):
        state_machine.update_fields(None, 1, {"status": "funded", "loan_amount": 5}, actor="josh")


def test_the_editable_fields_do_not_include_the_stage_or_the_audit_columns():
    assert not state_machine.EDITABLE_FIELDS & {"stage", "status", "closed_reason", "closed_at", "borrower_id", "created_at"}


# ── The endpoint ───────────────────────────────────────────────────────────────

class _Result:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _Db:
    def __init__(self, exists=True):
        self.exists = exists

    def execute(self, *_args, **_kwargs):
        return _Result((1,) if self.exists else None)


def _client(monkeypatch, db):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_admin_token", SecretStr("secret-token"), raising=False)
    app = FastAPI()
    app.include_router(ledger_router.router)
    app.dependency_overrides[ledger_router.get_lending_db] = lambda: db
    return TestClient(app, raise_server_exceptions=False)


HEADERS = {"X-Admin-Token": "secret-token"}
URL = "/api/lending/loan-files/7/lifecycle"


def test_the_lifecycle_needs_the_admin_token(monkeypatch):
    client = _client(monkeypatch, _Db())
    assert client.get(URL).status_code == 401
    assert client.get(URL, headers={"X-Admin-Token": "nope"}).status_code == 401


def test_the_lifecycle_lists_events_in_the_order_the_ledger_gives(monkeypatch):
    event = {"id": 1, "event_type": "file_created", "actor": "system:ingest", "rule_version": 1, "diff": {}, "detail": {},
             "ref_table": None, "ref_id": None, "occurred_at": datetime(2026, 11, 24, tzinfo=timezone.utc)}
    monkeypatch.setattr(deal_ledger, "lifecycle", lambda db, fid: [event, {**event, "id": 2, "event_type": "stage_transition"}])
    response = _client(monkeypatch, _Db()).get(URL, headers=HEADERS)
    assert response.status_code == 200 and [e["id"] for e in response.json()] == [1, 2]


def test_an_unknown_file_is_404(monkeypatch):
    response = _client(monkeypatch, _Db(exists=False)).get(URL, headers=HEADERS)
    assert response.status_code == 404 and response.json() == {"detail": "Loan file not found"}


def test_a_failure_is_500_without_the_cause(monkeypatch):
    def boom(db, fid):
        raise RuntimeError("password=hunter2")

    monkeypatch.setattr(deal_ledger, "lifecycle", boom)
    response = _client(monkeypatch, _Db()).get(URL, headers=HEADERS)
    assert response.status_code == 500 and "hunter2" not in response.text


# ── Every loan-file write path is classified ───────────────────────────────────

_WRITE = re.compile(r"(INSERT INTO|UPDATE|DELETE FROM)\s+lending\.loan_files\b")

# file -> number of SQL statements that write lending.loan_files.  A new write path, in any file, changes a count (or
# adds a file) and fails this test until its author has either recorded a ledger event for it or listed it here as
# exempt with a reason.
LOAN_FILE_WRITES = {
    # 5: stage move, close, payment balance, file creation, field edit: each records an event in the same transaction.
    "src/lending/state_machine.py": 5,
    # 2: partner door creates a file (file_created) and moves its status (status_change); both through record_safely.
    "src/lending/partners.py": 2,
    # 3: lender_fit + PDF path (quote_generated); the other two stamp partner-notice delivery times (notice_*), which is
    # delivery bookkeeping, not a change to the deal.
    "src/lending/partner_pipeline.py": 3,
    # 1: a person links the file to its GHL deal (field_update, dedupe key per file and deal).
    "src/lending/ghl_stage_bridge.py": 1,
}
MUST_REFERENCE_THE_LEDGER = ("src/lending/state_machine.py", "src/lending/partners.py", "src/lending/partner_pipeline.py",
                             "src/lending/ghl_stage_bridge.py")


def _writers() -> dict[str, int]:
    found = {}
    for path in (REPO / "src").rglob("*.py"):
        count = len(_WRITE.findall(path.read_text(encoding="utf-8")))
        if count:
            found[path.relative_to(REPO).as_posix()] = count
    return found


def test_every_path_that_writes_a_loan_file_is_classified():
    assert _writers() == LOAN_FILE_WRITES, (
        "a statement that writes lending.loan_files was added or removed: record a deal_ledger event for it "
        "(src/lending/audit/deal_ledger.py) or classify it in LOAN_FILE_WRITES with a reason")


@pytest.mark.parametrize("path", MUST_REFERENCE_THE_LEDGER)
def test_files_that_change_a_deal_call_the_ledger(path):
    assert "deal_ledger." in (REPO / path).read_text(encoding="utf-8")


def test_cora_sweep_runs_the_card_catch_up():
    source = (REPO / "src/lending/cora_agent.py").read_text(encoding="utf-8")
    assert "deal_ledger_sync=make_deal_ledger_step(engine)" in source and "services.deal_ledger_sync()" in source


# ── PR #475 review fixes ───────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,source,expected", [
    ("Josh\tSmith", "api", "human:Josh_Smith"),        # a tab no longer fails the ledger's actor check
    ("human:", "api", "system:api"),                   # a reserved prefix with nothing after it
    ("system:", "webhook", "system:webhook"),
    ("slack:", "api", "system:api"),
    ("human:Josh  Smith", "api", "human:Josh_Smith"),
    ("slack:U0JOSH", "router", "human:U0JOSH"),
    ("cora", "router", "cora"),
])
def test_loose_actor_labels_always_become_a_valid_ledger_actor(raw, source, expected):
    actor = deal_ledger.tag_actor(raw, source=source)
    assert actor == expected and deal_ledger._ACTOR.match(actor)


@pytest.mark.parametrize("actor", ["human:", "system:", "Josh\tSmith", "  "])
def test_the_stage_api_refuses_an_actor_that_names_nobody(actor):
    from pydantic import ValidationError

    from src.api.lending_loan_stage_router import StageIn

    with pytest.raises(ValidationError):
        StageIn(to_stage="FIT", actor=actor)
    assert StageIn(to_stage="FIT", actor="Josh Smith").actor == "Josh Smith"


def test_the_public_quote_api_cannot_attach_a_quote_to_a_loan_file():
    """Review 1: quote credentials authorise quotes, not a loan's history."""
    from src.api.lending_instant_quote_router import QuoteIn

    assert "loan_file_id" not in QuoteIn.model_fields


def test_a_document_chase_ties_itself_to_a_file_only_when_the_phone_names_one():
    """Review 2: with two deals on one number the chase (and its quote) is tied to neither."""
    from src.lending.closing import doc_chase

    sql = str(doc_chase._LOAN_FILE)
    assert "HAVING count(*) = 1" in sql and "ORDER BY created_at DESC" not in sql


def test_each_card_decision_has_its_own_key():
    """Review 3: an automatic approval returned to pending and the person's later decision are two events."""
    source = (REPO / "src/lending/audit/deal_ledger.py").read_text(encoding="utf-8")
    assert ":decided:{card['decided_at'].isoformat()}" in source
