"""R1 0.5 T3: /ghl-stage on the inbound journal, with a GHL read-through of the deal's current stage (no database).

The handler's database writes (stage event, current stage, loan bridge) are replaced by an in-memory book; the GHL
reader is injected. DB-backed versions of the same scenarios: test_inbound_ghl_stage_db.py.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.lending.ghl_account import GhlAccount
from src.lending.inbound import ghl_stage

ACCOUNT = GhlAccount(api_key="k", location_id="loc-1")
PIPELINES = {"pipelines": [{"id": "pipe-1", "name": "NDL", "stages": [
    {"id": "st-sub", "name": "Submitted to lender"}, {"id": "st-fund", "name": "Funded"}]}]}


class _Http:
    """GHL by path: GET /opportunities/{id} and GET /opportunities/pipelines."""

    def __init__(self, opportunity=None, status=200, error=None):
        self.opportunity, self.status, self.error, self.calls = opportunity, status, error, []

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.error:
            raise self.error
        if url.endswith("/opportunities/pipelines"):
            return SimpleNamespace(status_code=200, json=lambda: PIPELINES)
        return SimpleNamespace(status_code=self.status, json=lambda: {"opportunity": self.opportunity})


# ---- GHL reader ---------------------------------------------------------------------------------------------------

def test_reader_names_the_opportunitys_current_stage_from_its_pipeline():
    http = _Http({"id": "opp-1", "pipelineId": "pipe-1", "pipelineStageId": "st-fund", "locationId": "loc-1"})
    stage = ghl_stage.GhlStageReader(ACCOUNT, request=http)("opp-1")
    assert stage == ghl_stage.GhlStage(stage_name="Funded", pipeline_id="pipe-1")
    assert http.calls[0][0] == "GET" and http.calls[0][1].endswith("/opportunities/opp-1")


@pytest.mark.parametrize("http", [
    _Http(status=404), _Http(status=500), _Http(error=ConnectionError("boom")),
    _Http({"id": "opp-1", "pipelineId": "pipe-1", "pipelineStageId": "st-unknown"}),
])
def test_reader_raises_when_ghl_cannot_say(http):
    with pytest.raises(ghl_stage.GhlStageReadError):
        ghl_stage.GhlStageReader(ACCOUNT, request=http)("opp-1")


# ---- handler (journal source "ghl_stage") ----------------------------------------------------------------------

class _Book:
    """Stage events, each deal's current stage and the linked loan file, kept in memory. A repeat event never moves
    the deal (the 1 h redelivery hold of blockers.upsert_current_stage, simplified: every repeat is within the hour)."""

    def __init__(self):
        self.events: list[tuple[str, str]] = []
        self.current: dict[str, str] = {}
        self.loan: dict[str, str] = {}
        self.bridge: list[tuple[str, str]] = []
        self.held: list[str] = []

    def insert_event(self, db, *, opportunity_id, stage, **_):
        if (opportunity_id, stage.strip().lower()) in self.events:
            return False
        self.events.append((opportunity_id, stage.strip().lower()))
        return True

    def upsert(self, db, *, opportunity_id, stage_name, phone, now, repeat=False):
        key = stage_name.strip().lower()
        if repeat and self.current.get(opportunity_id) not in (None, key):
            return False
        self.current[opportunity_id] = key
        return True

    def apply_stage(self, db, *, opportunity_id, stage_name, phone=None):
        key = stage_name.strip().lower()
        if key not in ("funded", "paid"):
            return None
        if self.current.get(opportunity_id) != key:
            return "stale"
        self.bridge.append((opportunity_id, key))
        if self.loan.get(opportunity_id) == key:
            return "noop"
        self.loan[opportunity_id] = key
        return "applied"


@pytest.fixture
def book(monkeypatch):
    b = _Book()
    monkeypatch.setattr(ghl_stage, "_insert_stage_event", b.insert_event)
    monkeypatch.setattr(ghl_stage, "upsert_current_stage", b.upsert)
    monkeypatch.setattr(ghl_stage, "apply_stage", b.apply_stage)
    monkeypatch.setattr(ghl_stage, "current_stage", lambda db, opp: (b.current[opp], None) if opp in b.current else None)
    monkeypatch.setattr(ghl_stage, "add_to_inbox_safely", lambda db, **kw: b.held.append(kw["dedup_key"]))
    return b


class _Db:
    def begin_nested(self):
        from contextlib import nullcontext
        return nullcontext()


def _ghl_says(stage):
    seen = []

    def read(opportunity_id):
        seen.append(opportunity_id)
        if isinstance(stage, Exception):
            raise stage
        return ghl_stage.GhlStage(stage_name=stage, pipeline_id="pipe-1")

    read.seen = seen
    return read


def _body(stage, opp="opp-9"):
    return {"opportunity_id": opp, "stage_name": stage, "pipeline_id": "pipe-1", "phone": "(813) 555-0199"}


def test_the_same_stage_delivered_ten_times_is_one_event_one_current_stage_one_loan_move(book):
    read = _ghl_says("Funded")
    for _ in range(10):
        ghl_stage.handle_stage(_Db(), _body("Funded"), reader=read)
    assert book.events == [("opp-9", "funded")]
    assert book.current == {"opp-9": "funded"} and book.loan == {"opp-9": "funded"}


def test_an_older_stage_arriving_after_funded_leaves_the_deal_funded(book):
    read = _ghl_says("Funded")
    ghl_stage.handle_stage(_Db(), _body("Funded"), reader=read)
    ghl_stage.handle_stage(_Db(), _body("Submitted to lender"), reader=read)   # late, GHL still says Funded
    assert book.events == [("opp-9", "funded"), ("opp-9", "submitted to lender")]   # the body's event is kept
    assert book.current == {"opp-9": "funded"} and book.loan == {"opp-9": "funded"}


def test_funded_missed_then_an_older_stage_arrives_records_what_ghl_says(book):
    """Only "Submitted" reaches us, after the deal was Funded in GHL: GHL's stage is recorded and moves the loan."""
    ghl_stage.handle_stage(_Db(), _body("Submitted to lender"), reader=_ghl_says("Funded"))
    assert book.current == {"opp-9": "funded"} and book.loan == {"opp-9": "funded"}
    assert book.events == [("opp-9", "submitted to lender")]


@pytest.mark.parametrize("reader", [_ghl_says(ghl_stage.GhlStageReadError("HTTP 500")), None])
def test_when_ghl_cannot_be_read_the_body_stage_is_used(book, reader):
    ghl_stage.handle_stage(_Db(), _body("Application"), reader=reader)
    ghl_stage.handle_stage(_Db(), _body("Funded"), reader=reader)
    assert book.current == {"opp-9": "funded"} and book.loan == {"opp-9": "funded"}
    ghl_stage.handle_stage(_Db(), _body("Application"), reader=reader)         # a redelivery is still held
    assert book.current == {"opp-9": "funded"} and len(book.held) == 1


def test_reconcile_uses_the_fresh_stage_instead_of_the_search_result(book):
    read = _ghl_says("Lost")
    ghl_stage.handle_stage(_Db(), {"opportunity_id": "opp-9", "stage_name": "Funded", "pipeline_id": "pipe-1",
                                   "source": "reconcile"}, reader=read)
    assert read.seen == ["opp-9"] and book.current == {"opp-9": "lost"} and book.loan == {}


def test_a_ghl_key_value_body_under_custom_data_is_handled(book):
    ghl_stage.handle_stage(_Db(), {"contact_id": "c-1", "customData": _body("Funded", opp="opp-2")},
                           reader=_ghl_says("Funded"))
    assert book.current == {"opp-2": "funded"}


def test_the_handler_is_registered_with_the_journal():
    from src.lending import webhook_failures
    from src.lending.inbound import journal

    assert "src.lending.inbound.ghl_stage" in journal.HANDLER_MODULES
    assert journal._HANDLERS["ghl_stage"] is ghl_stage.handle_stage
    assert webhook_failures.ENDPOINT_HANDLERS["ghl_stage"] is ghl_stage.handle_stage


# ---- endpoint, journal flag on ----------------------------------------------------------------------------------

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}


@pytest.fixture
def ingested(monkeypatch):
    """/ghl-stage with the journal on and journal.ingest replaced: each call is recorded; no database is touched."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pydantic import SecretStr

    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    from src.lending.inbound import journal

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(get_settings(), "lending_inbound_journal_enabled", True, raising=False)
    calls: list = []
    state = {"error": None}

    def ingest(source, key, raw, *, rerun_duplicates=False, **_):
        if state["error"]:
            raise state["error"]
        calls.append((source, key, raw, rerun_duplicates))
        return journal.IngestResult(event_id=1, outcome="processed", value={"recorded": True, "stage": "Funded"})

    monkeypatch.setattr(journal, "ingest", ingest)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: SimpleNamespace()
    return SimpleNamespace(client=TestClient(app, raise_server_exceptions=False), calls=calls, state=state)


def test_flag_on_the_stage_goes_through_the_journal_keyed_by_opportunity_stage_and_hour(ingested, monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(ghl_stage, "_now", lambda: datetime(2026, 11, 25, 14, 37, tzinfo=timezone.utc))
    r = ingested.client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=_body(" Funded "))
    assert r.status_code == 200 and r.json() == {"recorded": True, "status": "processed"}
    assert ingested.calls == [("ghl_stage", "opp-9:funded:2026112514", _body(" Funded "), True)]


def test_flag_on_a_raw_save_failure_is_a_500_so_ghl_delivers_again(ingested):
    ingested.state["error"] = ConnectionError("db down")
    r = ingested.client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=_body("Funded"))
    assert r.status_code == 500 and r.json() == {"detail": "Stage event could not be recorded"}


@pytest.mark.parametrize("body", [{"opportunity_id": "opp-9"}, {"opportunity_id": "opp-9", "stage_name": "null"}])
def test_flag_on_a_bad_body_is_still_422_and_never_journaled(ingested, body):
    assert ingested.client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=body).status_code == 422
    assert ingested.calls == []


def test_flag_on_a_wrong_secret_is_401_and_never_journaled(ingested):
    r = ingested.client.post("/webhooks/lending/ghl-stage", headers={"X-Webhook-Secret": "nope"}, json=_body("Funded"))
    assert r.status_code == 401 and ingested.calls == []

def test_reconciliation_does_not_apply_a_search_result_when_the_fresh_read_fails(book):
    with pytest.raises(ghl_stage.GhlStageReadError):
        ghl_stage.handle_stage(_Db(), {**_body("Funded"), "source": "reconcile"}, reader=None)
    assert not book.current and not book.loan
