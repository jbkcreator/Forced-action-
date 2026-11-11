"""Cora's approved CRM writes to GoHighLevel: pipeline stage moves, tags and notes. HTTP is faked at the
request boundary; no network."""
from __future__ import annotations

import pytest

from src.lending import ghl_crm
from src.lending.ghl_account import GhlAccount
from src.lending.ghl_crm import GhlCrmError, GhlCrmWriter

ACCOUNT = GhlAccount("key-1", "loc-1")
BASE = "https://services.leadconnectorhq.com"
PIPELINES = {"pipelines": [
    {"id": "pipe-booked", "name": "Booked Calls",
     "stages": [{"id": "st-new", "name": "New Lead"}, {"id": "st-app", "name": "Application"},
                {"id": "st-funded", "name": "Funded"}]},
    {"id": "pipe-other", "name": "Other", "stages": [{"id": "st-x", "name": "Application"}]},
]}


class FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._body = status, body

    def json(self) -> dict:
        return self._body


class FakeGhl:
    """Answers by (method, path); records every call."""

    def __init__(self, routes: dict[tuple[str, str], tuple[int, dict]]) -> None:
        self.routes, self.calls = routes, []

    def __call__(self, method: str, url: str, **kwargs):
        path = url.removeprefix(BASE)
        self.calls.append((method, path, kwargs))
        status, body = self.routes[(method, path)]
        return FakeResponse(status, body)


@pytest.fixture(autouse=True)
def _not_halted(monkeypatch):
    monkeypatch.setattr(ghl_crm, "is_globally_halted", lambda: False)
    monkeypatch.setattr(ghl_crm, "_pipeline_id", lambda: "pipe-booked")


def _routes(opps: list[dict], put_status: int = 200) -> dict:
    return {("GET", "/opportunities/pipelines"): (200, PIPELINES),
            ("GET", "/opportunities/search"): (200, {"opportunities": opps}),
            ("PUT", "/opportunities/opp-1"): (put_status, {"opportunity": {"id": "opp-1"}})}


def test_move_stage_matches_the_stage_name_case_insensitively_and_moves_the_open_card() -> None:
    ghl = FakeGhl(_routes([{"id": "opp-1", "status": "open", "pipelineStageId": "st-new"}]))
    ref = GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", " application ")
    assert ref == "opp-1"
    method, path, kwargs = ghl.calls[-1]
    assert (method, path) == ("PUT", "/opportunities/opp-1")
    assert kwargs["json"] == {"pipelineStageId": "st-app"}
    search = ghl.calls[1][2]["params"]
    assert search == {"location_id": "loc-1", "contact_id": "ghl-123", "pipeline_id": "pipe-booked"}


def test_unknown_stage_fails_and_names_the_real_stages_without_writing() -> None:
    ghl = FakeGhl(_routes([{"id": "opp-1", "status": "open"}]))
    with pytest.raises(GhlCrmError, match="New Lead, Application, Funded"):
        GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", "Underwriting")
    assert all(method == "GET" for method, _, _ in ghl.calls)


def test_contact_without_a_card_in_the_pipeline_fails_without_writing() -> None:
    ghl = FakeGhl(_routes([]))
    with pytest.raises(GhlCrmError, match="no card"):
        GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", "Application")
    assert all(method == "GET" for method, _, _ in ghl.calls)


def test_ghl_rejecting_the_move_is_an_error_without_response_detail() -> None:
    ghl = FakeGhl(_routes([{"id": "opp-1", "status": "open"}], put_status=422))
    with pytest.raises(GhlCrmError, match="HTTP 422"):
        GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", "Application")


def test_nothing_is_written_during_the_lending_emergency_halt(monkeypatch) -> None:
    monkeypatch.setattr(ghl_crm, "is_globally_halted", lambda: True)
    ghl = FakeGhl(_routes([{"id": "opp-1", "status": "open"}]))
    with pytest.raises(GhlCrmError, match="halt"):
        GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", "Application")
    assert ghl.calls == []


def test_add_tag_adds_without_replacing_existing_tags() -> None:
    ghl = FakeGhl({("POST", "/contacts/ghl-123/tags"): (201, {"tags": ["old", "hot lead"]})})
    assert GhlCrmWriter(ACCOUNT, ghl).add_tag("ghl-123", "  hot lead ") == "ghl-123:tag:hot lead"
    assert [(m, p, k["json"]) for m, p, k in ghl.calls] == [("POST", "/contacts/ghl-123/tags", {"tags": ["hot lead"]})]


def test_add_note_returns_the_ghl_note_id() -> None:
    ghl = FakeGhl({("POST", "/contacts/ghl-123/notes"): (201, {"note": {"id": "note-9"}})})
    assert GhlCrmWriter(ACCOUNT, ghl).add_note("ghl-123", "Wants 250k for a Tampa flip") == "note-9"
    assert ghl.calls[0][2]["json"] == {"body": "Wants 250k for a Tampa flip"}


def test_tag_and_note_failures_and_halt_write_nothing(monkeypatch) -> None:
    ghl = FakeGhl({("POST", "/contacts/ghl-123/tags"): (400, {}), ("POST", "/contacts/ghl-123/notes"): (500, {})})
    writer = GhlCrmWriter(ACCOUNT, ghl)
    with pytest.raises(GhlCrmError, match="HTTP 400"):
        writer.add_tag("ghl-123", "hot")
    with pytest.raises(GhlCrmError, match="HTTP 500"):
        writer.add_note("ghl-123", "x")
    monkeypatch.setattr(ghl_crm, "is_globally_halted", lambda: True)
    calls_before = len(ghl.calls)
    for write in (lambda: writer.add_tag("ghl-123", "hot"), lambda: writer.add_note("ghl-123", "x")):
        with pytest.raises(GhlCrmError, match="halt"):
            write()
    assert len(ghl.calls) == calls_before


@pytest.mark.parametrize("halt_after_calls", [1, 2])
def test_a_halt_set_during_the_lookups_stops_the_stage_move(monkeypatch, halt_after_calls: int) -> None:
    ghl = FakeGhl(_routes([{"id": "opp-1", "status": "open"}]))
    monkeypatch.setattr(ghl_crm, "is_globally_halted", lambda: len(ghl.calls) >= halt_after_calls)
    with pytest.raises(GhlCrmError, match="halt"):
        GhlCrmWriter(ACCOUNT, ghl).move_stage("ghl-123", "Application")
    assert all(method == "GET" for method, _, _ in ghl.calls)
