"""Split T-11 / Section 0 line 10: FL wholesaler lists scrubbed, loaded to Partners, tagged in GHL."""
from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from config import lending_queues as q
from src.lending.dialer_load import LoadReport
from src.lending.wholesaler_lists import ghl_tags, parse_wholesaler_csv, push_to_ghl
from src.tasks import lending_wholesaler_load as task

CSV = """source,name,entity_name,phone,state,email
sunbiz,Ann Lee,Lee Homes LLC,(813) 555-0101,FL,ann@x.com
Hunter,Bo Diaz,,813-555-0102,fl,
sunbiz,Cy Ray,Ray LLC,404-555-0103,GA,
sunbiz,No Phone,,,FL,
zillow,Di Fox,,813-555-0104,FL,
sunbiz,Ann Lee,Lee Homes LLC,8135550101,FL,
"""


def test_parse_keeps_fl_known_sources_with_phone_and_tags_them():
    parsed = parse_wholesaler_csv(CSV)
    assert [r["phone"] for r in parsed.records] == ["+18135550101", "+18135550102"]
    first, second = parsed.records
    assert first["source_tag"] == "wholesaler_sunbiz" and second["source_tag"] == "wholesaler_hunter"
    assert first["pool"] == q.PARTNERS and first["state"] == "FL"
    assert first["borrower_name"] == "Ann Lee" and first["entity_name"] == "Lee Homes LLC"
    assert first["source_record_ref"] == "wholesaler:sunbiz:+18135550101"
    assert dict(parsed.dropped) == {"NOT_FL": 1, "NO_PHONE": 1, "UNKNOWN_SOURCE": 1, "DUPLICATE": 1}


def test_parse_rejects_csv_without_required_columns():
    with pytest.raises(ValueError):
        parse_wholesaler_csv("name,phone\nA,8135550101\n")


def test_ghl_tags_are_source_and_state():
    record = parse_wholesaler_csv(CSV).records[0]
    assert ghl_tags(record) == ["ndl-wholesaler", "ndl-src-sunbiz", "ndl-state-fl"]


class FakeSink:
    def __init__(self, fail_refs=()):
        self.pushed, self.fail_refs = [], set(fail_refs)

    def push(self, record, tags):
        if record["source_record_ref"] in self.fail_refs:
            raise RuntimeError("boom")
        self.pushed.append((record["source_record_ref"], tags))


def test_push_to_ghl_counts_failures_and_keeps_going():
    records = parse_wholesaler_csv(CSV).records
    sink = FakeSink(fail_refs={records[0]["source_record_ref"]})
    result = push_to_ghl(records, sink)
    assert result.pushed == 1 and result.failed == [records[0]["source_record_ref"]]
    assert sink.pushed[0][0] == records[1]["source_record_ref"]


# --- task ---------------------------------------------------------------------

@pytest.fixture
def csv_path(tmp_path):
    path = tmp_path / "wholesalers.csv"
    path.write_text(CSV, encoding="utf-8")
    return path


@pytest.fixture
def harness(monkeypatch):
    calls = {"loads": [], "sink": FakeSink()}

    def fake_load(records, session, *, run_id, dry_run, **kw):
        calls["loads"].append({"dry_run": dry_run, "records": list(records), **kw})
        report = LoadReport(run_id=run_id, dry_run=dry_run, total=len(records))
        report.needs_scrub = 1 if dry_run else 0  # one number already has a fresh scrub
        if not dry_run:
            report.loaded = 1
            report.loaded_refs = [records[0]["source_record_ref"]]  # second one blocked by the scrub
        return report

    @contextmanager
    def fake_db():
        class S:
            def rollback(self): calls["rolled_back"] = True
            def commit(self): pass
        yield S()

    monkeypatch.setattr(task, "run_dialer_load", fake_load)
    monkeypatch.setattr(task, "get_db_context", fake_db)
    monkeypatch.setattr(task.dialer_port, "get_dialer", lambda: object())
    monkeypatch.setattr(task, "ghl_sink", lambda: calls["sink"])
    return calls


def test_dry_run_is_default_scrubs_nothing_and_pushes_nothing(csv_path, harness):
    assert task.main(["--input", str(csv_path)]) == 0
    (load,) = harness["loads"]
    assert load["dry_run"] is True and "scrubber" not in load
    assert harness["rolled_back"] and harness["sink"].pushed == []


def test_live_refuses_without_matching_scrub_confirmation(csv_path, harness):
    assert task.main(["--input", str(csv_path), "--live"]) == 4
    assert task.main(["--input", str(csv_path), "--live", "--confirm-scrub", "2"]) == 4  # 2 rows, but only 1 needs a scrub
    assert all(load["dry_run"] for load in harness["loads"])


def test_live_with_confirmation_loads_partners_and_tags_only_loaded_numbers_in_ghl(csv_path, harness):
    assert task.main(["--input", str(csv_path), "--live", "--confirm-scrub", "1"]) == 0
    dry, load = harness["loads"]
    assert dry["dry_run"] is True
    assert load["dry_run"] is False and load["scrubber"] is not None
    assert {r["pool"] for r in load["records"]} == {q.PARTNERS}
    assert harness["sink"].pushed == [("wholesaler:sunbiz:+18135550101",
                                       ["ndl-wholesaler", "ndl-src-sunbiz", "ndl-state-fl"])]


def test_live_refuses_without_ghl_account(csv_path, harness, monkeypatch):
    monkeypatch.setattr(task, "ghl_sink", lambda: None)
    assert task.main(["--input", str(csv_path), "--live", "--confirm-scrub", "1"]) == 3
    assert all(load["dry_run"] for load in harness["loads"])


def test_ghl_transport_error_names_the_step(monkeypatch):
    from src.lending.ghl_account import GhlAccount
    from src.lending.wholesaler_lists import GhlWholesalerSink
    from src.services import ghl_webhook

    def boom(*a, **kw):
        raise ConnectionError("down")

    monkeypatch.setattr(ghl_webhook, "_ghl_request", boom)
    record = parse_wholesaler_csv(CSV).records[0]
    with pytest.raises(RuntimeError, match="contact upsert: ConnectionError"):
        GhlWholesalerSink(GhlAccount("k", "loc")).push(record, ghl_tags(record))
