"""Calling-list trace: the trace credit limit, files written before anything else, and the scrub budget. No network."""
from __future__ import annotations

import json

from src.services.property_radar.trace_contacts import LeadContacts
from src.tasks import lending_pr_calling_list_trace as task


def _lead(i, strength="likely"):
    return task.Lead(radar_id=f"R{i}", property_address=f"{i} Main St", city="Tampa", state="FL", zip="33602",
                     owner_name="Ann Lee", principal_name=None, match_strength=strength)


def test_batches_never_let_tracing_pass_its_credit_limit():
    assert [task.next_batch_size(648, spent) for spent in (0, 250, 500, 640, 648, 700)] == [250, 250, 148, 8, 0, 0]


def test_tracing_stops_at_the_limit_even_when_every_lead_is_found(monkeypatch, tmp_path):
    wallet = {"balance": 1297}

    def fake_trace(batch, **kwargs):
        kwargs["submit"]([{"label": lead.radar_id} for lead in batch])
        wallet["balance"] -= len(batch)  # every lead found: 1 credit each
        return type("Outcome", (), {"submitted": len(batch),
                                    "contacts": {lead.radar_id: LeadContacts(phones=("+18135550100",)) for lead in batch}})()

    monkeypatch.setattr(task, "trace_staged_leads", fake_trace)
    monkeypatch.setattr(task, "already_traced", lambda db, vendor, keys: {})
    result = task.run_trace(None, [_lead(i) for i in range(1000)], folder=tmp_path, trace_limit=648,
                            balance=lambda: wallet["balance"], submit=lambda rows: rows,
                            write_ledger=lambda e: None, write_contacts=lambda c: None)
    assert result["trace_credits_spent"] == 648 and result["found"] == 648 and result["left_untraced"] == 352
    assert wallet["balance"] == 649  # the rest is kept for the do-not-call check


def test_every_tracerfy_response_is_on_disk_before_the_caller_gets_it(tmp_path):
    seen_on_disk = []

    def vendor(payload):
        return [{"address": "1 Main St", "phone": "8135550100"}]

    wrapped = task.landed(vendor, tmp_path, "trace")
    received = wrapped([{"address": "1 Main St"}])
    seen_on_disk.extend(tmp_path.glob("trace_*.json"))
    (path,) = seen_on_disk
    assert json.loads(path.read_text()) == {"sent": [{"address": "1 Main St"}], "received": received}
    assert not list(tmp_path.glob("*.tmp"))


def test_confirmed_maturities_are_traced_before_likely_ones(monkeypatch):
    rows = [type("Row", (), {"_mapping": _lead(i, s).__dict__})() for i, s in
            enumerate(["likely", "confirmed", "likely", "confirmed"])]
    db = type("Db", (), {"execute": lambda self, *a, **k: rows})()
    assert [lead.match_strength for lead in task.load_leads(db, "verified_maturity")] == \
        ["confirmed", "confirmed", "likely", "likely"]


def test_live_refuses_a_trace_limit_that_leaves_too_little_for_the_scrub(monkeypatch, tmp_path):
    from contextlib import contextmanager

    @contextmanager
    def db_context():
        yield None

    monkeypatch.setattr(task, "get_db_context", db_context)
    monkeypatch.setattr(task, "load_leads", lambda db, k, resold_from=None, resold_before=None: [])
    monkeypatch.setattr(task, "_balance", lambda: 1297)
    from src.lending import pr_list_landing as landing
    monkeypatch.setattr(landing, "LANDING_ROOT", tmp_path)
    assert task.main(["--list", "verified_maturity", "--live", "--trace-credits", "700"]) == 3
    assert task.main(["--list", "verified_maturity", "--live"]) == 3


def test_landed_results_are_replayed_from_disk_once_and_never_resent(monkeypatch, tmp_path):
    """An interrupted run's paid results are written from the landing file; replaying again writes nothing."""
    from src.services.skip_trace_ledger import trace_key

    folder = tmp_path / "verified_maturity" / "tracerfy" / "run1"
    sent = [{"address": "412 Bailey Rd", "city": "Sarasota", "state": "FL", "zip": "34237",
             "first_name": "Ann", "last_name": "Lee", "label": "R1"},
            {"address": "9 Oak Ave", "city": "Tampa", "state": "FL", "zip": "33602",
             "first_name": "Bo", "last_name": "Diaz", "label": "R2"}]
    received = [{"address": "412 BAILEY RD", "city": "SARASOTA", "state": "FL", "zip": "",
                 "first_name": "ANN", "last_name": "LEE", "mobile_1": "8135550101"}]
    task.write_landing(folder, "trace_0001.json", {"sent": sent, "received": received})
    monkeypatch.setattr(task, "LANDING_ROOT", tmp_path)

    ledger_rows, contacts_written = [], {}

    def already(db, vendor, keys):
        done = {row["target_address"] for row in ledger_rows}
        return {k: {"normal"} for k in keys if k in done}

    monkeypatch.setattr(task, "already_traced", already)
    first = task.replay_landed(None, "verified_maturity", write_ledger=ledger_rows.extend,
                               write_contacts=contacts_written.update)
    assert first == {"replayed_addresses": 2, "replayed_found": 1}
    assert set(contacts_written) == {"R1"}
    assert {row["target_address"] for row in ledger_rows} == {trace_key("412 Bailey Rd", "34237"),
                                                              trace_key("9 Oak Ave", "33602")}
    second = task.replay_landed(None, "verified_maturity", write_ledger=ledger_rows.extend,
                                write_contacts=contacts_written.update)
    assert second == {"replayed_addresses": 0, "replayed_found": 0} and len(ledger_rows) == 2


def test_a_new_wrapper_per_batch_never_overwrites_an_earlier_batch_file(tmp_path):
    for batch in range(3):
        task.landed(lambda payload: [{"n": batch}], tmp_path, "trace")([{"label": f"R{batch}"}])
    files = sorted(p.name for p in tmp_path.glob("trace_*.json"))
    assert files == ["trace_0001.json", "trace_0002.json", "trace_0003.json"]
    assert [json.loads((tmp_path / f).read_text())["sent"][0]["label"] for f in files] == ["R0", "R1", "R2"]


def test_the_call_window_bypass_drops_only_the_calling_hours_block():
    from config.lending_compliance import ReasonCode
    from src.lending.dialer_load import rail_blocks
    rail = {"+18135550101": ReasonCode.OUTSIDE_CALL_WINDOW, "+18135550102": ReasonCode.ATTEMPT_CAP_REACHED,
            "+18135550103": ReasonCode.NO_FRESH_SCRUB, "+18135550104": ReasonCode.ATTEMPT_HISTORY_EXCEEDED}
    assert rail_blocks(rail, allow_outside_call_window=False) == rail
    assert rail_blocks(rail, allow_outside_call_window=True) == {
        "+18135550102": ReasonCode.ATTEMPT_CAP_REACHED, "+18135550103": ReasonCode.NO_FRESH_SCRUB,
        "+18135550104": ReasonCode.ATTEMPT_HISTORY_EXCEEDED}


def test_hits_that_arrive_after_a_batch_was_accepted_are_stored_once(tmp_path):
    task.write_landing(tmp_path, "trace_0001.json", {
        "queue_id": 77, "sent": [{"address": "1 Main St", "zip": "33602", "label": "R1"},
                                 {"address": "2 Elm St", "zip": "33602", "label": "R2"}],
        "received": [{"address": "1 MAIN ST", "mobile_1": "8135550101"}]})
    late = [{"address": "1 MAIN ST", "mobile_1": "8135550101"}, {"address": "2 ELM ST", "mobile_1": "8135550102"}]
    stored, ledger = {}, []

    class Db:
        def execute(self, *a, **k):
            return [(rid,) for rid in stored]

    result = task.reconcile_queues(Db(), tmp_path, fetch=lambda q: late, write_ledger=ledger.extend,
                                   write_contacts=stored.update)
    assert result == {"queues_checked": 1, "late_hits_stored": 1} and set(stored) == {"R2"} and len(ledger) == 1
    assert (tmp_path / "reconcile_77.json").exists()                      # on disk before the database
    again = task.reconcile_queues(Db(), tmp_path, fetch=lambda q: late, write_ledger=ledger.extend,
                                  write_contacts=stored.update)
    assert again["late_hits_stored"] == 0 and len(ledger) == 1            # never stored or ledgered twice


def test_a_live_trace_refuses_to_start_while_another_job_holds_the_lock(monkeypatch, tmp_path):
    from src.lending import pr_list_landing as landing
    monkeypatch.setattr(landing, "LANDING_ROOT", tmp_path)
    lock = landing.acquire_pull_lock()
    try:
        assert task.main(["--list", "wholesaler", "--live", "--trace-credits", "100"]) == 3
    finally:
        landing.release_pull_lock(lock)


def test_the_priority_window_is_passed_to_the_subject_query():
    from datetime import date
    seen = {}

    class Db:
        def execute(self, sql, params):
            seen.update(params)
            return []

    task.load_leads(Db(), "wholesaler", date(2026, 10, 1))
    assert seen == {"k": "wholesaler", "resold_from": date(2026, 10, 1), "resold_before": None}
    task.load_leads(Db(), "wholesaler", date(2026, 9, 1), date(2026, 10, 1))
    assert seen == {"k": "wholesaler", "resold_from": date(2026, 9, 1), "resold_before": date(2026, 10, 1)}
    task.load_leads(Db(), "wholesaler")
    assert seen["resold_from"] is None and seen["resold_before"] is None


def test_a_bigger_batch_size_sends_fewer_requests_but_never_passes_the_credit_limit():
    assert [task.next_batch_size(767, spent, 800) for spent in (0, 500, 767)] == [767, 267, 0]
    assert task.next_batch_size(5000, 0, task.MAX_BATCH_SIZE) == task.MAX_BATCH_SIZE


def test_a_batch_size_above_what_tracerfy_accepts_is_refused():
    import pytest
    with pytest.raises(SystemExit):
        task.main(["--list", "cash_buyer", "--batch-size", str(task.MAX_BATCH_SIZE + 1)])


def test_the_next_phone_is_checked_only_when_every_checked_phone_was_blocked():
    a, b, c = "+18135550101", "+18135550102", "+18135550103"
    assert task.next_unchecked_phone([a, b], blocked={a}, clean=set()) == b
    assert task.next_unchecked_phone([a, b], blocked=set(), clean={a}) is None      # already loadable
    assert task.next_unchecked_phone([a, b], blocked={a}, clean={b}) is None        # second already clean
    assert task.next_unchecked_phone([a, b], blocked={a, b}, clean=set()) is None   # nothing left to check
    assert task.next_unchecked_phone([a, b, c], blocked={a, b}, clean=set()) == c


def test_only_people_sent_with_an_unsplit_name_are_picked_for_a_retrace(monkeypatch, tmp_path):
    folder = tmp_path / "verified_maturity" / "tracerfy" / "run1"
    task.write_landing(folder, "trace_0001.json", {"sent": [
        {"first_name": "LEE,ANN", "last_name": "", "label": "R1"},
        {"first_name": "Bo", "last_name": "Diaz", "label": "R2"},
        {"first_name": "RITTER,ONA", "last_name": "KATHLEEN", "label": "R3"}], "received": []})
    task.write_landing(tmp_path / "verified_maturity" / "tracerfy" / "run2", "trace_0001.json", {"sent": [
        {"first_name": "ONA KATHLEEN", "last_name": "RITTER", "label": "R3"}], "received": []})
    monkeypatch.setattr(task, "LANDING_ROOT", tmp_path)
    assert task.sent_with_unsplit_name("verified_maturity") == {"R1"}   # R3 was already sent again, split


def test_a_retrace_lets_only_the_named_addresses_past_the_ledger(monkeypatch, tmp_path):
    from src.services.skip_trace_ledger import trace_key
    sent = []

    def submit(rows):
        sent.extend(r["label"] for r in rows)
        return [{"address": r["address"], "mobile_1": "8135550100"} for r in rows]

    submit.lands_on_disk = True
    leads = [_lead(1), _lead(2)]
    every_key = {trace_key(lead.property_address, lead.zip) for lead in leads}
    monkeypatch.setattr(task, "already_traced", lambda db, vendor, keys: {k: {"normal"} for k in every_key})
    task.run_trace(None, leads, folder=tmp_path, trace_limit=10, balance=lambda: 100, submit=submit,
                   write_ledger=lambda e: None, write_contacts=lambda c: None,
                   resend_keys=frozenset({trace_key(leads[0].property_address, leads[0].zip)}))
    assert sent == ["R1"]


def test_advanced_batches_are_sized_for_two_credits_a_hit():
    assert task.next_batch_size(100, 0, 500, task.ADVANCED_CREDITS_PER_HIT) == 50
