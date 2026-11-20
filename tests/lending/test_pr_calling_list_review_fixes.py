"""PR review fixes for the PropertyRadar calling lists: the seller's phone is never the owner's, wholesaler subjects
are built before a trace, late results match on street and city, an accepted queue is saved before polling, a later
sale is bought again, a pull re-plans under the lock, and the export reads the same scrubs as the dialer gate."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.lending import pr_list_landing as landing
from src.lending.pr_calling_lists import PullWindow
from src.lending.pr_list_trace_subjects import SUBJECT_KEY_PREFIX
from src.services.property_radar import live_trace
from src.services.skip_trace_ledger import ADVANCED, NORMAL
from src.tasks import lending_pr_calling_list_export as export
from src.tasks import lending_pr_calling_list_pull as pull
from src.tasks import lending_pr_calling_list_trace as trace

SELLER_PHONE, OWNER_PHONE = "+18135550202", "+18135550101"


# ── 1. a seller's phone is stored under the trace subject, never the property's RadarID ────────────────────────

def _wholesaler_tables(conn):
    from src.lending.models import LendingPrListPull, LendingPrListRecord, LendingPrListTraceSubject

    for table in (LendingPrListPull.__table__, LendingPrListRecord.__table__, LendingPrListTraceSubject.__table__):
        table.create(conn, checkfirst=True)
    conn.execute(text("""CREATE TABLE IF NOT EXISTS property_radar_traced_contacts (
        radar_id VARCHAR(50) PRIMARY KEY, phones VARCHAR[] NOT NULL DEFAULT '{}',
        emails VARCHAR[] NOT NULL DEFAULT '{}', traced_at TIMESTAMPTZ NOT NULL DEFAULT now())"""))


def _contacts(db, key, phone):
    db.execute(text("INSERT INTO property_radar_traced_contacts (radar_id, phones) VALUES (:k, ARRAY[:p]) "
                    "ON CONFLICT (radar_id) DO UPDATE SET phones = EXCLUDED.phones"), {"k": key, "p": phone})


@pytest.mark.parametrize("seller_first", [True, False])
def test_buyer_and_seller_of_one_property_keep_their_own_phones(lending_db, seller_first):
    _wholesaler_tables(lending_db.connection())
    pull_id = lending_db.execute(text(
        "INSERT INTO lending.pr_list_pulls (list_key, window_start, window_end, criteria, matching_count) "
        "VALUES ('wholesaler', '2026-10-01', '2026-10-31', '[]'::jsonb, 1) RETURNING id")).scalar()
    for list_key, owner in (("wholesaler", "NEW OWNER LLC"), ("cash_buyer", "LEE ANN")):
        lending_db.execute(text("""
            INSERT INTO lending.pr_list_records (list_key, pull_id, radar_id, property_address, city, state, zip,
                owner_name, last_transfer_seller, last_transfer_date, qualifies, match_strength, contact_key, raw)
            VALUES (:k, :p, 'RFX1', '9 Oak Ave', 'Tampa', 'FL', '33602', :owner, 'SMITH JOHN', '2026-10-15', true,
                    'confirmed', :ck, '{}'::jsonb)"""), {"k": list_key, "p": pull_id, "owner": owner, "ck": f"ck-{list_key}"})
    lending_db.execute(text("""
        INSERT INTO lending.pr_list_trace_subjects (list_key, contact_key, radar_id, subject_name, first_name,
            last_name, address, city, state, zip, source)
        VALUES ('wholesaler', 'ck-wholesaler', 'RFX1', 'SMITH JOHN', 'John', 'Smith', '9 Oak Ave', 'Tampa', 'FL',
                '33602', 'seller_property')"""))

    def trace_seller():
        (lead,) = trace.load_leads(lending_db, "wholesaler")
        assert lead.radar_id.startswith(SUBJECT_KEY_PREFIX)  # the owner's contacts never hide the seller
        _contacts(lending_db, lead.radar_id, SELLER_PHONE)

    def trace_owner():
        assert [lead.radar_id for lead in trace.load_leads(lending_db, "cash_buyer")] == ["RFX1"]
        _contacts(lending_db, "RFX1", OWNER_PHONE)

    for step in ((trace_seller, trace_owner) if seller_first else (trace_owner, trace_seller)):
        step()
    seller = lending_db.execute(export._SUBJECT_PEOPLE_SQL, {"k": "wholesaler"}).mappings().one()
    owner = lending_db.execute(export._PEOPLE_SQL, {"k": "cash_buyer"}).mappings().one()
    assert list(seller["phones"]) == [SELLER_PHONE] and list(owner["phones"]) == [OWNER_PHONE]


def test_a_legacy_seller_result_filed_under_a_radar_id_is_never_replayed(monkeypatch, tmp_path):
    folder = tmp_path / "wholesaler" / "tracerfy" / "run1"
    sent = [{"address": "9 Oak Ave", "city": "Tampa", "state": "FL", "zip": "33602", "first_name": "John",
             "last_name": "Smith", "label": "RFX1"}]
    trace.write_landing(folder, "trace_0001.json", {"sent": sent, "received": [
        {"address": "9 OAK AVE", "city": "TAMPA", "first_name": "JOHN", "last_name": "SMITH", "mobile_1": "8135550202"}]})
    monkeypatch.setattr(trace, "LANDING_ROOT", tmp_path)
    monkeypatch.setattr(trace, "already_traced", lambda db, vendor, keys: {})
    written = {}
    result = trace.replay_landed(None, "wholesaler", write_ledger=lambda e: None, write_contacts=written.update)
    assert written == {} and result == {"replayed_addresses": 0, "replayed_found": 0}


# ── 2. the live wholesaler trace builds its subjects before selecting anyone ───────────────────────────────────

def test_the_live_wholesaler_trace_builds_subjects_before_it_selects_leads(monkeypatch, tmp_path):
    from config.settings import get_settings
    from pydantic import SecretStr

    calls = []

    @contextmanager
    def db_context():
        yield SimpleNamespace(commit=lambda: calls.append("commit"))

    monkeypatch.setattr(get_settings(), "tracerfy_api_key", SecretStr("k"), raising=False)
    monkeypatch.setattr(trace, "get_db_context", db_context)
    monkeypatch.setattr(trace, "_balance", lambda: 5000)
    monkeypatch.setattr(trace, "build_list_subjects", lambda db, k: calls.append(("build", k)) or {"seller_property": 1})
    monkeypatch.setattr(trace, "resume_pending", lambda k, fetch: 0)
    monkeypatch.setattr(trace, "replay_landed", lambda *a, **k: {})
    monkeypatch.setattr(trace, "load_leads", lambda db, k, *a: calls.append(("leads", k)) or [])
    monkeypatch.setattr(trace, "reconcile_queues", lambda *a, **k: {})
    monkeypatch.setattr(trace, "run_dnc", lambda *a, **k: {})
    monkeypatch.setattr(landing, "LANDING_ROOT", tmp_path)
    monkeypatch.setattr(trace, "LANDING_ROOT", tmp_path)
    assert trace.main(["--list", "wholesaler", "--live", "--trace-credits", "10"]) == 0
    assert calls.index(("build", "wholesaler")) < calls.index(("leads", "wholesaler"))


# ── 3. a late result matches the street and the city it was sent for ───────────────────────────────────────────

SENT_TWO_CITIES = [
    {"address": "9 Oak Ave", "city": "Tampa", "zip": "33602", "label": "TAMPA"},
    {"address": "9 Oak Ave", "city": "Sarasota", "zip": "34237", "label": "SARASOTA"},
]


def test_a_late_result_goes_to_the_city_it_was_sent_for():
    late = {"address": "9 OAK AVE", "city": "SARASOTA", "mobile_1": "8135550303"}
    assert trace.match_sent(SENT_TWO_CITIES, late)["label"] == "SARASOTA"


def test_an_ambiguous_late_result_is_left_unresolved():
    assert trace.match_sent(SENT_TWO_CITIES, {"address": "9 OAK AVE", "mobile_1": "8135550303"}) is None
    assert trace.match_sent(SENT_TWO_CITIES[:1], {"address": "9 OAK AVE"})["label"] == "TAMPA"  # sent once


def test_reconciliation_stores_a_late_hit_against_the_right_city(tmp_path):
    folder = tmp_path / "run1"
    trace.write_landing(folder, "trace_0001.json", {"queue_id": "q1", "sent": SENT_TWO_CITIES, "received": [
        {"address": "9 OAK AVE", "city": "TAMPA", "mobile_1": "8135550101"}]})
    late = [{"address": "9 OAK AVE", "city": "TAMPA", "mobile_1": "8135550101"},
            {"address": "9 OAK AVE", "city": "SARASOTA", "mobile_1": "8135550303"}]
    db = SimpleNamespace(execute=lambda *a, **k: [])
    written = {}
    result = trace.reconcile_queues(db, folder, fetch=lambda q: late, write_ledger=lambda e: None,
                                    write_contacts=written.update)
    assert result["late_hits_stored"] == 1 and set(written) == {"SARASOTA"}


def test_the_first_pass_never_gives_one_citys_result_to_another():
    leads = [SimpleNamespace(radar_id=s["label"], property_address=s["address"], city=s["city"], state="FL",
                             zip=s["zip"], owner_name="Ann Lee", principal_name=None) for s in SENT_TWO_CITIES]
    written = {}
    live_trace.trace_staged_leads(
        leads, ledger={}, submit=lambda rows: [{"address": "9 OAK AVE", "city": "SARASOTA", "mobile_1": "8135550303"}],
        write_ledger=lambda e: None, write_contacts=written.update, cap=live_trace.RunSpendCap(10 ** 6))
    assert set(written) == {"SARASOTA"}


# ── 4. an accepted queue is on disk before polling; a stopped run resumes it ───────────────────────────────────

def test_a_queue_accepted_then_lost_while_polling_is_resumed_not_resubmitted(monkeypatch, tmp_path):
    from src.services import tracerfy_batch

    folder = tmp_path / "wholesaler" / "tracerfy" / "run1"
    submits = []
    monkeypatch.setattr(tracerfy_batch, "_submit_trace_batch",
                        lambda rows, key, address_only=False: submits.append(rows) or ("q-77", 0))

    def poll_dies(*args, **kwargs):
        raise TimeoutError("connection lost while waiting")

    monkeypatch.setattr(tracerfy_batch, "_poll_trace_queue", poll_dies)
    rows = [{"address": "9 Oak Ave", "label": "subject:1:abc"}]
    with pytest.raises(TimeoutError):
        trace.recorded_submit(folder, "key")(rows)
    (pending,) = folder.glob("pending_trace_*.json")
    assert json.loads(pending.read_text()) == {"queue_id": "q-77", "sent": rows, "mode": NORMAL}

    monkeypatch.setattr(trace, "LANDING_ROOT", tmp_path)
    received = [{"address": "9 OAK AVE", "mobile_1": "8135550202"}]
    assert trace.resume_pending("wholesaler", fetch=lambda q: received if q == "q-77" else []) == 1
    assert not list(folder.glob("pending_*.json")) and len(submits) == 1
    assert json.loads((folder / "trace_0001.json").read_text())["received"] == received


def test_an_empty_resumed_queue_stays_pending(monkeypatch, tmp_path):
    folder = tmp_path / "wholesaler" / "tracerfy" / "run1"
    trace.write_landing(folder, "pending_trace_0001.json", {"queue_id": "q-1", "sent": [], "mode": NORMAL})
    monkeypatch.setattr(trace, "LANDING_ROOT", tmp_path)
    assert trace.resume_pending("wholesaler", fetch=lambda q: []) == 0
    assert list(folder.glob("pending_trace_0001.json"))


def test_advanced_results_are_replayed_with_their_name_check(monkeypatch, tmp_path):
    folder = tmp_path / "verified_maturity" / "tracerfy" / "run1"
    sent = [{"address": "9 Oak Ave", "city": "Tampa", "zip": "33602", "first_name": "Ann", "last_name": "Lee",
             "label": "R1"},
            {"address": "5 Elm St", "city": "Tampa", "zip": "33602", "first_name": "Bo", "last_name": "Diaz",
             "label": "R2"}]
    received = [{"address": "9 OAK AVE", "city": "TAMPA", "last_name": "LEE", "mobile_1": "8135550101"},
                {"address": "5 ELM ST", "city": "TAMPA", "last_name": "STRANGER", "mobile_1": "8135550909"}]
    trace.write_landing(folder, "advanced_0001.json", {"sent": sent, "received": received})
    monkeypatch.setattr(trace, "LANDING_ROOT", tmp_path)
    modes = []
    # An address-only retry always follows a normal miss, so the ledger already holds the normal submission.
    monkeypatch.setattr(trace, "already_traced", lambda db, vendor, keys: {k: {NORMAL} for k in keys})
    real = trace.trace_staged_leads

    def spy(leads, **kwargs):
        modes.append(kwargs["mode"])
        return real(leads, **kwargs)

    monkeypatch.setattr(trace, "trace_staged_leads", spy)
    written = {}
    trace.replay_landed(None, "verified_maturity", write_ledger=lambda e: None, write_contacts=written.update)
    assert modes == [ADVANCED] and set(written) == {"R1"}  # the stranger's phone is not kept


# ── 5. a property sold again in a later window is bought again ────────────────────────────────────────────────

OCT, NOV = PullWindow(date(2026, 10, 1), date(2026, 10, 31)), PullWindow(date(2026, 11, 1), date(2026, 11, 30))


def test_one_radar_id_sold_in_two_windows_is_bought_again_for_the_later_sale():
    stored = {"R1": date(2026, 10, 15)}  # bought in October with the October seller
    assert pull.bought_for_window("wholesaler", OCT, stored, {}) == {"R1"}
    assert pull.bought_for_window("wholesaler", NOV, stored, {}) == set()  # the November sale is bought
    assert pull.bought_for_window("wholesaler", NOV, {"R1": None}, {}) == {"R1"}  # no date: never paid twice
    assert pull.bought_for_window("verified_maturity", NOV, stored, {}) == {"R1"}  # not searched by sale date


def test_another_lists_old_sale_is_not_copied_into_a_new_window():
    held = {"R1": {"RadarID": "R1", "LastTransferRecDate": "2026-10-15"},
            "R2": {"RadarID": "R2", "LastTransferRecDate": "2026-11-10"}}
    reuse = pull.reusable_elsewhere("cash_buyer", NOV, lambda ids: {i: held[i] for i in ids})
    assert set(reuse(["R1", "R2"])) == {"R2"}


# ── 6. a second pull re-plans under the lock ──────────────────────────────────────────────────────────────────

def test_a_pull_replans_under_the_lock_and_never_buys_what_another_run_just_bought(monkeypatch, tmp_path):
    plans = [pull.PullPlan(to_buy={"s": ["R1", "R2"]}, matching={"s": 2}, already_stored=0, free_remaining=10),
             pull.PullPlan(to_buy={"s": []}, matching={"s": 2}, already_stored=2, free_remaining=10)]
    monkeypatch.setattr(pull, "make_plan", lambda *a, **k: plans.pop(0))
    monkeypatch.setattr(pull, "get_property_radar_port", lambda: object())

    @contextmanager
    def db_context():
        yield None

    monkeypatch.setattr(pull, "get_db_context", db_context)
    monkeypatch.setattr(landing, "LANDING_ROOT", tmp_path)
    ran = []
    monkeypatch.setattr(pull, "_run_live", lambda *a, **k: ran.append(1) or pull.EXIT_OK)
    args = ["--list", "cash_buyer", "--from", "2026-11-01", "--to", "2026-11-30", "--live", "--confirm-exports", "2"]
    assert pull.main(args) == pull.EXIT_OK and ran == []  # the first run bought both while we waited


def test_a_plan_that_grew_under_the_lock_is_refused():
    args = SimpleNamespace(confirm_exports=2, max_paid_exports=0)
    grown = pull.PullPlan(to_buy={"s": ["R1", "R2", "R3"]}, matching={"s": 3}, already_stored=0, free_remaining=10)
    assert "grew" in pull._refusal_after_lock(args, grown)


# ── 7. the export reads the same do-not-call results as the dialer gate ───────────────────────────────────────

def test_a_clean_result_only_in_the_shared_cache_is_exported(monkeypatch):
    from src.lending.compliance import ScrubResult

    person = {"contact_key": "ck1", "radar_id": "R1", "list_key": "cash_buyer", "qualifies": True,
              "match_strength": "confirmed", "owner_name": "LEE,ANN", "ownership_type": None, "principal_name": None,
              "property_address": "9 Oak Ave", "city": "Tampa", "state": "FL", "zip": "33602", "county": "Hillsborough",
              "apn": "1", "phones": [OWNER_PHONE]}
    db = SimpleNamespace(execute=lambda *a, **k: [SimpleNamespace(_mapping=person)])
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(export, "fresh_scrubs", lambda db, phones: {
        OWNER_PHONE: ScrubResult(national_dnc=False, litigator=False, state_dnc=False, checked_at=now)})
    records, counts = export.build(db, "cash_buyer")
    assert counts["load_ready"] == 1 and records[0]["phone"] == OWNER_PHONE


def test_a_stale_result_counts_as_not_checked(monkeypatch):
    from src.lending import compliance
    from src.lending.compliance import ScrubResult

    old = datetime.now(timezone.utc) - timedelta(days=30)
    monkeypatch.setattr(compliance, "_stored_scrubs", lambda db, phones: {
        OWNER_PHONE: ScrubResult(national_dnc=False, litigator=False, state_dnc=False, checked_at=old)})
    assert compliance.fresh_scrubs(None, [OWNER_PHONE]) == {}
    assert OWNER_PHONE in compliance.fresh_scrubs(None, [OWNER_PHONE], fresh_only=False)
