"""PropertyRadar calling lists: search criteria, how bought records are judged and stored, and the pull guards.

No network, no database: the PropertyRadar port is the fake, the store is recorded in memory.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pytest

from config import lending_calling_lists as cfg
from src.lending.pr_calling_lists import build_criteria_sets, contact_key, pull_window, shape_record
from src.services.property_radar_port import AllowanceInfo, FakePropertyRadarPort
from src.lending import pr_list_landing as landing
from src.tasks import lending_pr_calling_list_pull as task

AS_OF = date(2026, 11, 3)  # maturity window 2027-02-01 .. 2027-05-02


def _criterion(sets, label, name):
    criteria = next(s.criteria for s in sets if s.label == label)
    return next(c["value"] for c in criteria if c["name"] == name)


# Criteria

def test_verified_maturity_searches_each_assumed_term_for_private_and_named_lenders():
    window = pull_window(cfg.VERIFIED_MATURITY, as_of=AS_OF, start=None, end=None)
    assert (window.start, window.end) == (date(2027, 2, 1), date(2027, 5, 2))
    sets = build_criteria_sets(cfg.VERIFIED_MATURITY, as_of=AS_OF, window=window)
    assert [s.label for s in sets] == ["1yr_private", "1yr_hard_money_lender", "2yr_private",
                                       "2yr_hard_money_lender", "3yr_private", "3yr_hard_money_lender"]
    assert _criterion(sets, "1yr_private", "FirstDate") == ["from: 02/01/2026 to: 05/02/2026"]
    assert _criterion(sets, "3yr_hard_money_lender", "FirstDate") == ["from: 02/01/2024 to: 05/02/2024"]
    assert _criterion(sets, "2yr_private", "FirstLoanType") == ["P"]
    assert all(c["name"] != "PropertyType" for s in sets for c in s.criteria)  # land included
    assert "Kiavi" in _criterion(sets, "2yr_hard_money_lender", "FirstLenderOriginal")
    for s in sets:
        names = {c["name"]: c["value"] for c in s.criteria}
        assert names["State"] == ["FL"] and names["isSameMailingOrExempt"] == [0] and names["isListedForSale"] == [0]


def test_cash_buyers_are_all_cash_non_owner_occupied_owners_of_2_to_99_properties():
    window = pull_window(cfg.CASH_BUYER, as_of=AS_OF, start=date(2026, 10, 4), end=date(2026, 11, 3))
    (only,) = build_criteria_sets(cfg.CASH_BUYER, as_of=AS_OF, window=window)
    names = {c["name"]: c["value"] for c in only.criteria}
    assert names["LastTransferRecDate"] == ["from: 10/04/2026 to: 11/03/2026"]
    assert names["LastTransferDownPaymentPercent"] == [[100, None]]
    assert names["NumberPropertiesOwned"] == [[2, 99]]
    assert names["isSameMailingOrExempt"] == [0]


def test_wholesalers_search_flips_resold_in_the_window():
    window = pull_window(cfg.WHOLESALER, as_of=AS_OF, start=date(2026, 10, 4), end=date(2026, 11, 3))
    (only,) = build_criteria_sets(cfg.WHOLESALER, as_of=AS_OF, window=window)
    names = {c["name"]: c["value"] for c in only.criteria}
    assert names["LastTransferType"] == ["MarketFlip", "ShortSaleFlip", "REOFlip"]


def test_wholesalers_search_only_buys_resales_inside_the_hold_limit():
    window = pull_window(cfg.WHOLESALER, as_of=AS_OF, start=date(2026, 10, 4), end=date(2026, 11, 3))
    (only,) = build_criteria_sets(cfg.WHOLESALER, as_of=AS_OF, window=window)
    names = {c["name"]: c["value"] for c in only.criteria}
    assert names["PriorTransferMonthsSince"] == [[0, cfg.WHOLESALER_MAX_HOLD_MONTHS]]


def test_a_windowed_list_needs_a_valid_window():
    with pytest.raises(ValueError):
        pull_window(cfg.CASH_BUYER, as_of=AS_OF, start=None, end=None)
    with pytest.raises(ValueError):
        pull_window(cfg.WHOLESALER, as_of=AS_OF, start=date(2026, 11, 3), end=date(2026, 10, 4))


# Judging a bought record

def _raw(**fields):
    return {"RadarID": "P1", "Owner": "Sunshine Homes LLC", "OwnerAddress": "1 Main St", "OwnerZipFive": "33602",
            "State": "FL", **fields}


@pytest.mark.parametrize("recorded,term,expected", [
    ("2026-03-01", 1, ("confirmed", None)),                 # matures 2027-03-01: inside the window
    ("2025-03-01", 2, ("confirmed", None)),
    ("2026-03-01", 30, (None, "maturity_outside_window")),  # a 30-year loan found by the 1-year search
    ("2026-01-15", 1, (None, "maturity_outside_window")),   # matures 2027-01-15: before the window
    ("2026-03-01", None, ("likely", None)),                 # no term on record
    ("2026-03-01", "Unknown", ("likely", None)),
])
def test_verified_maturity_is_confirmed_by_the_term_or_likely_without_one(recorded, term, expected):
    row = shape_record(cfg.VERIFIED_MATURITY, _raw(FirstDate=recorded, FirstTermInYears=term), as_of=AS_OF)
    assert (row["match_strength"], row["exclusion_reason"]) == expected
    assert row["qualifies"] is (expected[1] is None)


@pytest.mark.parametrize("months,seller,expected", [
    (0, "Quick Flip LLC", ("confirmed", None)),
    (1, "Quick Flip LLC", (None, "resold_after_hold_limit")),   # 1 month can be 30-60 days: outside the rule
    (2, "Quick Flip LLC", (None, "resold_after_hold_limit")),
    (None, "Quick Flip LLC", (None, "hold_unknown")),
    (0, None, (None, "seller_unknown")),
])
def test_wholesalers_keep_resales_inside_30_days_and_need_a_seller(months, seller, expected):
    row = shape_record(cfg.WHOLESALER, _raw(PriorTransferMonthsSince=months, LastTransferSeller=seller), as_of=AS_OF)
    assert (row["match_strength"], row["exclusion_reason"]) == expected


def test_every_shaped_row_satisfies_the_tables_qualifies_rule():
    """The table checks qualifies = (exclusion_reason IS NULL); the shaper must never break it."""
    for list_key, raw in [(cfg.CASH_BUYER, _raw()), (cfg.WHOLESALER, _raw(PriorTransferMonthsSince=5)),
                          (cfg.VERIFIED_MATURITY, _raw(FirstDate="2026-03-01", FirstTermInYears=30))]:
        row = shape_record(list_key, raw, as_of=AS_OF)
        assert row["qualifies"] == (row["exclusion_reason"] is None)
        assert (row["match_strength"] is None) == (not row["qualifies"])


def test_a_cash_buyer_is_one_contact_however_the_name_is_written():
    a = shape_record(cfg.CASH_BUYER, _raw(Owner="Sunshine Homes, LLC"), as_of=AS_OF)
    b = shape_record(cfg.CASH_BUYER, _raw(RadarID="P2", Owner="SUNSHINE HOMES LLC"), as_of=AS_OF)
    other_zip = shape_record(cfg.CASH_BUYER, _raw(RadarID="P3", OwnerZipFive="32801"), as_of=AS_OF)
    assert a["contact_key"] == b["contact_key"] != other_zip["contact_key"]


def test_a_wholesaler_is_keyed_by_the_seller_not_the_buyer():
    row = shape_record(cfg.WHOLESALER, _raw(LastTransferSeller="Quick Flip LLC", PriorTransferMonthsSince=0), as_of=AS_OF)
    assert row["contact_key"] == contact_key("QUICK FLIP LLC")


def test_principal_and_numbers_are_read_from_the_record():
    row = shape_record(cfg.CASH_BUYER, _raw(
        Persons=[{"OwnershipRole": "Principal", "PersonType": "Person", "FirstName": "Ana", "LastName": "Diaz"}],
        LastTransferValue="350000", LastTransferDownPaymentPercent=100, LastTransferRecDate="2026-10-20"),
        as_of=AS_OF)
    assert row["principal_name"] == "Ana Diaz"
    assert row["last_transfer_value"] == 350000 and row["down_payment_pct"] == 100
    assert row["last_transfer_date"] == date(2026, 10, 20)


def test_contact_key_is_none_without_a_name():
    assert contact_key(None) is None and contact_key("  ") is None


# Storage: no paid row may be dropped

class FakeSavepoint:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RejectingDb:
    """Rejects the batch statement and any single row whose loan_type is too long."""

    def __init__(self):
        self.written = []

    def begin_nested(self):
        return FakeSavepoint(self)

    def execute(self, statement, params):
        from sqlalchemy.exc import DataError
        rows = params if isinstance(params, list) else [params]
        if isinstance(params, list) or any(len(r.get("loan_type") or "") > 10 for r in rows):
            raise DataError("INSERT", {}, Exception("value too long"))
        self.written.extend(rows)


def test_a_row_the_database_rejects_is_kept_raw_instead_of_losing_the_page():
    from src.lending import pr_calling_list_store as real_store
    good = shape_record(cfg.CASH_BUYER, _raw(RadarID="P1"), as_of=AS_OF)
    bad = shape_record(cfg.VERIFIED_MATURITY, _raw(RadarID="P2", FirstLoanType="Non-Conforming",
                                                    FirstDate="2026-03-01", FirstTermInYears=1), as_of=AS_OF)
    db = RejectingDb()
    assert real_store.store_records(db, 7, [good, bad]) == 2
    by_id = {r["radar_id"]: r for r in db.written}
    assert set(by_id) == {"P1", "P2"}
    assert by_id["P2"]["loan_type"] is None and by_id["P2"]["owner_name"] is None   # parsed columns left empty
    assert '"FirstLoanType": "Non-Conforming"' in by_id["P2"]["raw"]                 # the paid record is kept
    assert by_id["P2"]["qualifies"] is True and by_id["P2"]["match_strength"] == "confirmed"


# The pull task

class BillingFakePort(FakePropertyRadarPort):
    """The fake port, but buying a record uses up the free allowance like PropertyRadar does."""

    def purchase(self, criteria, max_records=None, fields=None):
        for record in super().purchase(criteria, max_records, fields):
            self.canned_allowance.quantity_free_remaining -= 1
            yield record


class MemoryStore:
    def __init__(self, stored=(), fail_on_store=False):
        self.stored, self.fail_on_store = set(stored), fail_on_store
        self.pulls, self.rows, self.updates = [], [], []

    def stored_radar_ids(self, db, list_key):
        return set(self.stored)

    held_elsewhere: dict = {}

    def raw_held_by_other_lists(self, db, list_key, ids):
        return {i: raw for i, raw in self.held_elsewhere.items() if i in ids}

    def start_pull(self, db, list_key, start, end, criteria, matching):
        self.pulls.append((list_key, start, end, matching))
        return 7

    def store_records(self, db, pull_id, rows):
        if self.fail_on_store and rows:
            raise RuntimeError("db down")
        self.rows.extend(rows)
        return len(rows)

    def update_pull(self, db, pull_id, **kwargs):
        self.updates.append(kwargs)

    def list_report(self, db, list_key):
        return {"records": len(self.rows)}


class FakeDb:
    def __init__(self):
        self.commits = self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def harness(monkeypatch, tmp_path):
    port = BillingFakePort(canned_records=[_raw(RadarID=f"P{i}") for i in range(3)],
                           canned_allowance=AllowanceInfo(quantity_free_remaining=10, quantity_purchased_remaining=0))
    memory, db = MemoryStore(), FakeDb()

    @contextmanager
    def db_context():
        yield db

    monkeypatch.setattr(task, "get_property_radar_port", lambda: port)
    monkeypatch.setattr(task, "get_db_context", db_context)
    monkeypatch.setattr(task, "store", memory)
    monkeypatch.setattr("src.lending.pr_list_landing.LANDING_ROOT", tmp_path)
    return {"port": port, "store": memory, "db": db, "landing": tmp_path}


CASH_ARGS = ["--list", "cash_buyer", "--from", "2026-10-04", "--to", "2026-11-03"]


def test_dry_run_lists_for_free_and_buys_nothing(harness):
    assert task.main(CASH_ARGS) == task.EXIT_OK
    assert harness["port"].purchase_calls == [] and harness["store"].pulls == []


def test_live_refuses_without_the_matching_export_confirmation(harness):
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "2"]) == task.EXIT_REFUSED
    assert harness["port"].purchase_calls == []


def test_live_refuses_to_go_past_the_free_allowance_without_a_paid_cap(harness):
    harness["port"].canned_allowance.quantity_free_remaining = 2
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_REFUSED
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3", "--max-paid-exports", "1"]) == task.EXIT_OK


def test_live_buys_by_radar_id_and_records_the_balance_spent(harness):
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_OK
    assert [r["radar_id"] for r in harness["store"].rows] == ["P0", "P1", "P2"]
    (bought,) = harness["port"].purchase_calls
    assert bought == [{"name": "RadarID", "value": ["P0", "P1", "P2"]}]
    assert harness["store"].updates[-1] == {"exports_consumed": 3, "records_stored": 3, "status": "done", "error": None}


def test_records_already_stored_are_never_bought_again(harness):
    harness["store"].stored = {"P0", "P2"}
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_REFUSED  # only 1 to buy now
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "1"]) == task.EXIT_OK
    assert harness["port"].purchase_calls == [[{"name": "RadarID", "value": ["P1"]}]]


def test_every_bought_page_is_on_disk_as_returned_before_the_database_sees_it(harness):
    harness["store"].fail_on_store = True
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_FAILED
    last = harness["store"].updates[-1]
    assert last["status"] == "failed" and last["exports_consumed"] == 3 and last["error"] == "RuntimeError"
    (page,) = landing.iter_pages("cash_buyer", root=harness["landing"])
    assert page.pull_id == 7 and page.records == harness["port"].canned_records   # exactly as returned
    assert not list(harness["landing"].rglob("*.tmp"))


def test_after_a_failed_database_write_nothing_is_bought_again_and_the_files_load(harness):
    harness["store"].fail_on_store = True
    task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"])
    harness["store"].fail_on_store = False
    purchases_before = len(harness["port"].purchase_calls)
    assert task.main(CASH_ARGS + ["--live"]) == task.EXIT_OK                       # nothing left to buy
    assert len(harness["port"].purchase_calls) == purchases_before
    assert task.main(CASH_ARGS + ["--load-files"]) == task.EXIT_OK
    assert sorted(r["radar_id"] for r in harness["store"].rows) == ["P0", "P1", "P2"]


def test_landing_files_round_trip(tmp_path):
    landing.write_page("wholesaler", 3, 1, [{"RadarID": "A"}, {"RadarID": "B", "Owner": "x"}], root=tmp_path)
    landing.write_page("wholesaler", 3, 2, [{"RadarID": "C"}], root=tmp_path)
    assert landing.landed_radar_ids("wholesaler", root=tmp_path) == {"A", "B", "C"}
    assert [p.path.name for p in landing.iter_pages("wholesaler", root=tmp_path)] == ["page_0001.json", "page_0002.json"]
    assert landing.landed_radar_ids("cash_buyer", root=tmp_path) == set()


def test_a_windowed_list_without_dates_is_a_bad_argument(harness):
    assert task.main(["--list", "wholesaler"]) == task.EXIT_BAD_ARGS


def test_config_is_valid():
    cfg.validate_calling_list_config()



def test_a_record_another_list_already_bought_is_copied_not_bought(harness):
    harness["store"].held_elsewhere = {"P1": _raw(RadarID="P1", Owner="From Cash Buyers")}
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "2"]) == task.EXIT_OK
    assert harness["port"].purchase_calls == [[{"name": "RadarID", "value": ["P0", "P2"]}]]
    assert sorted(r["radar_id"] for r in harness["store"].rows) == ["P0", "P1", "P2"]
    assert landing.landed_radar_ids("cash_buyer", root=harness["landing"]) == {"P0", "P1", "P2"}


def test_paid_exports_never_pass_the_cap_when_the_free_balance_drops_mid_run(harness, monkeypatch):
    """Something else (the daily pull) uses free exports during the run: the cap is re-checked per page."""
    monkeypatch.setattr(task, "BUY_PAGE", 1)
    port = harness["port"]
    port.canned_allowance.quantity_free_remaining = 3
    real_preview, calls = port.cost_preview, {"n": 0}

    def preview_with_other_usage(criteria):
        calls["n"] += 1
        if calls["n"] > 1:  # after planning, another job takes one free export per check
            port.canned_allowance.quantity_free_remaining = max(0, port.canned_allowance.quantity_free_remaining - 1)
        return real_preview(criteria)

    monkeypatch.setattr(port, "cost_preview", preview_with_other_usage)
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_FAILED
    last = harness["store"].updates[-1]
    assert last["status"] == "failed" and last["error"] == "PaidCapReached"
    assert len(harness["port"].purchase_calls) == 1          # stopped before the page that would cost money
    assert landing.landed_radar_ids("cash_buyer", root=harness["landing"]) == {"P0"}  # what was bought is kept


def test_only_one_pull_runs_at_a_time(harness):
    lock = landing.acquire_pull_lock(root=harness["landing"])
    try:
        with pytest.raises(landing.PullLocked):
            landing.acquire_pull_lock(root=harness["landing"])
        assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_REFUSED
        assert harness["port"].purchase_calls == []
    finally:
        landing.release_pull_lock(lock)
    assert task.main(CASH_ARGS + ["--live", "--confirm-exports", "3"]) == task.EXIT_OK
    assert not (harness["landing"] / ".pull.lock").exists()   # released after the run
