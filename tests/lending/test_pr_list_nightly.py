"""Nightly PropertyRadar calling-list pipeline: lists, states, windows and planning.

No network, no database: the PropertyRadar port is the fake.
"""
from __future__ import annotations

from datetime import date

import pytest

from config import lending_calling_lists as cfg
from src.lending import pr_list_landing as landing
from src.lending.pr_calling_lists import PullWindow, build_criteria_sets, nightly_window
from src.services.property_radar_port import FakePropertyRadarPort
from src.tasks.lending_pr_calling_list_pull import plan_pull

AS_OF = date(2026, 11, 20)


def _names(criteria_set) -> dict:
    return {c["name"]: c["value"] for c in criteria_set.criteria}


# ── lists and states ────────────────────────────────────────────────────────

def test_the_config_is_valid():
    cfg.validate_calling_list_config()


def test_nightly_lists_are_bought_maturity_first_then_cash_then_investor_purchases():
    assert cfg.NIGHTLY_LISTS == (cfg.VERIFIED_MATURITY, cfg.CASH_BUYER, cfg.INVESTOR_PURCHASE)


@pytest.mark.parametrize("list_key", [cfg.VERIFIED_MATURITY, cfg.CASH_BUYER, cfg.INVESTOR_PURCHASE])
def test_georgia_searches_company_owners_instead_of_the_owner_occupied_flag(list_key):
    window = nightly_window(list_key, as_of=AS_OF)
    for criteria_set in build_criteria_sets(list_key, as_of=AS_OF, window=window, state="GA"):
        names = _names(criteria_set)
        assert names["State"] == ["GA"] and names["OwnershipType"] == ["Corporate"]
        assert "isSameMailingOrExempt" not in names


@pytest.mark.parametrize("list_key", [cfg.VERIFIED_MATURITY, cfg.CASH_BUYER, cfg.INVESTOR_PURCHASE])
def test_florida_searches_non_owner_occupied_owners(list_key):
    window = nightly_window(list_key, as_of=AS_OF)
    for criteria_set in build_criteria_sets(list_key, as_of=AS_OF, window=window, state="FL"):
        names = _names(criteria_set)
        assert names["State"] == ["FL"] and names["isSameMailingOrExempt"] == [0]
        assert "OwnershipType" not in names


def test_an_unsupported_state_is_refused():
    with pytest.raises(ValueError):
        build_criteria_sets(cfg.CASH_BUYER, as_of=AS_OF, window=PullWindow(AS_OF, AS_OF), state="TX")


def test_investor_purchases_are_any_purchase_by_an_owner_of_2_to_99_properties():
    window = PullWindow(date(2026, 10, 21), date(2026, 11, 19))
    (only,) = build_criteria_sets(cfg.INVESTOR_PURCHASE, as_of=AS_OF, window=window)
    names = _names(only)
    assert names["LastTransferRecDate"] == ["from: 10/21/2026 to: 11/19/2026"]
    assert names["NumberPropertiesOwned"] == [[2, 99]]
    assert "LastTransferDownPaymentPercent" not in names  # cash or financed


# ── nightly windows ─────────────────────────────────────────────────────────

def test_the_nightly_maturity_window_ends_at_the_trigger_and_looks_back_for_late_recordings():
    window = nightly_window(cfg.VERIFIED_MATURITY, as_of=AS_OF)
    assert window.end == date(2027, 5, 4)  # 165 days after 2026-11-20
    assert (window.end - window.start).days == cfg.NIGHTLY_LOOKBACK_DAYS - 1


def test_the_nightly_maturity_window_searches_recordings_one_term_earlier():
    window = nightly_window(cfg.VERIFIED_MATURITY, as_of=AS_OF)
    sets = build_criteria_sets(cfg.VERIFIED_MATURITY, as_of=AS_OF, window=window)
    first_date = {s.label: _names(s)["FirstDate"] for s in sets}
    assert first_date["1yr_private"] == ["from: 04/05/2026 to: 05/04/2026"]
    assert first_date["3yr_hard_money_lender"] == ["from: 04/05/2024 to: 05/04/2024"]


def test_a_maturity_window_on_feb_29_maps_to_feb_28_in_a_non_leap_year():
    sets = build_criteria_sets(cfg.VERIFIED_MATURITY, as_of=AS_OF,
                               window=PullWindow(date(2028, 2, 1), date(2028, 2, 29)))
    first_date = {s.label: _names(s)["FirstDate"] for s in sets}
    assert first_date["1yr_private"] == ["from: 02/01/2027 to: 02/28/2027"]


@pytest.mark.parametrize("list_key", [cfg.CASH_BUYER, cfg.INVESTOR_PURCHASE])
def test_the_nightly_purchase_window_ends_yesterday(list_key):
    window = nightly_window(list_key, as_of=AS_OF)
    assert window.end == date(2026, 11, 19) and window.start == date(2026, 10, 21)


def test_a_list_without_a_nightly_window_is_refused():
    with pytest.raises(ValueError):
        nightly_window("not_a_list", as_of=AS_OF)


# ── planning ────────────────────────────────────────────────────────────────

def _port(*radar_ids: str) -> FakePropertyRadarPort:
    return FakePropertyRadarPort(canned_records=[{"RadarID": rid} for rid in radar_ids])


def test_a_record_left_to_another_list_is_never_bought():
    sets = build_criteria_sets(cfg.INVESTOR_PURCHASE, as_of=AS_OF, window=nightly_window(cfg.INVESTOR_PURCHASE, as_of=AS_OF))
    plan = plan_pull(_port("A", "B", "C"), sets, stored={"A"}, exclude=frozenset({"B"}))
    assert plan.to_buy == {"investor_purchases": ["C"]}
    assert (plan.already_stored, plan.excluded, plan.matched_ids) == (1, 1, frozenset({"A", "B", "C"}))


def test_a_record_left_to_another_list_is_not_copied_from_it_either():
    sets = build_criteria_sets(cfg.INVESTOR_PURCHASE, as_of=AS_OF, window=nightly_window(cfg.INVESTOR_PURCHASE, as_of=AS_OF))
    plan = plan_pull(_port("B", "C"), sets, stored=set(), exclude=frozenset({"B"}),
                     held_elsewhere=lambda ids: {rid: {"RadarID": rid} for rid in ids})
    assert plan.to_buy == {"investor_purchases": []} and set(plan.reused) == {"C"}


# ── pull lock ───────────────────────────────────────────────────────────────

def test_a_lock_left_by_a_process_that_is_gone_is_taken_over(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("pid 999999", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: False)
    path = landing.acquire_pull_lock(root=tmp_path)
    assert path.read_text(encoding="utf-8").startswith("pid ")
    landing.release_pull_lock(path)


def test_a_lock_held_by_a_running_process_is_respected(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("pid 4242", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: True)
    with pytest.raises(landing.PullLocked):
        landing.acquire_pull_lock(root=tmp_path)
    assert (tmp_path / ".pull.lock").read_text(encoding="utf-8") == "pid 4242"


def test_an_unreadable_lock_is_never_taken_over(tmp_path, monkeypatch):
    (tmp_path / ".pull.lock").write_text("garbage", encoding="utf-8")
    monkeypatch.setattr(landing, "_process_running", lambda pid: False)
    with pytest.raises(landing.PullLocked):
        landing.acquire_pull_lock(root=tmp_path)


# ── the night and its spend ─────────────────────────────────────────────────

import json  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from src.tasks import lending_pr_nightly as nightly  # noqa: E402
from src.tasks.lending_pr_calling_list_pull import PullPlan  # noqa: E402

NIGHT = date(2026, 11, 20)


def test_every_stage_of_one_night_shares_its_et_date():
    assert nightly.night_of(datetime(2026, 11, 20, 6, 30, tzinfo=timezone.utc)) == NIGHT  # 01:30 EST pull
    assert nightly.night_of(datetime(2026, 11, 20, 14, 5, tzinfo=timezone.utc)) == NIGHT  # 09:05 EST load
    assert nightly.night_start(NIGHT) == datetime(2026, 11, 20, 5, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("utc_hour,expected", [(13, False), (14, True), (15, False)])
def test_the_load_acts_only_during_9_am_eastern_in_winter(utc_hour, expected):
    assert nightly.in_load_hour(datetime(2026, 11, 20, utc_hour, 5, tzinfo=timezone.utc)) is expected


def test_the_load_hour_follows_daylight_saving():
    assert nightly.in_load_hour(datetime(2026, 7, 1, 13, 5, tzinfo=timezone.utc))  # 09:05 EDT


def _land(folder, name, sent):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(json.dumps({"sent": sent, "received": []}), encoding="utf-8")


def test_trace_spend_counts_every_row_sent_tonight_including_pending_and_address_only(tmp_path):
    maturity = nightly.nightly_folder(cfg.VERIFIED_MATURITY, NIGHT, tmp_path)
    cash = nightly.nightly_folder(cfg.CASH_BUYER, NIGHT, tmp_path)
    _land(maturity, "trace_0001.json", [{}] * 5)
    _land(maturity, "pending_trace_0002.json", [{}] * 3)  # accepted, not polled yet: still counted
    _land(cash, "advanced_0001.json", [{}] * 2)  # address-only: 2 credits a hit
    _land(cash, "dnc_0001.json", ["+18135550100"])  # not a trace
    _land(nightly.nightly_folder(cfg.CASH_BUYER, date(2026, 11, 19), tmp_path), "trace_0001.json", [{}] * 50)
    assert nightly.trace_credits_sent(NIGHT, tmp_path) == 5 + 3 + 4
    assert nightly.dnc_credits_sent(NIGHT, tmp_path) == 1


def test_caps_read_from_settings_and_refuse_an_unsupported_state():
    settings = SimpleNamespace(lending_pr_nightly_states=" fl, GA ", lending_pr_nightly_max_exports=300,
                               lending_pr_nightly_max_paid_exports=-5, lending_pr_nightly_trace_max_credits=40,
                               lending_pr_nightly_dnc_max_credits=40)
    caps = nightly.nightly_caps(settings)
    assert caps.states == ("FL", "GA") and caps.max_exports == 300 and caps.max_paid_exports == 0
    with pytest.raises(ValueError):
        nightly.nightly_caps(SimpleNamespace(**{**vars(settings), "lending_pr_nightly_states": "FL,TX"}))


# ── pull ────────────────────────────────────────────────────────────────────

def _plan(ids, free=10_000):
    return PullPlan(to_buy={"search": list(ids)}, matching={"search": len(ids)}, already_stored=0,
                    free_remaining=free, matched_ids=frozenset(ids))


def test_a_trimmed_plan_keeps_the_first_purchases_in_search_order():
    plan = PullPlan(to_buy={"a": ["1", "2"], "b": ["3", "4"]}, matching={}, already_stored=0, free_remaining=9)
    assert nightly.trim_plan(plan, 3).to_buy == {"a": ["1", "2"], "b": ["3"]}
    assert nightly.trim_plan(plan, 0).total == 0


class NightStore:
    def __init__(self, spent_tonight=0, cash_stored=()):
        self.spent_tonight, self.cash_stored = spent_tonight, set(cash_stored)
        self.pulls, self.updates = [], []

    def nightly_exports_since(self, db, since):
        return self.spent_tonight

    def stored_radar_ids(self, db, list_key):
        return self.cash_stored if list_key == cfg.CASH_BUYER else set()

    def start_pull(self, db, list_key, start, end, criteria, matching, *, state, run_kind):
        self.pulls.append((list_key, state, run_kind, matching))
        return len(self.pulls)

    def update_pull(self, db, pull_id, **values):
        self.updates.append((pull_id, values))


class Db:
    def commit(self):
        pass

    def rollback(self):
        pass


def _run_pull(plans, *, caps=None, store=None, apply=True, fail_on=None):
    """plans: (list_key, state) -> ids the free listing matches. Records what each list was asked to buy."""
    asked: dict = {}
    excluded: dict = {}

    def make_plan(db, port, list_key, sets, window, exclude=frozenset()):
        state = next(c["value"][0] for c in sets[0].criteria if c["name"] == "State")
        excluded[(list_key, state)] = exclude
        return _plan([i for i in plans.get((list_key, state), []) if i not in exclude])

    def buy_and_store(db, port, pull_id, list_key, plan, *, as_of, progress, max_paid, balance_criteria):
        key = next(k for k in plans if k[0] == list_key and set(plans[k]) >= {i for ids in plan.to_buy.values() for i in ids})
        if fail_on == key:
            raise RuntimeError("provider down")
        asked[key] = [i for ids in plan.to_buy.values() for i in ids]
        progress.bought = progress.stored = plan.total

    caps = caps or nightly.NightlyCaps(states=("FL", "GA"), max_exports=100, max_paid_exports=0, trace_credits=0,
                                       dnc_credits=0)
    store = store or NightStore()
    outcome = nightly.run_pull(Db(), port=None, caps=caps, night=NIGHT, apply=apply, make_plan=make_plan,
                               buy_and_store=buy_and_store, store=store)
    return outcome, asked, excluded, store


def test_the_night_buys_maturity_first_and_stops_at_the_export_cap():
    plans = {(cfg.VERIFIED_MATURITY, "FL"): ["M1", "M2", "M3"], (cfg.CASH_BUYER, "FL"): ["C1", "C2"],
             (cfg.INVESTOR_PURCHASE, "FL"): ["I1"]}
    caps = nightly.NightlyCaps(states=("FL",), max_exports=4, max_paid_exports=0, trace_credits=0, dnc_credits=0)
    outcome, asked, _, store = _run_pull(plans, caps=caps)
    assert asked == {(cfg.VERIFIED_MATURITY, "FL"): ["M1", "M2", "M3"], (cfg.CASH_BUYER, "FL"): ["C1"]}
    assert outcome.bought == 4 and outcome.left_for_later == 2  # C2 and I1 wait for a later night
    assert all(run_kind == "nightly" for _, _, run_kind, _ in store.pulls)


def test_a_retried_run_counts_what_the_night_already_spent():
    plans = {(cfg.VERIFIED_MATURITY, "FL"): ["M1", "M2", "M3"]}
    caps = nightly.NightlyCaps(states=("FL",), max_exports=5, max_paid_exports=0, trace_credits=0, dnc_credits=0)
    outcome, asked, _, _ = _run_pull(plans, caps=caps, store=NightStore(spent_tonight=4))
    assert asked == {(cfg.VERIFIED_MATURITY, "FL"): ["M1"]} and outcome.bought == 1


def test_paid_exports_are_never_bought_without_a_paid_cap():
    plans = {(cfg.VERIFIED_MATURITY, "FL"): ["M1", "M2", "M3"]}
    caps = nightly.NightlyCaps(states=("FL",), max_exports=100, max_paid_exports=0, trace_credits=0, dnc_credits=0)

    def make_plan(db, port, list_key, sets, window, exclude=frozenset()):
        return _plan(plans.get((list_key, "FL"), []), free=2)  # only 2 free exports left this month

    bought = {}

    def buy(db, port, pull_id, list_key, plan, *, progress, **kw):
        bought[list_key] = plan.total
        progress.bought = plan.total

    nightly.run_pull(Db(), None, caps=caps, night=NIGHT, apply=True, make_plan=make_plan, buy_and_store=buy,
                     store=NightStore())
    assert bought == {cfg.VERIFIED_MATURITY: 2}


def test_investor_purchases_leave_tonights_and_earlier_cash_purchases_to_the_cash_list():
    plans = {(cfg.CASH_BUYER, "FL"): ["C1"], (cfg.INVESTOR_PURCHASE, "FL"): ["C1", "C0", "I1"]}
    caps = nightly.NightlyCaps(states=("FL",), max_exports=100, max_paid_exports=0, trace_credits=0, dnc_credits=0)
    _, asked, excluded, _ = _run_pull(plans, caps=caps, store=NightStore(cash_stored={"C0"}))
    assert excluded[(cfg.INVESTOR_PURCHASE, "FL")] == {"C1", "C0"}
    assert asked[(cfg.INVESTOR_PURCHASE, "FL")] == ["I1"]


def test_both_states_are_pulled_for_every_list():
    plans = {(k, s): [f"{k[:2]}{s}"] for k in cfg.NIGHTLY_LISTS for s in ("FL", "GA")}
    _, asked, _, _ = _run_pull(plans)
    assert set(asked) == set(plans)


def test_a_dry_run_plans_every_list_and_buys_nothing():
    plans = {(cfg.VERIFIED_MATURITY, "FL"): ["M1"]}
    outcome, asked, _, store = _run_pull(plans, apply=False)
    assert asked == {} and store.pulls == [] and outcome.by_list[f"{cfg.VERIFIED_MATURITY}:FL"]["buying_tonight"] == 1


def test_a_failed_purchase_stops_the_night_and_is_recorded_failed():
    plans = {(cfg.VERIFIED_MATURITY, "FL"): ["M1"], (cfg.CASH_BUYER, "FL"): ["C1"]}
    caps = nightly.NightlyCaps(states=("FL",), max_exports=100, max_paid_exports=0, trace_credits=0, dnc_credits=0)
    outcome, asked, _, store = _run_pull(plans, caps=caps, fail_on=(cfg.VERIFIED_MATURITY, "FL"))
    assert outcome.stopped and asked == {}
    assert store.updates[-1][1]["status"] == "failed"


# ── trace and DNC caps ──────────────────────────────────────────────────────

def test_the_dnc_stage_spends_only_what_is_left_of_tonights_credits(tmp_path, monkeypatch):
    from src.tasks import lending_pr_calling_list_trace as trace

    _land(nightly.nightly_folder(cfg.VERIFIED_MATURITY, NIGHT, tmp_path), "dnc_0001.json", ["p"] * 7)
    calls = []
    monkeypatch.setattr(trace, "run_dnc", lambda db, ids, **kw: calls.append(kw["max_phones"]) or {"clean": 0})

    class QueryDb(Db):
        def execute(self, statement, params):
            return [("R1",)]

    caps = nightly.NightlyCaps(states=("FL",), max_exports=0, max_paid_exports=0, trace_credits=0, dnc_credits=10)
    nightly.run_dnc_stage(QueryDb(), caps=caps, night=NIGHT, balance=lambda: 100, scrubber=None, apply=True,
                          root=tmp_path)
    assert calls == [3, 3, 3]  # 10 - 7 already sent, for each list (run_dnc stubbed: nothing new lands)


def test_the_dnc_stage_never_spends_more_than_the_balance(tmp_path, monkeypatch):
    from src.tasks import lending_pr_calling_list_trace as trace

    calls = []
    monkeypatch.setattr(trace, "run_dnc", lambda db, ids, **kw: calls.append(kw["max_phones"]) or {})

    class QueryDb(Db):
        def execute(self, statement, params):
            return []

    caps = nightly.NightlyCaps(states=("FL",), max_exports=0, max_paid_exports=0, trace_credits=0, dnc_credits=500)
    result = nightly.run_dnc_stage(QueryDb(), caps=caps, night=NIGHT, balance=lambda: 0, scrubber=None, apply=True,
                                   root=tmp_path)
    assert calls == [] and all("skipped" in r for r in result.values())


def test_the_trace_stage_replays_paid_results_but_traces_nothing_once_the_credits_are_used(tmp_path, monkeypatch):
    from src.tasks import lending_pr_calling_list_trace as trace

    _land(nightly.nightly_folder(cfg.CASH_BUYER, NIGHT, tmp_path), "trace_0001.json", [{}] * 20)
    replayed, traced = [], []
    monkeypatch.setattr(trace, "load_leads", lambda db, k: ["lead"])
    monkeypatch.setattr(trace, "resume_pending", lambda k, fetch: 0)
    monkeypatch.setattr(trace, "replay_landed", lambda db, k, **kw: replayed.append(k) or {})
    monkeypatch.setattr(trace, "run_trace", lambda *a, **kw: traced.append(kw["trace_limit"]) or {})
    caps = nightly.NightlyCaps(states=("FL",), max_exports=0, max_paid_exports=0, trace_credits=20, dnc_credits=0)
    nightly.run_trace_stage(Db(), caps=caps, night=NIGHT, balance=lambda: 1000, submit_for=lambda f: None,
                            fetch=lambda q: [], write_ledger=None, write_contacts=None, apply=True, root=tmp_path)
    assert replayed == list(cfg.NIGHTLY_LISTS) and traced == []


def test_the_trace_stage_traces_within_the_credits_left(tmp_path, monkeypatch):
    from src.tasks import lending_pr_calling_list_trace as trace

    traced = []
    monkeypatch.setattr(trace, "load_leads", lambda db, k: ["lead"])
    monkeypatch.setattr(trace, "resume_pending", lambda k, fetch: 0)
    monkeypatch.setattr(trace, "replay_landed", lambda db, k, **kw: {})
    monkeypatch.setattr(trace, "run_trace", lambda *a, **kw: traced.append(kw["trace_limit"]) or {})
    monkeypatch.setattr(trace, "reconcile_queues", lambda *a, **kw: {})
    caps = nightly.NightlyCaps(states=("FL",), max_exports=0, max_paid_exports=0, trace_credits=25, dnc_credits=0)
    nightly.run_trace_stage(Db(), caps=caps, night=NIGHT, balance=lambda: 10, submit_for=lambda f: None,
                            fetch=lambda q: [], write_ledger=None, write_contacts=None, apply=True, root=tmp_path)
    assert traced == [10, 10, 10]  # the balance (10) is below the night's credits (25)


# ── morning load ────────────────────────────────────────────────────────────

def test_the_load_takes_each_phone_once_earliest_list_first_and_skips_phones_already_loaded(monkeypatch):
    from src.tasks import lending_pr_calling_list_export as export

    built = {cfg.VERIFIED_MATURITY: [{"phone": "+1A", "pool": "verified_maturity"}],
             cfg.CASH_BUYER: [{"phone": "+1A", "pool": "transaction_ready"}, {"phone": "+1B", "pool": "transaction_ready"}],
             cfg.INVESTOR_PURCHASE: [{"phone": "+1C", "pool": "transaction_ready"}]}
    monkeypatch.setattr(export, "build", lambda db, k: (built[k], {"load_ready": len(built[k])}))

    class ActiveDb(Db):
        def execute(self, statement, params):
            return [("+1C",)]  # already in BatchDialer

    records, counts = nightly.load_ready_records(ActiveDb())
    assert [(r["phone"], r["pool"]) for r in records] == [("+1A", "verified_maturity"), ("+1B", "transaction_ready")]
    assert counts["already_loaded"] == 1 and counts[cfg.CASH_BUYER]["on_an_earlier_list"] == 1


def test_the_switched_off_pipeline_does_nothing(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_pr_nightly_enabled", False, raising=False)
    monkeypatch.setattr(nightly, "_run_stage", lambda *a: pytest.fail("ran while switched off"))
    assert nightly.main(["--stage", "pull", "--apply"]) == nightly.EXIT_OK


def test_the_load_outside_its_hour_does_nothing(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_pr_nightly_enabled", True, raising=False)
    monkeypatch.setattr(nightly, "in_load_hour", lambda now: False)
    monkeypatch.setattr(nightly, "_run_stage", lambda *a: pytest.fail("loaded outside 09:xx ET"))
    assert nightly.main(["--stage", "load", "--apply"]) == nightly.EXIT_OK


def test_the_live_pull_refuses_a_fake_property_radar(monkeypatch):
    settings = SimpleNamespace(property_radar_mode="fake")
    summary, failed = nightly._stage_pull(Db(), settings, None, NIGHT, apply=True)
    assert failed and "refused" in summary


# ── do-not-call places, GHL tags ───────────────────────────────────────────

def test_a_georgia_company_is_judged_as_a_georgia_company():
    from src.tasks.lending_pr_calling_list_trace import record_places

    class PlacesDb:
        def execute(self, statement, params):
            return SimpleNamespace(all=lambda: [SimpleNamespace(radar_id="G1", state="GA", ownership_type="Corporate"),
                                                SimpleNamespace(radar_id="G2", state="GA", ownership_type="Individual")])

    places = record_places(PlacesDb(), ["G1", "G2"])
    assert places == {"G1": {"state": "GA", "entity_status": "LLC"}, "G2": {"state": "GA", "entity_status": None}}


@pytest.mark.parametrize("calling_list,expected", [
    ("verified_maturity", ["ndl", "ndl-sig-maturity"]),
    ("cash_buyer", ["ndl", "ndl-sig-cash-purchase"]),
    ("investor_purchase", ["ndl", "ndl-sig-portfolio"]),
    ("wholesaler", []),
    (None, []),
])
def test_calling_list_contacts_get_the_ndl_and_signal_tags(calling_list, expected):
    from src.lending.ghl_load_sync import sync_tags

    tags = sync_tags({"pool": "verified_maturity", "state": "FL", "calling_list": calling_list})
    assert tags == ["ndl-verified-maturity", "ndl-cold", "ndl-state-fl", *expected]


def test_the_ndl_and_signal_tags_are_never_call_lists():
    from config import lending_compliance as lc

    lc.validate_lending_compliance_config()
    assert not ({lc.GHL_TAG_NDL, *lc.GHL_CALLING_LIST_SIGNAL_TAGS.values()} & set(lc.GHL_CALL_LIST_TAGS))


def test_investor_purchases_land_in_transaction_ready():
    from config.lending_queues import SOURCE_TAG_QUEUES
    from src.tasks.lending_pr_calling_list_export import LIST_EXPORTS, investor_purchase_record

    person = {"list_key": cfg.INVESTOR_PURCHASE, "radar_id": "I1", "ownership_type": "Corporate",
              "owner_name": "Acme LLC", "principal_name": "Jane Doe", "property_address": "1 Main St", "city": "Tampa",
              "zip": "33602", "county": "Hillsborough", "state": "FL", "apn": "A1", "match_strength": "confirmed"}
    record = investor_purchase_record(person, "+18135550100")
    assert record["pool"] == LIST_EXPORTS[cfg.INVESTOR_PURCHASE][2] == SOURCE_TAG_QUEUES[record["source_tag"]]
    assert record["source_record_ref"] == "pr_list:investor_purchase:I1"
