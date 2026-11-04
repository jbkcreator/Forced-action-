"""BatchDialer load -> GHL contact sync."""
from __future__ import annotations

import pytest

from config import lending_compliance as lc
from src.lending import ghl_load_sync as sync


def _row(i: int = 1, **kw):
    base = {"id": i, "pool": "verified_maturity", "source_record_ref": f"pr_list:verified_maturity:P{i}",
            "phone": f"+1813555010{i}", "borrower_name": "A B", "entity_name": None, "state": "FL",
            "warm": False, "suppressed": False, "ghl_sync_attempts": 0}
    return {**base, **kw}


class Sink:
    def __init__(self, fail: set[int] = frozenset()):
        self.fail, self.pushed = fail, []

    def push(self, record, tags):
        if record["id"] in self.fail:
            raise RuntimeError("contact upsert: HTTP 500")
        self.pushed.append((record["id"], tags))


def test_every_dialer_pool_has_a_ghl_call_list_tag():
    from config.lending_dialer import POOL_CAMPAIGN_TAGS
    assert set(POOL_CAMPAIGN_TAGS) <= set(lc.GHL_LOAD_SYNC_POOL_TAGS)
    assert set(lc.GHL_LOAD_SYNC_POOL_TAGS.values()) <= set(lc.GHL_CALL_LIST_TAGS)


@pytest.mark.parametrize("pool,state,tags", [
    ("verified_maturity", "GA", ["ndl-verified-maturity", "ndl-cold", "ndl-state-ga"]),
    ("partners", None, ["ndl-partner", "ndl-cold"]),
    ("builders", "fl", ["ndl-builder", "ndl-cold", "ndl-state-fl"]),
    ("no_such_pool", "FL", None),
])
def test_tags_follow_pool_and_state(pool, state, tags):
    assert sync.sync_tags({"pool": pool, "state": state}) == tags


def test_warm_suppressed_and_unmapped_are_skipped_not_pushed():
    sink = Sink()
    rows = [_row(1), _row(2, warm=True), _row(3, suppressed=True), _row(4, pool="mystery")]
    outcomes = {o.id: o.status for o in sync.process(rows, sink)}
    assert outcomes == {1: sync.PUSHED, 2: sync.SKIPPED_WARM, 3: sync.SKIPPED_SUPPRESSED, 4: sync.SKIPPED_UNMAPPED}
    assert sink.pushed == [(1, ["ndl-verified-maturity", "ndl-cold", "ndl-state-fl"])]


def test_failure_stays_pending_until_last_attempt():
    sink = Sink(fail={1, 2})
    last = lc.GHL_LOAD_SYNC_MAX_ATTEMPTS - 1
    retry, final = sync.process([_row(1), _row(2, ghl_sync_attempts=last)], sink)
    assert (retry.status, retry.attempted) == (None, 1) and "HTTP 500" in retry.error
    assert final.status == sync.FAILED


def test_dry_run_pushes_nothing():
    (outcome,) = sync.process([_row(1)], None)
    assert outcome.status == sync.PUSHED and outcome.attempted == 0


def test_run_records_outcomes_and_counts(monkeypatch):
    executed = []

    class Db:
        def execute(self, stmt, params):
            executed.append(params)

            class R:
                def mappings(self):
                    return [_row(1), _row(2, warm=True)]
            return R()

    result = sync.run(Db(), Sink())
    assert dict(result.counts) == {sync.PUSHED: 1, sync.SKIPPED_WARM: 1}
    assert executed[-1]["ids"] == [1, 2] and executed[-1]["statuses"] == [sync.PUSHED, sync.SKIPPED_WARM]


def test_main_idle_when_flag_off(monkeypatch):
    class S:
        lending_ghl_load_sync_enabled = False
    monkeypatch.setattr(sync, "get_settings", lambda: S())
    monkeypatch.setattr(sync, "ghl_sink", lambda: (_ for _ in ()).throw(AssertionError("must not push")))
    assert sync.main([]) == sync.EXIT_OK


def test_main_refuses_while_halted(monkeypatch):
    class S:
        lending_ghl_load_sync_enabled = True
    monkeypatch.setattr(sync, "get_settings", lambda: S())
    monkeypatch.setattr(sync, "is_globally_halted", lambda: True)
    assert sync.main([]) == sync.EXIT_REFUSED
