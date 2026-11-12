"""BatchDialer load -> GHL contact sync."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from config import lending_compliance as lc
from src.lending import ghl_load_sync as sync
from src.lending.ghl_load_sync import SyncStatus


def _row(i: int = 1, **kw):
    base = {"id": i, "pool": "verified_maturity", "source_record_ref": f"pr_list:verified_maturity:P{i}",
            "phone": f"+1813555010{i}", "borrower_name": "A B", "entity_name": None, "state": "FL",
            "wholesaler_source": None, "warm": False, "suppressed": False, "already_in_ghl": False,
            "ghl_sync_attempts": 0}
    return {**base, **kw}


class Sink:
    def __init__(self, fail: set[int] = frozenset(), error: Exception | None = None):
        self.fail, self.pushed = fail, []
        self.error = error or RuntimeError("contact upsert: HTTP 500")

    def push(self, record, tags):
        if record["id"] in self.fail:
            raise self.error
        self.pushed.append((record["id"], tags))
        return f"ghl-{record['id']}"


def test_every_dialer_pool_has_a_ghl_call_list_tag():
    from config.lending_dialer import POOL_CAMPAIGN_TAGS
    assert set(POOL_CAMPAIGN_TAGS) <= set(lc.GHL_LOAD_SYNC_POOL_TAGS)
    assert set(lc.GHL_LOAD_SYNC_POOL_TAGS.values()) <= set(lc.GHL_CALL_LIST_TAGS)
    lc.validate_lending_compliance_config()


@pytest.mark.parametrize("row,tags", [
    ({"pool": "verified_maturity", "state": "GA"}, ["ndl-verified-maturity", "ndl-cold", "ndl-state-ga"]),
    ({"pool": "partners", "state": None}, ["ndl-partner", "ndl-cold"]),
    ({"pool": "wholesalers", "state": "FL", "wholesaler_source": "sunbiz"},
     ["ndl-wholesaler", "ndl-cold", "ndl-state-fl", "ndl-src-sunbiz"]),
    ({"pool": "no_such_pool", "state": "FL"}, None),
])
def test_tags_follow_pool_state_and_source(row, tags):
    assert sync.sync_tags(row) == tags


def test_skips_warm_suppressed_reloaded_and_unmapped():
    sink = Sink()
    rows = [_row(1), _row(2, warm=True), _row(3, suppressed=True), _row(4, already_in_ghl=True),
            _row(5, pool="mystery")]
    outcomes = {o.id: o.status for o in sync.process(rows, sink)}
    assert outcomes == {1: SyncStatus.PUSHED, 2: SyncStatus.SKIPPED_WARM, 3: SyncStatus.SKIPPED_SUPPRESSED,
                        4: SyncStatus.SKIPPED_ALREADY_IN_GHL, 5: SyncStatus.SKIPPED_UNMAPPED}
    assert sink.pushed == [(1, ["ndl-verified-maturity", "ndl-cold", "ndl-state-fl"])]


def test_hold_tag_rides_on_every_push():
    sink = Sink()
    sync.process([_row(1)], sink, extra_tags=(lc.GHL_DO_NOT_CALL_NOW_TAG,))
    assert sink.pushed[0][1][-1] == "ndl-do-not-call-now"


@pytest.mark.parametrize("gate_on,expected", [(True, ("ndl-do-not-call-now",)), (False, ())])
def test_hold_tag_only_while_the_call_gate_runs(monkeypatch, gate_on, expected):
    class S:
        lending_ghl_call_gate_enabled = gate_on
    monkeypatch.setattr(sync, "get_settings", lambda: S())
    assert sync.hold_tags() == expected


def test_halt_mid_run_stops_and_leaves_the_rest_pending(monkeypatch):
    monkeypatch.setattr(sync, "GHL_LOAD_SYNC_HALT_CHECK_EVERY", 2)
    checks = iter([False, True])
    sink = Sink()
    outcomes = sync.process([_row(i) for i in range(1, 6)], sink, halted=lambda: next(checks))
    assert [o.id for o in outcomes] == [1, 2] and [p[0] for p in sink.pushed] == [1, 2]


def test_halt_before_first_row_pushes_nothing():
    sink = Sink()
    assert sync.process([_row(1)], sink, halted=lambda: True) == [] and sink.pushed == []


def test_failure_stays_pending_until_last_attempt():
    sink = Sink(fail={1, 2})
    last = lc.GHL_LOAD_SYNC_MAX_ATTEMPTS - 1
    retry, final = sync.process([_row(1), _row(2, ghl_sync_attempts=last)], sink)
    assert (retry.status, retry.attempted, retry.error) == (None, 1, "RuntimeError: contact upsert: HTTP 500")
    assert final.status is SyncStatus.FAILED


@pytest.mark.parametrize("exc,stored", [
    (RuntimeError("add tags: HTTP 422"), "RuntimeError: add tags: HTTP 422"),
    (RuntimeError("contact upsert: ConnectionError"), "RuntimeError: contact upsert: ConnectionError"),
    (RuntimeError("bad phone +18135550101 for Ann Lee"), "RuntimeError"),
    (ValueError("+18135550101"), "ValueError"),
])
def test_stored_error_never_carries_request_data(exc, stored):
    assert sync.safe_error(exc) == stored


def test_dry_run_pushes_nothing():
    (outcome,) = sync.process([_row(1)], None)
    assert outcome.status is SyncStatus.PUSHED and outcome.attempted == 0


def test_summarize_counts_retries_and_failures():
    result = sync.summarize([sync.Outcome(1, SyncStatus.PUSHED), sync.Outcome(2, None),
                             sync.Outcome(3, SyncStatus.FAILED)])
    assert dict(result.counts) == {"pushed": 1, "retry": 1, "failed": 1} and result.newly_failed == [3]


def test_record_writes_all_outcomes_in_one_statement():
    calls = []

    class Db:
        def execute(self, stmt, params):
            calls.append(params)

    sync.record(Db(), [sync.Outcome(1, SyncStatus.PUSHED, 1), sync.Outcome(2, None, 1, "RuntimeError")])
    (params,) = calls
    assert params["ids"] == [1, 2] and params["statuses"] == ["pushed", None]
    assert params["attempted"] == [1, 1] and params["errors"] == [None, "RuntimeError"]


def test_notify_failed_never_raises(monkeypatch):
    from src.lending import ops_alerts
    monkeypatch.setattr(ops_alerts, "post_ops_notice", lambda m: (_ for _ in ()).throw(RuntimeError("slack")))
    sync._notify_failed([1, 2])


def _settings(monkeypatch, enabled: bool):
    class S:
        lending_ghl_load_sync_enabled = enabled
        lending_ghl_call_gate_enabled = False
    monkeypatch.setattr(sync, "get_settings", lambda: S())


def test_main_idle_when_flag_off(monkeypatch):
    _settings(monkeypatch, False)
    monkeypatch.setattr(sync, "ghl_sink", lambda: (_ for _ in ()).throw(AssertionError("must not push")))
    assert sync.main([]) == sync.EXIT_OK


def test_main_refuses_while_halted(monkeypatch):
    _settings(monkeypatch, True)
    monkeypatch.setattr(sync, "is_globally_halted", lambda: True)
    assert sync.main([]) == sync.EXIT_REFUSED


def test_live_run_reads_commits_then_pushes_then_records(monkeypatch):
    """No transaction is open while GHL is called: the read commits first, outcomes go in a second transaction."""
    events = []

    class Db:
        def execute(self, stmt, params):
            events.append("record" if "ids" in params else "read")

            class R:
                def mappings(self):
                    return [_row(1)]
            return R()

        def commit(self):
            events.append("commit")

    @contextmanager
    def ctx():
        yield Db()

    class LoggingSink(Sink):
        def push(self, record, tags):
            events.append("push")
            super().push(record, tags)

    import src.core.database as database
    monkeypatch.setattr(database, "get_db_context", ctx)
    monkeypatch.setattr(sync, "is_globally_halted", lambda: False)
    _settings(monkeypatch, True)
    result = sync._live_run(10, LoggingSink())
    assert events == ["read", "commit", "push", "record", "commit"]
    assert dict(result.counts) == {"pushed": 1}


def test_every_status_fits_the_status_column():
    from src.lending.models import LendingDialerLoadRecord

    width = LendingDialerLoadRecord.__table__.c.ghl_sync_status.type.length
    assert max(len(s.value) for s in SyncStatus) <= width



def test_a_pushed_row_records_the_ghl_contact_id_and_a_failed_one_does_not():
    by_id = {o.id: o for o in sync.process([_row(1), _row(2)], Sink(fail={2}))}
    assert by_id[1].ghl_contact_id == "ghl-1" and by_id[2].ghl_contact_id is None
