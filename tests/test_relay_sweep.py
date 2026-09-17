"""
Tests for src.services.relay.sweep (RELAY-v2.2 sub-tasks R1 + R3).

R3 addition under test: run_sweep() calls sync_unsubscribes() before
execute_batch().

WP-T2-1 (production-execution review finding 3): a sync FAILURE
(SuppressionSyncFailed) must now defer the whole batch rather than execute
it against a possibly stale suppression list -- the opposite of R3's
original "must never stop already-approved sends" rule, which conflated an
unconfigured venture (not an error) with a genuine poll failure (a reason
to hold off). See test_sync_failure_defers_the_batch below.

PR #195 review addition: run_sweep() must refuse to execute a batch for a
deactivated venture (ventures.is_active = false) rather than dispatch its
approved items under whatever config it resolves to.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services.relay import sweep
from src.services.relay.suppression_sync import SuppressionSyncFailed, SyncResult


def _synced(count: int = 0):
    return lambda venture_key=None: SyncResult(status="synced", count=count)


def test_sweep_returns_empty_result_with_no_approved_items(monkeypatch):
    monkeypatch.setattr(sweep, "sync_unsubscribes", _synced())
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    result = sweep.run_sweep()

    assert result.sent == 0
    assert result.processed_ids == []


def test_sweep_calls_sync_before_execute(monkeypatch):
    order = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: order.append("sync") or SyncResult(status="synced"))
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: order.append("query") or [])

    sweep.run_sweep()

    assert order == ["sync", "query"]


def test_sync_failure_defers_the_batch(monkeypatch):
    """A failed Instantly poll means this batch's suppression view may be
    stale, so run_sweep() must defer (not execute) the whole batch and page
    EXCEPTIONS -- reversed from R3's original 'never stop already-approved
    sends' behavior (production-execution review finding 3)."""
    def _raiser(venture_key=None):
        raise SuppressionSyncFailed("Instantly API down")

    monkeypatch.setattr(sweep, "sync_unsubscribes", _raiser)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [SimpleNamespace(id=1), SimpleNamespace(id=2)])
    alerts = []
    monkeypatch.setattr(sweep, "post_exceptions_alert", lambda **kw: alerts.append(kw) or True)
    executed = []
    monkeypatch.setattr(sweep, "execute_batch", lambda *a, **k: executed.append((a, k)))

    result = sweep.run_sweep()  # must not raise

    assert executed == []  # never reached
    assert result.sent == 0
    assert result.deferred == 2
    assert len(alerts) == 1
    assert alerts[0]["rule"] == "relay_suppression_sync_failed"


def test_sweep_syncs_unsubscribes_for_its_own_venture(monkeypatch):
    """CLONE-v2.2 / CL3: the sync must be scoped to the venture being swept,
    not a fleet-wide default — otherwise a second venture's unsubscribes
    would never be pulled in."""
    seen: list[str] = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: seen.append(venture_key) or SyncResult(status="synced"))
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    sweep.run_sweep(venture_key="venture_two")

    assert seen == ["venture_two"]


def test_sweep_refuses_to_execute_for_a_deactivated_venture(monkeypatch):
    """PR #195 review: a deactivated venture (is_active=false) must never
    reach execute_batch, regardless of what its resolved config looks like
    -- this is the independent second gate on top of venture_config.py
    resolving a disabled config for it."""
    monkeypatch.setattr(sweep, "sync_unsubscribes", _synced())
    monkeypatch.setattr(
        sweep.queue, "approved_batch",
        lambda limit=50, venture_key=None: [SimpleNamespace(id=1)],
    )
    monkeypatch.setattr(
        sweep, "get_venture_config",
        lambda key: SimpleNamespace(is_active=False, venture_key=key),
    )
    executed: list = []
    monkeypatch.setattr(sweep, "execute_batch", lambda *a, **k: executed.append((a, k)))

    result = sweep.run_sweep(venture_key="venture_deactivated")

    assert executed == []  # never reached
    assert result.halted is True
    assert result.sent == 0


def test_sweep_still_executes_for_an_active_venture(monkeypatch):
    """Sanity check for the guard above: an active venture's batch must
    still reach execute_batch unchanged."""
    monkeypatch.setattr(sweep, "sync_unsubscribes", _synced())
    monkeypatch.setattr(
        sweep.queue, "approved_batch",
        lambda limit=50, venture_key=None: [SimpleNamespace(id=1)],
    )
    monkeypatch.setattr(
        sweep, "get_venture_config",
        lambda key: SimpleNamespace(is_active=True, venture_key=key),
    )
    executed: list = []
    monkeypatch.setattr(
        sweep, "execute_batch",
        lambda items, *, batch_id, venture=None: executed.append(items) or sweep.BatchResult(sent=1),
    )

    result = sweep.run_sweep(venture_key="venture_active")

    assert len(executed) == 1
    assert result.sent == 1
