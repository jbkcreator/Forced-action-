"""
Tests for src.services.relay.sweep (RELAY-v2.2 sub-tasks R1 + R3).

R3 addition under test: run_sweep() calls sync_unsubscribes() before
execute_batch(), and a failure in that sync must never stop already-approved
items from being sent.

PR #195 review addition: run_sweep() must refuse to execute a batch for a
deactivated venture (ventures.is_active = false) rather than dispatch its
approved items under whatever config it resolves to.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services.relay import sweep


def test_sweep_returns_empty_result_with_no_approved_items(monkeypatch):
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: 0)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    result = sweep.run_sweep()

    assert result.sent == 0
    assert result.processed_ids == []


def test_sweep_calls_sync_before_execute(monkeypatch):
    order = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: order.append("sync") or 0)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: order.append("query") or [])

    sweep.run_sweep()

    assert order == ["sync", "query"]


def test_sync_failure_does_not_stop_the_batch(monkeypatch):
    """A dead Instantly API must not block already-approved sends."""
    def _raiser(venture_key=None):
        raise RuntimeError("Instantly API down")

    monkeypatch.setattr(sweep, "sync_unsubscribes", _raiser)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    result = sweep.run_sweep()  # must not raise

    assert result.sent == 0


def test_sweep_syncs_unsubscribes_for_its_own_venture(monkeypatch):
    """CLONE-v2.2 / CL3: the sync must be scoped to the venture being swept,
    not a fleet-wide default — otherwise a second venture's unsubscribes
    would never be pulled in."""
    seen: list[str] = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: seen.append(venture_key) or 0)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    sweep.run_sweep(venture_key="venture_two")

    assert seen == ["venture_two"]


def test_sweep_refuses_to_execute_for_a_deactivated_venture(monkeypatch):
    """PR #195 review: a deactivated venture (is_active=false) must never
    reach execute_batch, regardless of what its resolved config looks like
    -- this is the independent second gate on top of venture_config.py
    resolving a disabled config for it."""
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: 0)
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
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda venture_key=None: 0)
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
