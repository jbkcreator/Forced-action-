"""
Tests for src.services.relay.sweep (RELAY-v2.2 sub-tasks R1 + R3).

R3 addition under test: run_sweep() calls sync_unsubscribes() before
execute_batch(), and a failure in that sync must never stop already-approved
items from being sent.
"""
from __future__ import annotations

from src.services.relay import sweep


def test_sweep_returns_empty_result_with_no_approved_items(monkeypatch):
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda: 0)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    result = sweep.run_sweep()

    assert result.sent == 0
    assert result.processed_ids == []


def test_sweep_calls_sync_before_execute(monkeypatch):
    order = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", lambda: order.append("sync") or 0)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: order.append("query") or [])

    sweep.run_sweep()

    assert order == ["sync", "query"]


def test_sync_failure_does_not_stop_the_batch(monkeypatch):
    """A dead Instantly API must not block already-approved sends."""
    def _raiser():
        raise RuntimeError("Instantly API down")

    monkeypatch.setattr(sweep, "sync_unsubscribes", _raiser)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda limit=50, venture_key=None: [])

    result = sweep.run_sweep()  # must not raise

    assert result.sent == 0
