"""
Tests for the Relay execution engine (RELAY-v2.2 sub-task R1).

Uses a lightweight in-memory fake for the queue's claim/mark calls and the
channel dispatch registry — no real DB or Slack needed. Exercises the
engine's core safety guarantees directly: idempotency (double-pickup
guard), kill-switch halt (build spec §9.1 "halts within one cycle"), and
per-item failure isolation.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.services.relay import engine as relay_engine
from src.services.relay.queue import QueueItem


def _make_item(item_id: int, channel: str = "fake") -> QueueItem:
    return QueueItem(
        id=item_id,
        idempotency_key=f"key-{item_id}",
        batch_id=None,
        thread_id=None,
        channel=channel,
        recipient="test@example.com",
        payload={"subject": "hi"},
        status="approved",
        slack_message_ts=None,
        decided_by="U_TEST",
        decided_at=None,
        error=None,
        dispatched_at=None,
        created_at=datetime.now(timezone.utc),
    )


class _FakeQueueBackend:
    """In-memory stand-in for src.services.relay.queue's claim/mark calls."""

    def __init__(self):
        self.claimed: set[int] = set()
        self.sent: list[int] = []
        self.failed: dict[int, str] = {}
        self.skipped: dict[int, str] = {}

    def try_claim_for_batch(self, item_id: int, batch_id: str) -> bool:
        if item_id in self.claimed:
            return False
        self.claimed.add(item_id)
        return True

    def mark_sent(self, item_id: int) -> None:
        self.sent.append(item_id)

    def mark_failed(self, item_id: int, error: str) -> None:
        self.failed[item_id] = error

    def mark_skipped(self, item_id: int, reason: str) -> None:
        self.skipped[item_id] = reason


@pytest.fixture
def fake_backend(monkeypatch):
    backend = _FakeQueueBackend()
    monkeypatch.setattr(relay_engine.queue, "try_claim_for_batch", backend.try_claim_for_batch)
    monkeypatch.setattr(relay_engine.queue, "mark_sent", backend.mark_sent)
    monkeypatch.setattr(relay_engine.queue, "mark_failed", backend.mark_failed)
    monkeypatch.setattr(relay_engine.queue, "mark_skipped", backend.mark_skipped)
    return backend


@pytest.fixture
def green_kill_switch(monkeypatch):
    monkeypatch.setattr(
        relay_engine, "get_kill_switch_status", lambda feature: {"color": "unknown"}
    )


def test_batch_dispatches_all_items(fake_backend, green_kill_switch, monkeypatch):
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    items = [_make_item(1), _make_item(2)]

    result = relay_engine.execute_batch(items, batch_id="b1")

    assert result.sent == 2
    assert result.failed == 0
    assert result.skipped == 0
    assert not result.halted
    assert calls == [1, 2]
    assert fake_backend.sent == [1, 2]


def test_idempotency_skips_already_claimed_item(fake_backend, green_kill_switch, monkeypatch):
    """The core money/safety assertion: a row already claimed by a prior
    (or overlapping) run is never dispatched twice."""
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    item = _make_item(1)

    first = relay_engine.execute_batch([item], batch_id="b1")
    second = relay_engine.execute_batch([item], batch_id="b2")  # simulates a re-pickup

    assert first.sent == 1
    assert second.sent == 0
    assert second.skipped == 1
    assert calls == [1]  # dispatcher called exactly once, never twice


def test_kill_switch_halts_before_batch(fake_backend, monkeypatch):
    monkeypatch.setattr(relay_engine, "get_kill_switch_status", lambda feature: {"color": "red"})
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1")

    assert result.halted is True
    assert result.sent == 0
    assert calls == []
    assert fake_backend.claimed == set()  # never even attempted to claim


def test_kill_switch_halts_mid_batch(fake_backend, monkeypatch):
    """Per-item re-check (not just a batch preflight) is what delivers the
    spec's 'halts within one cycle' — item 2 must never dispatch once red
    appears between item 1 and item 2."""
    colors = iter(["unknown", "unknown", "red"])  # preflight, item1-check, item2-check
    monkeypatch.setattr(
        relay_engine, "get_kill_switch_status", lambda feature: {"color": next(colors)}
    )
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))

    result = relay_engine.execute_batch([_make_item(1), _make_item(2)], batch_id="b1")

    assert result.sent == 1
    assert result.halted is True
    assert calls == [1]  # item 2 never dispatched
    assert 2 not in fake_backend.claimed


def test_failure_isolation_one_bad_item_does_not_abort_batch(fake_backend, green_kill_switch, monkeypatch):
    def _raiser(item):
        raise RuntimeError("boom")

    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", _raiser)
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake_ok", lambda item: None)

    items = [_make_item(1, channel="fake"), _make_item(2, channel="fake_ok")]
    result = relay_engine.execute_batch(items, batch_id="b1")

    assert result.failed == 1
    assert result.sent == 1
    assert fake_backend.failed[1] == "boom"
    assert fake_backend.sent == [2]


def test_unknown_channel_marks_failed_without_dispatch(fake_backend, green_kill_switch):
    item = _make_item(1, channel="does_not_exist")

    result = relay_engine.execute_batch([item], batch_id="b1")

    assert result.failed == 1
    assert fake_backend.failed[1] == "unknown_channel:does_not_exist"
