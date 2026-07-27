"""
Real-Postgres end-to-end tests for RELAY-v2.2 sub-task R4 — completion
receipts + the batch-intake contract's acceptance criterion: "a real batch
executes, produces receipts, and a forced retry proves idempotency held."

Every other Relay test (test_relay_engine.py, test_relay_queue.py) uses an
in-memory fake for the queue backend. That proves the engine's control flow
is correct but never exercises the actual SQL the no-double-send guarantee
lives in:

    UPDATE relay_approval_queue SET batch_id = :batch_id, ...
    WHERE id = :id AND status = :approved
      AND (batch_id IS NULL OR updated_at < now() - make_interval(...))

These tests run the full chain (enqueue -> approve -> execute) against real
Postgres via the existing `fresh_db` fixture (tests/conftest.py), which
rolls back after each test -- no manual cleanup, no rows left behind.
Auto-skips if DATABASE_URL isn't configured, same as test_ab_engine.py's
precedent for this fixture.
"""
from __future__ import annotations

import contextlib
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src.services.relay import engine, queue as relay_queue
from src.services.relay.channels import DISPATCHERS
from src.services.relay.queue import QueueItem


def _require(item: QueueItem | None) -> QueueItem:
    assert item is not None
    return item

# Fixed, in-send-window instant (14:00 ET) -- see test_relay_engine.py for
# why the R3 send-window guard makes this necessary for any execute_batch()
# call in a test.
_IN_WINDOW_NOW = datetime(2026, 7, 27, 14, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)


@pytest.fixture
def relay_db(fresh_db, monkeypatch):
    """Route every src.services.relay.queue call through the same
    rollback-per-test session, instead of each call opening its own via
    get_db_context(). Keeps the whole enqueue -> approve -> execute chain
    inside one transaction that auto-rolls-back -- no real rows survive
    the test."""
    @contextlib.contextmanager
    def _use_fresh_db():
        yield fresh_db

    monkeypatch.setattr(relay_queue, "get_db_context", _use_fresh_db)
    monkeypatch.setattr(engine, "get_kill_switch_status", lambda feature: {"color": "unknown"})
    return fresh_db


def test_seed_approve_sweep_produces_a_real_receipt(relay_db, monkeypatch):
    """The full chain against real Postgres: enqueue -> approve -> execute
    -> the row IS the completion receipt (dispatched_at/channel/thread_id/
    status), per the module docstring's documented contract."""
    monkeypatch.setitem(DISPATCHERS, "email", lambda item: None)  # no real Instantly call

    item = relay_queue.enqueue(
        idempotency_key=f"e2e-{uuid.uuid4().hex[:12]}",
        channel="email",
        recipient="prospect@example.com",
        payload={"subject": "Hi", "body": "Hello"},
        thread_id="OPP-2026-00042",
    )
    relay_queue.record_decision(item.id, approved=True, decided_by="U_TEST")

    result = engine.execute_batch(
        [_require(relay_queue.get_item(item.id))], batch_id="e2e-1", now=_IN_WINDOW_NOW
    )

    receipt = _require(relay_queue.get_item(item.id))
    assert result.sent == 1
    assert receipt.status == "sent"
    assert receipt.dispatched_at is not None
    assert receipt.channel == "email"
    assert receipt.thread_id == "OPP-2026-00042"


def test_forced_retry_does_not_double_send(relay_db, monkeypatch):
    """The core R4 acceptance criterion, against REAL Postgres: a second
    execution attempt on the same row (simulating a crashed/duplicate
    sweep re-picking it up by id) must not dispatch again. This is what
    proves the real UPDATE ... WHERE status = 'approved' guard holds --
    not a Python dict, as test_relay_engine.py's fake-backend test does."""
    calls = []
    monkeypatch.setitem(DISPATCHERS, "email", lambda item: calls.append(item.id))

    item = relay_queue.enqueue(
        idempotency_key=f"e2e-retry-{uuid.uuid4().hex[:12]}",
        channel="email",
        recipient="prospect@example.com",
        payload={"subject": "Hi", "body": "Hello"},
    )
    relay_queue.record_decision(item.id, approved=True, decided_by="U_TEST")

    first = engine.execute_batch(
        [_require(relay_queue.get_item(item.id))], batch_id="retry-1", now=_IN_WINDOW_NOW
    )
    # Forced retry: re-select and re-execute the SAME (now-'sent') row.
    second = engine.execute_batch(
        [_require(relay_queue.get_item(item.id))], batch_id="retry-2", now=_IN_WINDOW_NOW
    )

    assert first.sent == 1
    assert second.sent == 0
    assert calls == [item.id]  # dispatcher called exactly once, against a real DB claim

    receipt = _require(relay_queue.get_item(item.id))
    assert receipt.status == "sent"  # unchanged by the forced retry
