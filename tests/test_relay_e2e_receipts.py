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

from src.core import redis_client
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
    the test.

    Also forces the Redis sandbox on (PR #179 review finding #2:
    execute_batch() now reserves a real atomic daily-ceiling slot via
    guards.reserve_daily_slot() before every dispatch). Without this, these
    tests would fail closed against whatever Redis state this environment
    actually has (often no reachable Redis at all), deferring every item
    instead of exercising the receipt/idempotency behavior they're for.
    Uses the same settings.redis_sandbox -> fakeredis mechanism
    src/core/redis_client.py already ships, not a monkeypatch around the
    ceiling logic -- these tests still exercise the real reservation path,
    just against fakeredis instead of a real server."""
    @contextlib.contextmanager
    def _use_fresh_db():
        yield fresh_db

    monkeypatch.setattr(relay_queue, "get_db_context", _use_fresh_db)
    monkeypatch.setattr(engine, "get_kill_switch_status", lambda feature: {"color": "unknown"})
    monkeypatch.setattr(redis_client.settings, "redis_sandbox", True)
    redis_client.reset_client_cache()
    yield fresh_db
    redis_client.reset_client_cache()


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
        thread_id="OPP-2026-00077",
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


def test_concurrent_claim_never_corrupts_the_winners_row(relay_db):
    """PR #179 review finding #1, proven against real Postgres: a losing
    worker's lost claim must never write anything to a row another worker
    currently owns, and that owning worker's eventual mark_sent() must
    succeed and produce a correct, uncorrupted receipt.

    Simulates the actual race directly at the queue layer (rather than
    threading two real execute_batch() calls, which would be flaky to time
    precisely): claim the row as 'batch_A' first (worker A, mid-dispatch),
    then run worker B's losing-claim handling against the SAME row --
    before this fix, that used to call mark_skipped() and downgrade a
    still-'approved', A-owned row to 'skipped'. Then have worker A finish
    and confirm its receipt is exactly correct.
    """
    item = relay_queue.enqueue(
        idempotency_key=f"e2e-race-{uuid.uuid4().hex[:12]}",
        channel="email",
        recipient="prospect@example.com",
        payload={"subject": "Hi", "body": "Hello"},
        thread_id="OPP-2026-00099",
    )
    relay_queue.record_decision(item.id, approved=True, decided_by="U_TEST")

    # Worker A claims first -- still 'approved', now owned by batch_A.
    claimed_a = relay_queue.try_claim_for_batch(item.id, "batch_A")
    assert claimed_a is True

    # Worker B (an overlapping sweep) tries the same row and loses the race
    # -- this is the exact engine.py branch PR #179 flagged: per the fix,
    # it must do NOTHING to the row (no mark_skipped call at all).
    claimed_b = relay_queue.try_claim_for_batch(item.id, "batch_B")
    assert claimed_b is False

    mid_flight = _require(relay_queue.get_item(item.id))
    assert mid_flight.status == "approved"  # untouched by B's lost claim
    assert mid_flight.batch_id == "batch_A"  # still A's, not overwritten

    # Worker A's dispatch now completes -- mark_sent guarded on status AND
    # batch_id='batch_A' must succeed, since A is still the genuine owner.
    relay_queue.mark_sent(item.id, batch_id="batch_A")

    receipt = _require(relay_queue.get_item(item.id))
    assert receipt.status == "sent"
    assert receipt.dispatched_at is not None
    assert receipt.thread_id == "OPP-2026-00099"


def test_mark_sent_refuses_to_finalize_under_the_wrong_batch_id(relay_db):
    """The other half of finding #1's defense-in-depth: even if a row is
    genuinely still 'approved', mark_sent()/mark_failed() must refuse to
    finalize it under a batch_id that isn't the current owner -- proving
    the new guard clause is real, not just present in the SQL text."""
    item = relay_queue.enqueue(
        idempotency_key=f"e2e-wrongbatch-{uuid.uuid4().hex[:12]}",
        channel="email",
        recipient="prospect@example.com",
        payload={"subject": "Hi", "body": "Hello"},
        thread_id="OPP-2026-00088",
    )
    relay_queue.record_decision(item.id, approved=True, decided_by="U_TEST")
    relay_queue.try_claim_for_batch(item.id, "batch_real_owner")

    # A stale/wrong worker attempts to finalize the row under a DIFFERENT
    # batch_id than the one that actually claimed it.
    relay_queue.mark_sent(item.id, batch_id="batch_impostor")

    unchanged = _require(relay_queue.get_item(item.id))
    assert unchanged.status == "approved"  # the impostor's write did nothing
    assert unchanged.dispatched_at is None

    # The real owner's finalize still works correctly afterward.
    relay_queue.mark_sent(item.id, batch_id="batch_real_owner")
    receipt = _require(relay_queue.get_item(item.id))
    assert receipt.status == "sent"
