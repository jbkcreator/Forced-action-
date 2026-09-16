"""
Tests for the Relay execution engine (RELAY-v2.2 sub-task R1; reworked for
PR #179 review findings #1 and #2).

Uses a lightweight in-memory fake for the queue's claim/mark calls and the
channel dispatch registry — no real DB or Redis needed. Exercises the
engine's core safety guarantees directly: idempotency (double-pickup
guard, and that a lost claim NEVER writes to the row -- finding #1),
kill-switch halt (build spec §9.1 "halts within one cycle"), the
reserve/release ceiling handshake (finding #2, engine-level wiring only --
see test_relay_guards.py for reserve_daily_slot's own unit coverage and
test_relay_concurrency.py for the real-Redis concurrent proof), and
per-item failure isolation.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src.services.relay import engine as relay_engine
from src.services.relay.queue import QueueItem

# Fixed, in-send-window instant (14:00 ET) so these pre-R3 tests never
# depend on what time of day the suite happens to run (RELAY-v2.2 R3
# introduced a send-window guard; see test_relay_guards.py for its own
# coverage).
_IN_WINDOW_NOW = datetime(2026, 7, 27, 14, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)


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
        self.sent_batch_ids: dict[int, str] = {}
        self.failed: dict[int, str] = {}
        self.failed_batch_ids: dict[int, str] = {}
        self.skipped: dict[int, str] = {}

    def try_claim_for_batch(self, item_id: int, batch_id: str) -> bool:
        if item_id in self.claimed:
            return False
        self.claimed.add(item_id)
        return True

    def mark_sent(self, item_id: int, *, batch_id: str) -> None:
        self.sent.append(item_id)
        self.sent_batch_ids[item_id] = batch_id

    def mark_failed(self, item_id: int, error: str, *, batch_id: str) -> None:
        self.failed[item_id] = error
        self.failed_batch_ids[item_id] = batch_id

    def mark_skipped(self, item_id: int, reason: str) -> bool:
        self.skipped[item_id] = reason
        return True


@pytest.fixture
def fake_backend(monkeypatch):
    backend = _FakeQueueBackend()
    monkeypatch.setattr(relay_engine.queue, "try_claim_for_batch", backend.try_claim_for_batch)
    monkeypatch.setattr(relay_engine.queue, "mark_sent", backend.mark_sent)
    monkeypatch.setattr(relay_engine.queue, "mark_failed", backend.mark_failed)
    monkeypatch.setattr(relay_engine.queue, "mark_skipped", backend.mark_skipped)
    return backend


@pytest.fixture
def unlimited_ceiling(monkeypatch):
    """RELAY-v2.2 PR #179 fix: the ceiling is now an atomic Redis reservation
    (guards.reserve_daily_slot/release_daily_slot), not a local dict. Tests
    unrelated to the ceiling itself stub it to always allow, recording
    release() calls so the claim-lost/dispatch-failure refund path can be
    asserted where relevant."""
    released = []
    monkeypatch.setattr(relay_engine.guards, "reserve_daily_slot", lambda channel, now, settings: True)
    monkeypatch.setattr(
        relay_engine.guards, "release_daily_slot",
        lambda channel, now, settings: released.append(channel),
    )
    return released


@pytest.fixture
def green_kill_switch(monkeypatch):
    monkeypatch.setattr(
        relay_engine, "get_kill_switch_status", lambda feature: {"color": "unknown"}
    )


def test_batch_dispatches_all_items(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    items = [_make_item(1), _make_item(2)]

    result = relay_engine.execute_batch(items, batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.sent == 2
    assert result.failed == 0
    assert result.skipped == 0
    assert not result.halted
    assert calls == [1, 2]
    assert fake_backend.sent == [1, 2]
    assert fake_backend.sent_batch_ids == {1: "b1", 2: "b1"}


def test_idempotency_skips_already_claimed_item(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    """The core money/safety assertion: a row already claimed by a prior
    (or overlapping) run is never dispatched twice -- AND (PR #179 finding
    #1) the losing run never writes anything to the row. It's counted as
    'skipped' in this run's in-memory BatchResult for observability, but
    fake_backend.skipped (the DB write) must stay empty."""
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    item = _make_item(1)

    first = relay_engine.execute_batch([item], batch_id="b1", now=_IN_WINDOW_NOW)
    second = relay_engine.execute_batch([item], batch_id="b2", now=_IN_WINDOW_NOW)  # simulates a re-pickup

    assert first.sent == 1
    assert second.sent == 0
    assert second.skipped == 1
    assert calls == [1]  # dispatcher called exactly once, never twice
    assert fake_backend.skipped == {}  # finding #1: the losing run writes NOTHING to the row


def test_lost_claim_releases_its_ceiling_reservation(fake_backend, green_kill_switch, monkeypatch):
    """PR #179 finding #2: a reservation taken before a lost claim must be
    refunded, or it would permanently (and wrongly) eat into today's cap
    for a row this run never actually sent."""
    released = []
    monkeypatch.setattr(relay_engine.guards, "reserve_daily_slot", lambda channel, now, settings: True)
    monkeypatch.setattr(
        relay_engine.guards, "release_daily_slot",
        lambda channel, now, settings: released.append(channel),
    )
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: None)
    item = _make_item(1)

    relay_engine.execute_batch([item], batch_id="b1", now=_IN_WINDOW_NOW)
    relay_engine.execute_batch([item], batch_id="b2", now=_IN_WINDOW_NOW)  # loses the claim

    assert released == ["fake"]  # exactly one release: the losing run's reservation


def test_kill_switch_halts_before_batch(fake_backend, unlimited_ceiling, monkeypatch):
    monkeypatch.setattr(relay_engine, "get_kill_switch_status", lambda feature: {"color": "red"})
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1")

    assert result.halted is True
    assert result.sent == 0
    assert calls == []
    assert fake_backend.claimed == set()  # never even attempted to claim


def test_kill_switch_halts_mid_batch(fake_backend, unlimited_ceiling, monkeypatch):
    """Per-item re-check (not just a batch preflight) is what delivers the
    spec's 'halts within one cycle' — item 2 must never dispatch once red
    appears between item 1 and item 2."""
    colors = iter(["unknown", "unknown", "red"])  # preflight, item1-check, item2-check
    monkeypatch.setattr(
        relay_engine, "get_kill_switch_status", lambda feature: {"color": next(colors)}
    )
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))

    result = relay_engine.execute_batch(
        [_make_item(1), _make_item(2)], batch_id="b1", now=_IN_WINDOW_NOW
    )

    assert result.sent == 1
    assert result.halted is True
    assert calls == [1]  # item 2 never dispatched
    assert 2 not in fake_backend.claimed


def test_failure_isolation_one_bad_item_does_not_abort_batch(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    def _raiser(item):
        raise RuntimeError("boom")

    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", _raiser)
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake_ok", lambda item: None)

    items = [_make_item(1, channel="fake"), _make_item(2, channel="fake_ok")]
    result = relay_engine.execute_batch(items, batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.failed == 1
    assert result.sent == 1
    assert fake_backend.failed[1] == "boom"
    assert fake_backend.failed_batch_ids[1] == "b1"
    assert fake_backend.sent == [2]


def test_failed_dispatch_releases_its_ceiling_reservation(fake_backend, green_kill_switch, monkeypatch):
    """PR #179 finding #2: a reservation for an item whose dispatch raised
    must be refunded -- a transient send failure shouldn't permanently
    shrink today's cap."""
    released = []
    monkeypatch.setattr(relay_engine.guards, "reserve_daily_slot", lambda channel, now, settings: True)
    monkeypatch.setattr(
        relay_engine.guards, "release_daily_slot",
        lambda channel, now, settings: released.append(channel),
    )
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: (_ for _ in ()).throw(RuntimeError("boom")))

    relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert released == ["fake"]


def test_unknown_channel_marks_failed_without_dispatch(fake_backend, unlimited_ceiling, green_kill_switch):
    item = _make_item(1, channel="does_not_exist")

    result = relay_engine.execute_batch([item], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.failed == 1
    assert fake_backend.failed[1] == "unknown_channel:does_not_exist"


# ---------------------------------------------------------------------------
# RELAY-v2.2 sub-task R3 — guard wiring
# ---------------------------------------------------------------------------

def test_deferred_item_never_claimed_or_dispatched(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    """DEFER must leave the row completely untouched -- never claimed,
    never dispatched -- so the next sweep treats it as brand new."""
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    monkeypatch.setattr(
        relay_engine.guards, "evaluate",
        lambda item, **kw: relay_engine.guards.Verdict(relay_engine.guards.DEFER, "outside_send_window"),
    )

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.deferred == 1
    assert result.sent == 0
    assert calls == []
    assert fake_backend.claimed == set()
    assert 1 not in fake_backend.skipped


def test_blocked_item_marked_skipped_never_claimed(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    """BLOCK is terminal -- marked skipped with the guard's reason, never
    claimed, never dispatched, never retried."""
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))
    monkeypatch.setattr(
        relay_engine.guards, "evaluate",
        lambda item, **kw: relay_engine.guards.Verdict(relay_engine.guards.BLOCK, "suppressed:email_opt_out"),
    )

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.skipped == 1
    assert calls == []
    assert fake_backend.claimed == set()
    assert fake_backend.skipped[1] == "suppressed:email_opt_out"


def test_fa_max_block_is_surfaced_to_its_slack_lane(fake_backend, unlimited_ceiling, green_kill_switch, monkeypatch):
    """A send-layer refusal is durable first, then visible to the operator."""
    item = replace(_make_item(1), venture_key="fa_max_lending", lane="EXCEPTIONS")
    post = []
    monkeypatch.setattr(
        relay_engine.guards, "evaluate",
        lambda item, **kw: relay_engine.guards.Verdict(relay_engine.guards.BLOCK, "suppressed:consent_withdrawn"),
    )
    monkeypatch.setattr(
        "src.services.relay.slack_post.post_blocked_action",
        lambda queue_item, reason: post.append((queue_item.id, reason)),
    )

    result = relay_engine.execute_batch([item], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.skipped == 1
    assert fake_backend.skipped[1] == "suppressed:consent_withdrawn"
    assert post == [(1, "suppressed:consent_withdrawn")]


# ---------------------------------------------------------------------------
# RELAY-v2.2 PR #179 review finding #2 — engine-level reserve/release wiring
# ---------------------------------------------------------------------------

def test_reservation_denied_defers_without_claiming(fake_backend, green_kill_switch, monkeypatch):
    """When reserve_daily_slot() denies (ceiling reached, or Redis down),
    the item must defer -- never claimed, never dispatched -- exactly like
    a window-DEFER, and the ceiling denial must be checked BEFORE any
    claim is attempted (a claim would otherwise need to be undone)."""
    monkeypatch.setattr(relay_engine.guards, "reserve_daily_slot", lambda channel, now, settings: False)
    calls = []
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: calls.append(item.id))

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.deferred == 1
    assert result.sent == 0
    assert calls == []
    assert fake_backend.claimed == set()  # never even attempted to claim


def test_reservation_taken_before_claim_attempt(fake_backend, green_kill_switch, monkeypatch):
    """Ordering assertion: reserve happens before try_claim_for_batch, per
    the fix design (a lost claim only ever needs a refund, never an
    un-claim, because nothing claim-side has happened yet when we reserve)."""
    order = []
    monkeypatch.setattr(
        relay_engine.guards, "reserve_daily_slot",
        lambda channel, now, settings: order.append("reserve") or True,
    )
    real_claim = fake_backend.try_claim_for_batch

    def _tracking_claim(item_id, batch_id):
        order.append("claim")
        return real_claim(item_id, batch_id)

    monkeypatch.setattr(relay_engine.queue, "try_claim_for_batch", _tracking_claim)
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: None)

    relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert order == ["reserve", "claim"]


def test_successful_send_does_not_release_its_reservation(fake_backend, green_kill_switch, monkeypatch):
    """A reservation that was genuinely used (the send succeeded) must NOT
    be refunded -- only lost claims and failed dispatches release."""
    released = []
    monkeypatch.setattr(relay_engine.guards, "reserve_daily_slot", lambda channel, now, settings: True)
    monkeypatch.setattr(
        relay_engine.guards, "release_daily_slot",
        lambda channel, now, settings: released.append(channel),
    )
    monkeypatch.setitem(relay_engine.DISPATCHERS, "fake", lambda item: None)

    result = relay_engine.execute_batch([_make_item(1)], batch_id="b1", now=_IN_WINDOW_NOW)

    assert result.sent == 1
    assert released == []
