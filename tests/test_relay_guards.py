"""
Tests for src.services.relay.guards (RELAY-v2.2 sub-task R3; ceiling
reworked for the PR #179 review fix).

evaluate() is a pure function over an injected clock (`now`) for the window
check, so those cases need no DB and no clock mocking. The suppression check
is the only DB-touching branch in evaluate(); it's exercised by
monkeypatching `is_email_suppressed` directly (the same no-real-DB
convention used in test_relay_queue.py) rather than mocking get_db_context.

reserve_daily_slot()/release_daily_slot() are exercised here by
monkeypatching `rincr`/`rdecr` directly (unit-level, no real Redis needed) --
the real-Redis concurrent-reservation proof lives in
test_relay_concurrency.py, since that's what actually demonstrates the fix
(two truly concurrent callers can't both get past the ceiling).
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import guards
from src.services.relay.queue import QueueItem

_ET = ZoneInfo("America/New_York")


def _et(year, month, day, hour, minute=0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=_ET).astimezone(timezone.utc)


def _make_item(channel: str = "email", recipient: str = "prospect@example.com") -> QueueItem:
    return QueueItem(
        id=1,
        idempotency_key="key-1",
        batch_id=None,
        thread_id=None,
        channel=channel,
        recipient=recipient,
        payload={"subject": "Hi", "body": "Hello"},
        status="approved",
        slack_message_ts=None,
        decided_by="U_TEST",
        decided_at=None,
        error=None,
        dispatched_at=None,
        created_at=datetime.now(timezone.utc),
    )


def test_inside_window_allows(monkeypatch):
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: False)
    item = _make_item()
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14))
    assert verdict.outcome == guards.ALLOW


def test_before_window_defers():
    item = _make_item()
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 9))
    assert verdict.outcome == guards.DEFER
    assert verdict.reason == "outside_send_window"


def test_after_window_defers():
    item = _make_item()
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 19))
    assert verdict.outcome == guards.DEFER
    assert verdict.reason == "outside_send_window"


def test_window_respects_dst(monkeypatch):
    """Same wall-clock hour (14:00 ET) must resolve to ALLOW in both EST
    (January) and EDT (July) — ZoneInfo handles the offset, not us."""
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: False)
    item = _make_item()
    for now in (_et(2026, 1, 15, 14), _et(2026, 7, 15, 14)):
        verdict = guards.evaluate(item, now=now)
        assert verdict.outcome == guards.ALLOW


def test_suppressed_email_blocks(monkeypatch):
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: True)
    item = _make_item(channel="email")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14))
    assert verdict.outcome == guards.BLOCK
    assert verdict.reason == "suppressed:email_opt_out"


def test_window_checked_before_db(monkeypatch):
    """Outside the window, evaluate() must return before ever touching
    is_email_suppressed (or the DB it needs) -- cost discipline for a
    deferred batch."""
    calls = []
    monkeypatch.setattr(
        guards, "is_email_suppressed",
        lambda db, email: calls.append(1) or False,
    )
    item = _make_item()
    guards.evaluate(item, now=_et(2026, 7, 27, 9))
    assert calls == []


def test_noop_channel_allows_without_suppression_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        guards, "is_email_suppressed",
        lambda db, email: calls.append(1) or False,
    )
    item = _make_item(channel="noop")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14))
    assert verdict.outcome == guards.ALLOW
    assert calls == []  # 'noop' branch in _suppression_reason never calls is_email_suppressed


# ---------------------------------------------------------------------------
# RELAY-v2.2 PR #179 review fix — atomic daily-ceiling reservation
# ---------------------------------------------------------------------------

def _fake_settings(ceiling: int = 20, venture_key: str = DEFAULT_VENTURE_KEY):
    # venture_key must be set explicitly (CLONE-v2.2 / CL3): _daily_slot_key
    # reads it with a getattr fallback, and a MagicMock answers every getattr
    # with another Mock, which would silently produce a garbage Redis key
    # rather than fall back to the default.
    return MagicMock(
        relay_send_window_timezone="America/New_York",
        relay_daily_ceiling=ceiling,
        venture_key=venture_key,
    )


def test_reserve_allows_under_ceiling(monkeypatch):
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: 5)
    assert guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20)) is True


def test_reserve_allows_the_exact_ceiling_count(monkeypatch):
    """The Nth reservation when ceiling=N is the last one PERMITTED (matches
    the original >= semantics: up to and including the ceiling count is
    allowed; only the (N+1)th is denied)."""
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: 20)
    assert guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20)) is True


def test_reserve_denies_over_ceiling(monkeypatch):
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: 21)
    assert guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20)) is False


def test_reserve_fails_closed_when_redis_unavailable(monkeypatch):
    """rincr() returns 0 ONLY on a Redis failure (a real INCR can never
    return 0) -- reserve_daily_slot() must treat that as "no slot", not
    "plenty of room", since deferring is always safe and silently
    exceeding the ceiling is not."""
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: 0)
    assert guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20)) is False


def test_reserve_key_is_scoped_by_venture_channel_and_local_date(monkeypatch):
    seen_keys = []
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: seen_keys.append(key) or 1)
    guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20))
    guards.reserve_daily_slot("sms", _et(2026, 7, 27, 14), _fake_settings(20))
    assert seen_keys == [
        f"relay_daily_sent:{DEFAULT_VENTURE_KEY}:email:2026-07-27",
        f"relay_daily_sent:{DEFAULT_VENTURE_KEY}:sms:2026-07-27",
    ]


def test_reserve_key_separates_two_ventures(monkeypatch):
    """CLONE-v2.2 / CL3: the ceiling is a per-sender reputation limit, so two
    ventures must not share one counter."""
    seen_keys = []
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: seen_keys.append(key) or 1)
    guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20, "venture_one"))
    guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20, "venture_two"))
    assert seen_keys == [
        "relay_daily_sent:venture_one:email:2026-07-27",
        "relay_daily_sent:venture_two:email:2026-07-27",
    ]


def test_reserve_key_falls_back_to_venture_one_without_a_venture_key(monkeypatch):
    """A plain settings object has no venture_key and belongs to venture #1 by
    definition — any pre-CL3 caller keeps counting against the same key."""
    seen_keys = []
    monkeypatch.setattr(guards, "rincr", lambda key, ttl_seconds=None: seen_keys.append(key) or 1)

    class _SettingsWithoutVenture:
        relay_send_window_timezone = "America/New_York"
        relay_daily_ceiling = 20

    guards.reserve_daily_slot("email", _et(2026, 7, 27, 14), _SettingsWithoutVenture())
    assert seen_keys == [f"relay_daily_sent:{DEFAULT_VENTURE_KEY}:email:2026-07-27"]


def test_release_decrements_the_same_key(monkeypatch):
    seen = {}
    monkeypatch.setattr(guards, "rdecr", lambda key: seen.setdefault("key", key))
    guards.release_daily_slot("email", _et(2026, 7, 27, 14), _fake_settings(20))
    assert seen["key"] == f"relay_daily_sent:{DEFAULT_VENTURE_KEY}:email:2026-07-27"
