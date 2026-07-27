"""
Tests for src.services.relay.guards (RELAY-v2.2 sub-task R3).

evaluate() is a pure function over an injected clock (`now`) and an injected
per-channel send count (`sent_today`) for the window/ceiling checks, so those
cases need no DB and no clock mocking. The suppression check is the only
DB-touching branch; it's exercised by monkeypatching
`is_email_suppressed` directly (the same no-real-DB convention used in
test_relay_queue.py) rather than mocking get_db_context.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={})
    assert verdict.outcome == guards.ALLOW


def test_before_window_defers():
    item = _make_item()
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 9), sent_today={})
    assert verdict.outcome == guards.DEFER
    assert verdict.reason == "outside_send_window"


def test_after_window_defers():
    item = _make_item()
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 19), sent_today={})
    assert verdict.outcome == guards.DEFER
    assert verdict.reason == "outside_send_window"


def test_window_respects_dst(monkeypatch):
    """Same wall-clock hour (14:00 ET) must resolve to ALLOW in both EST
    (January) and EDT (July) — ZoneInfo handles the offset, not us."""
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: False)
    item = _make_item()
    for now in (_et(2026, 1, 15, 14), _et(2026, 7, 15, 14)):
        verdict = guards.evaluate(item, now=now, sent_today={})
        assert verdict.outcome == guards.ALLOW


def test_ceiling_reached_defers(monkeypatch):
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: False)
    item = _make_item(channel="email")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={"email": 20})
    assert verdict.outcome == guards.DEFER
    assert verdict.reason == "daily_ceiling_reached:email"


def test_ceiling_is_per_channel(monkeypatch):
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: False)
    item = _make_item(channel="sms")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={"email": 20})
    # sms isn't at its ceiling; falls through to the suppression check, which
    # for sms goes through validate_outbound (not mocked here) -- assert we
    # at least got past the ceiling guard, not blocked by it.
    assert verdict.reason != "daily_ceiling_reached:sms"


def test_suppressed_email_blocks(monkeypatch):
    monkeypatch.setattr(guards, "is_email_suppressed", lambda db, email: True)
    item = _make_item(channel="email")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={})
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
    guards.evaluate(item, now=_et(2026, 7, 27, 9), sent_today={})
    assert calls == []


def test_ceiling_checked_before_suppression(monkeypatch):
    """A ceiling-blocked item must also never touch is_email_suppressed."""
    calls = []
    monkeypatch.setattr(
        guards, "is_email_suppressed",
        lambda db, email: calls.append(1) or False,
    )
    item = _make_item(channel="email")
    guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={"email": 20})
    assert calls == []


def test_noop_channel_allows_without_suppression_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        guards, "is_email_suppressed",
        lambda db, email: calls.append(1) or False,
    )
    item = _make_item(channel="noop")
    verdict = guards.evaluate(item, now=_et(2026, 7, 27, 14), sent_today={})
    assert verdict.outcome == guards.ALLOW
    assert calls == []  # 'noop' branch in _suppression_reason never calls is_email_suppressed
