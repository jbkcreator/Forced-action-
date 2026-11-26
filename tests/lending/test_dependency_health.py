"""R1 0.6 fail-closed pause, no database: the freshness rule, the per-channel scope, fail closed on unreadable reads
and the off switch (alert-only). The breaker hook is in test_channel_breaker_suppression_pause.py."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from config import lending_fallbacks as fb
from src.lending import dependency_health as dh

NOW = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)


def _fresh_rows(**ages_min):
    """Every timed feed fresh (1 min old) unless overridden: feed=minutes (None = never succeeded)."""
    rows = {}
    for feed in fb.OPT_OUT_FEEDS:
        minutes = ages_min.get(feed.key, 1)
        rows[feed.key] = dh.HealthRow(feed.key, None if minutes is None else NOW - timedelta(minutes=minutes))
    return rows


# ── Policy table ──────────────────────────────────────────────────────────────

def test_freshness_limit_is_three_missed_runs_with_a_five_minute_floor():
    by_key = fb.FEEDS_BY_KEY
    assert by_key[fb.FEED_FA_OPT_OUT_POLLER].stale_after_s == 5 * 60       # 15 s runs -> floor
    assert by_key[fb.FEED_IMAP_OPT_OUT].stale_after_s == 15 * 60           # 5 min runs
    assert by_key[fb.FEED_GHL_DND_BACKSTOP].stale_after_s == 45 * 60       # 15 min runs


def test_the_instantly_reply_webhook_is_a_push_feed_with_no_timer():
    feed = fb.FEEDS_BY_KEY[fb.FEED_INSTANTLY_REPLIES]
    assert feed.kind == fb.FEED_PUSH and feed.stale_after_s is None and not feed.timed


def test_the_email_inbox_protects_email_channels_only():
    assert fb.FEEDS_BY_KEY[fb.FEED_IMAP_OPT_OUT].protects == (fb.CHANNEL_GHL_EMAIL, fb.CHANNEL_INSTANTLY)
    sms_feeds = {f.key for f in fb.feeds_protecting(fb.CHANNEL_GHL_SMS)}
    assert sms_feeds == {fb.FEED_GHL_DND_BACKSTOP, fb.FEED_FA_OPT_OUT_POLLER}


def test_every_policy_row_is_complete():
    assert {d.key for d in fb.DEPENDENCIES} == {"ghl", "batchdialer", "clay", "propertyradar", "sunbiz",
                                                 "lendingflow", "tracerfy", "telnyx", "instantly"}
    for dep in fb.DEPENDENCIES:
        assert dep.fallback and dep.restart
        assert dep.timeout_s is not None or dep.key == "lendingflow"
    assert not fb.BY_KEY["clay"].enforced
    for feed in fb.OPT_OUT_FEEDS:
        assert feed.kind in (fb.FEED_PULL, fb.FEED_PUSH) and all(c in fb.ALL_CHANNELS for c in feed.protects)


def test_timeouts_keep_the_values_the_clients_used_before():
    assert (fb.timeout("ghl"), fb.quick_timeout("ghl"), fb.timeout("instantly"), fb.timeout("propertyradar"),
            fb.timeout("telnyx"), fb.timeout("batchdialer"), fb.quick_timeout("batchdialer"),
            fb.timeout("sunbiz"), fb.timeout("tracerfy")) == (15, 4, 20, 90, 10, 20, 5, 60, 30)
    with pytest.raises(KeyError):
        fb.timeout("lendingflow")


# ── Pure rule ─────────────────────────────────────────────────────────────────

def test_fresh_feeds_allow_every_channel():
    for channel in fb.ALL_CHANNELS:
        assert dh.unverified_reason(channel, _fresh_rows(), NOW, suppression_readable=True) is None


def test_a_stale_ghl_sync_pauses_every_channel():
    rows = _fresh_rows(**{fb.FEED_GHL_DND_BACKSTOP: 46})
    for channel in fb.ALL_CHANNELS:
        reason = dh.unverified_reason(channel, rows, NOW, suppression_readable=True)
        assert reason and "GHL opt-out sync" in reason and "46 min" in reason


def test_one_slow_ghl_run_does_not_pause():
    assert dh.unverified_reason(fb.CHANNEL_GHL_SMS, _fresh_rows(**{fb.FEED_GHL_DND_BACKSTOP: 44}), NOW,
                                suppression_readable=True) is None


def test_a_stale_email_inbox_pauses_email_only():
    rows = _fresh_rows(**{fb.FEED_IMAP_OPT_OUT: 16})
    assert dh.unverified_reason(fb.CHANNEL_GHL_EMAIL, rows, NOW, suppression_readable=True)
    assert dh.unverified_reason(fb.CHANNEL_INSTANTLY, rows, NOW, suppression_readable=True)
    for channel in (fb.CHANNEL_GHL_SMS, fb.CHANNEL_GHL_CRM, fb.CHANNEL_DIALER):
        assert dh.unverified_reason(channel, rows, NOW, suppression_readable=True) is None


def test_a_feed_that_never_succeeded_counts_as_stale():
    reason = dh.unverified_reason(fb.CHANNEL_DIALER, _fresh_rows(**{fb.FEED_FA_OPT_OUT_POLLER: None}), NOW,
                                  suppression_readable=True)
    assert reason and "no success recorded" in reason


def test_unreadable_suppression_or_health_fails_closed():
    assert "suppression list" in dh.unverified_reason(fb.CHANNEL_GHL_SMS, _fresh_rows(), NOW,
                                                      suppression_readable=False)
    assert "health table" in dh.unverified_reason(fb.CHANNEL_GHL_SMS, None, NOW, suppression_readable=True)


def test_an_unknown_channel_is_a_programming_error():
    with pytest.raises(ValueError):
        dh.unverified_reason("fax", {}, NOW, suppression_readable=True)


def test_off_switch_allows_but_reports_would_pause():
    decision = dh.decide(fb.CHANNEL_GHL_SMS, "stale", enforce=False)
    assert decision.allowed and decision.would_pause
    decision = dh.decide(fb.CHANNEL_GHL_SMS, "stale", enforce=True)
    assert not decision.allowed and decision.reason == "stale"
    assert dh.decide(fb.CHANNEL_GHL_SMS, None, enforce=True).allowed


# ── Reads through a fake session ──────────────────────────────────────────────

class _Db:
    def __init__(self, rows=(), *, fail_suppression=False, fail_health=False):
        self.rows, self.fail_suppression, self.fail_health = list(rows), fail_suppression, fail_health
        self.writes: list[tuple] = []

    def execute(self, stmt, params=None):
        if stmt is dh._SUPPRESSION_PROBE:
            if self.fail_suppression:
                raise RuntimeError("relation missing")
            return SimpleNamespace(first=lambda: (1,))
        if stmt is dh._READ:
            if self.fail_health:
                raise RuntimeError("relation missing")
            return iter([r for r in self.rows if r.dependency in params["deps"]])
        self.writes.append((stmt, params))
        return None


def _use(monkeypatch, db, *, enabled=True):
    @contextmanager
    def session():
        yield db

    monkeypatch.setattr(dh, "_session_factory", session)
    monkeypatch.setattr(dh, "pause_enabled", lambda: enabled)


def _rows_list(**ages):
    return [SimpleNamespace(dependency=r.dependency, last_success_at=r.last_success_at, failing_since=None,
                            detail=None) for r in _fresh_rows(**ages).values()]


def test_outbound_allowed_reads_nothing_while_the_switch_is_off(monkeypatch):
    def boom():
        raise AssertionError("no read while off")

    monkeypatch.setattr(dh, "_session_factory", boom)
    monkeypatch.setattr(dh, "pause_enabled", lambda: False)
    assert dh.outbound_allowed(fb.CHANNEL_GHL_SMS).allowed


def test_outbound_allowed_pauses_on_a_stale_feed(monkeypatch):
    _use(monkeypatch, _Db(_rows_list(**{fb.FEED_GHL_DND_BACKSTOP: 120})))
    decision = dh.outbound_allowed(fb.CHANNEL_GHL_SMS, now=NOW)
    assert not decision.allowed and "GHL opt-out sync" in decision.reason


def test_outbound_allowed_with_fresh_feeds(monkeypatch):
    _use(monkeypatch, _Db(_rows_list()))
    assert dh.outbound_allowed(fb.CHANNEL_DIALER, now=NOW).allowed


@pytest.mark.parametrize("db", [_Db(fail_suppression=True), _Db(fail_health=True)])
def test_outbound_allowed_fails_closed_on_read_errors(monkeypatch, db):
    _use(monkeypatch, db)
    assert not dh.outbound_allowed(fb.CHANNEL_GHL_SMS, now=NOW).allowed


def test_outbound_allowed_fails_closed_when_no_session(monkeypatch):
    def broken():
        raise RuntimeError("database down")

    monkeypatch.setattr(dh, "_session_factory", broken)
    monkeypatch.setattr(dh, "pause_enabled", lambda: True)
    assert not dh.outbound_allowed(fb.CHANNEL_GHL_SMS, now=NOW).allowed


def test_record_success_and_failure_write_and_never_raise(monkeypatch):
    db = _Db()
    _use(monkeypatch, db)
    dh.record_success(fb.FEED_IMAP_OPT_OUT, now=NOW)
    dh.record_failure(fb.FEED_IMAP_OPT_OUT, "IMAP4.error" * 40, now=NOW)
    (s1, p1), (s2, p2) = db.writes
    assert s1 is dh._SUCCESS and p1 == {"d": fb.FEED_IMAP_OPT_OUT, "now": NOW}
    assert s2 is dh._FAILURE and len(p2["detail"]) == 200

    def broken():
        raise RuntimeError("database down")

    monkeypatch.setattr(dh, "_session_factory", broken)
    dh.record_success(fb.FEED_IMAP_OPT_OUT)
    dh.record_failure(fb.FEED_IMAP_OPT_OUT, "x")


def test_freshness_returns_age_or_none(monkeypatch):
    _use(monkeypatch, _Db(_rows_list(**{fb.FEED_IMAP_OPT_OUT: 7})))
    assert dh.freshness(fb.FEED_IMAP_OPT_OUT, now=NOW) == timedelta(minutes=7)
    assert dh.freshness("never_seen", now=NOW) is None
