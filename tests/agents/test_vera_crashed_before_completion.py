"""
Unit tests for check_crashed_before_completion() (src/agents/vera/checks/live_state.py).

Follows the same DB-mocking pattern as tests/agents/test_vera_revenue_truth.py
(monkeypatch vera_db.session_scope with a fake session returning controlled
rows) rather than a live vera_readonly connection — this check's interesting
logic is the Python-side age-guard filter, not the query itself, so a fake
session is both sufficient and far faster/more deterministic than the real
thing.

Regression coverage for a real self-review finding: the check originally had
no minimum-age guard at all, so a source still legitimately mid-run when the
report happens to fire would be reported as "crashed" just for being in
progress — the exact false-alarm failure mode this whole classification
system exists to close.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import src.agents.vera.checks.live_state as live_state
from src.agents.vera.checks.live_state import check_crashed_before_completion


@contextmanager
def _fake_session_scope(rows):
    class _Result:
        def mappings(self):
            return self

        def all(self):
            return rows

    class _FakeSession:
        def execute(self, *_args, **_kwargs):
            return _Result()

    yield _FakeSession()


def _row(source_type, county_id, attempt_started_at):
    return {
        "source_type": source_type,
        "county_id": county_id,
        "attempt_started_at": attempt_started_at,
    }


def test_old_enough_heartbeat_with_no_completion_is_flagged(monkeypatch):
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    started = now - timedelta(hours=3)  # well past the default 120-minute guard
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([_row("flood_damage", "hillsborough", started)]),
    )
    crashed = check_crashed_before_completion(now=now)
    assert len(crashed) == 1
    assert crashed[0]["source_type"] == "flood_damage"


def test_recent_heartbeat_within_age_guard_is_not_flagged(monkeypatch):
    """A source that started 5 minutes ago and hasn't completed yet is very
    likely still legitimately running — must not be reported as crashed."""
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    started = now - timedelta(minutes=5)
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([_row("flood_damage", "hillsborough", started)]),
    )
    crashed = check_crashed_before_completion(now=now)
    assert crashed == []


def test_exactly_at_the_boundary_is_not_flagged(monkeypatch):
    """min_age_minutes is a strict "older than" threshold, not "at least" —
    a heartbeat exactly at the cutoff hasn't definitively been running long
    enough to call it crashed yet."""
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    started = now - timedelta(minutes=120)
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([_row("flood_damage", "hillsborough", started)]),
    )
    crashed = check_crashed_before_completion(now=now, min_age_minutes=120)
    assert crashed == []


def test_custom_min_age_minutes_is_respected(monkeypatch):
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    started = now - timedelta(minutes=45)
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([_row("flood_damage", "hillsborough", started)]),
    )
    # A stricter 30-minute guard should flag a 45-minute-old heartbeat.
    crashed = check_crashed_before_completion(now=now, min_age_minutes=30)
    assert len(crashed) == 1


def test_naive_db_datetime_is_treated_as_utc(monkeypatch):
    """scraper_run_stats.attempt_started_at is a naive DateTime column
    (stores UTC via func.now(), same convention as last_success in
    check_cron_freshness()) — the age guard must attach UTC tzinfo before
    comparing, not crash on a naive-vs-aware comparison."""
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    started_naive = (now - timedelta(hours=3)).replace(tzinfo=None)
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([_row("flood_damage", "hillsborough", started_naive)]),
    )
    crashed = check_crashed_before_completion(now=now)
    assert len(crashed) == 1


def test_no_rows_returns_empty_list(monkeypatch):
    monkeypatch.setattr(
        live_state.vera_db, "session_scope",
        lambda: _fake_session_scope([]),
    )
    assert check_crashed_before_completion() == []
