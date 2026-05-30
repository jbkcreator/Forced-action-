"""Phase 2 tests — churn signal extraction (churn_signals.py).

All DB I/O is stubbed via fake sessions. No real DB required.
Clock is always injected so results are deterministic.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.churn_signals import (
    engagement_dampener,
    inactivity_trajectory,
    payment_stress,
    usage_slope,
)

# ── Helpers ────────────────────────────────────────────────────────────────

_NOW = datetime(2026, 5, 30, 13, 0, 0, tzinfo=timezone.utc)


def _dt(days_ago: float) -> datetime:
    return _NOW - timedelta(days=days_ago)


class _FakeResult:
    """Wraps a canned value; supports .scalars().all(), .scalar_one_or_none(), .fetchall()."""

    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def all(self):
        return self._value

    def scalar_one_or_none(self):
        return self._value

    def fetchall(self):
        return self._value


class _FakeDB:
    """Queue-based fake session. Each execute() pops the next canned result."""

    def __init__(self, results):
        self._queue = list(results)
        self._added = []

    def execute(self, *_args, **_kwargs):
        return self._queue.pop(0)

    def add(self, obj):
        self._added.append(obj)

    def flush(self):
        pass


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


# ── inactivity_trajectory ─────────────────────────────────────────────────


def test_inactivity_personalized_vs_global():
    """Bursty (14-day cadence) buyer silent 3 days scores LOW; daily buyer scores HIGH."""
    # --- bursty buyer: debit every 14 days, last was 3 days ago ---
    bursty_debits = [_dt(3 + 14 * i) for i in range(10)]  # 10 debits, each 14d apart
    bursty_created = _dt(200)

    db_bursty = _FakeDB([
        _FakeResult(bursty_debits),       # debit_times query
        _FakeResult(bursty_created),       # subscriber.created_at
    ])
    result_bursty = inactivity_trajectory(1, db_bursty, now=_NOW)

    # 3 days since last debit vs 14-day median → score should be 0 (on-cadence)
    assert result_bursty["score"] == 0.0, f"Bursty buyer score too high: {result_bursty}"
    assert result_bursty["raw"]["personalized"] is True

    # --- daily buyer: debit every 1 day, last was 3 days ago ---
    daily_debits = [_dt(3 + i) for i in range(10)]  # 10 debits, each 1d apart
    daily_created = _dt(200)

    db_daily = _FakeDB([
        _FakeResult(daily_debits),
        _FakeResult(daily_created),
    ])
    result_daily = inactivity_trajectory(2, db_daily, now=_NOW)

    # 3 days since last debit vs 1-day median → score >= 0.5
    assert result_daily["score"] >= 0.5, f"Daily buyer score too low: {result_daily}"
    assert result_daily["raw"]["personalized"] is True

    assert result_bursty["score"] < result_daily["score"]


def test_global_fallback_for_new_account():
    """Account younger than 21 days uses flat days_since / ONSET_DAYS threshold."""
    # New account: 10 days old, 2 debits (below MIN_HISTORY_DEBITS=3)
    debits = [_dt(1), _dt(5)]
    new_created = _dt(10)

    db = _FakeDB([
        _FakeResult(debits),
        _FakeResult(new_created),
    ])
    result = inactivity_trajectory(3, db, now=_NOW)

    assert result["raw"]["personalized"] is False
    # days_since = 1, flat score = 1/5 = 0.2
    assert result["score"] == pytest.approx(0.2, abs=0.01)


def test_inactivity_no_history_yields_max_score():
    """Subscriber with no debit history gets score 1.0 (999 days since any debit)."""
    db = _FakeDB([
        _FakeResult([]),         # no debits
        _FakeResult(_dt(200)),   # created 200 days ago
    ])
    result = inactivity_trajectory(4, db, now=_NOW)
    assert result["score"] == 1.0
    assert result["raw"]["days_since_last_debit"] == 999.0


# ── usage_slope ───────────────────────────────────────────────────────────


def test_usage_slope_detects_decline():
    """10 debits last week → 2 this week yields high (≥0.8) score."""
    # recent window: 2 debits at amount=-10 each
    recent = [-10, -10]
    # prior window: 10 debits at amount=-10 each
    prior = [-10] * 10

    db = _FakeDB([
        _FakeResult(recent),
        _FakeResult(prior),
    ])
    result = usage_slope(1, db, now=_NOW)

    # slope = (2-10)/10 = -0.8 → score = 0.8
    assert result["score"] == pytest.approx(0.8, abs=0.01)
    assert result["raw"]["recent_7d_count"] == 2
    assert result["raw"]["prior_7d_count"] == 10
    assert result["raw"]["count_slope"] == pytest.approx(-0.8, abs=0.01)


def test_usage_slope_flat_when_stable():
    """Steady cadence → score ≈ 0."""
    db = _FakeDB([
        _FakeResult([-10, -10, -10, -10, -10]),  # recent: 5
        _FakeResult([-10, -10, -10, -10, -10]),  # prior: 5
    ])
    result = usage_slope(2, db, now=_NOW)

    assert result["score"] == pytest.approx(0.0, abs=0.01)
    assert result["raw"]["count_slope"] == pytest.approx(0.0, abs=0.01)


def test_usage_slope_zero_prior_no_divide():
    """Zero prior debits does not raise ZeroDivisionError."""
    db = _FakeDB([
        _FakeResult([]),   # recent: 0
        _FakeResult([]),   # prior: 0
    ])
    result = usage_slope(3, db, now=_NOW)
    # (0-0)/max(0,1) = 0 → score 0
    assert result["score"] == 0.0


# ── payment_stress ────────────────────────────────────────────────────────


def test_payment_stress_grace_and_recovery_flags():
    """grace + recovery_day3_sent → score ≥ 0.55."""
    sub = _ns(
        status="grace",
        recovery_day3_sent=True,
        recovery_day5_sent=False,
        missed_lead_count=0,
        disputed_count=0,
    )
    db = MagicMock()
    result = payment_stress(sub, db)

    assert result["score"] == pytest.approx(0.55, abs=0.01)
    assert result["raw"]["in_grace"] is True
    assert result["raw"]["recovery_day3_sent"] is True


def test_payment_stress_max_clipped():
    """All flags set → score clips to 1.0."""
    sub = _ns(
        status="grace",
        recovery_day3_sent=True,
        recovery_day5_sent=True,
        missed_lead_count=5,
        disputed_count=2,
    )
    result = payment_stress(sub, MagicMock())
    assert result["score"] == 1.0


def test_payment_stress_clean_subscriber():
    """Active subscriber with no flags → score 0.0."""
    sub = _ns(
        status="active",
        recovery_day3_sent=False,
        recovery_day5_sent=False,
        missed_lead_count=0,
        disputed_count=0,
    )
    result = payment_stress(sub, MagicMock())
    assert result["score"] == 0.0


# ── engagement_dampener ───────────────────────────────────────────────────


def test_engagement_dampener_reduces_risk():
    """Silent buyer with a recent reply scores higher than silent buyer with no engagement."""
    # With one reply
    row_with_reply = _ns(replied_at=_dt(1), clicked_at=None)
    db_with_reply = _FakeDB([_FakeResult([row_with_reply])])
    result_engaged = engagement_dampener(1, db_with_reply, now=_NOW)

    # With no engagement
    db_silent = _FakeDB([_FakeResult([])])
    result_silent = engagement_dampener(2, db_silent, now=_NOW)

    assert result_engaged["score"] > result_silent["score"]
    assert result_engaged["score"] == pytest.approx(0.75)
    assert result_silent["score"] == 0.0


def test_engagement_dampener_two_replies_max():
    """Two or more replies → score 1.0 (maximum dampening)."""
    rows = [_ns(replied_at=_dt(1), clicked_at=None), _ns(replied_at=_dt(3), clicked_at=None)]
    db = _FakeDB([_FakeResult(rows)])
    result = engagement_dampener(1, db, now=_NOW)
    assert result["score"] == 1.0


def test_engagement_dampener_click_only():
    """Click (no reply) yields partial dampening."""
    row = _ns(replied_at=None, clicked_at=_dt(2))
    db = _FakeDB([_FakeResult([row])])
    result = engagement_dampener(1, db, now=_NOW)
    assert result["score"] == pytest.approx(0.30)


# ── purity ────────────────────────────────────────────────────────────────


def test_signal_functions_pure():
    """Signal functions must not write to the DB (no .add() calls, no events emitted)."""
    sub = _ns(
        status="active",
        recovery_day3_sent=False,
        recovery_day5_sent=False,
        missed_lead_count=0,
        disputed_count=0,
    )
    debits = [_dt(2 + i) for i in range(5)]
    created = _dt(100)

    db_inact = _FakeDB([_FakeResult(debits), _FakeResult(created)])
    db_slope = _FakeDB([_FakeResult([-5, -5]), _FakeResult([-5, -5])])
    db_damp = _FakeDB([_FakeResult([])])

    inactivity_trajectory(1, db_inact, now=_NOW)
    usage_slope(1, db_slope, now=_NOW)
    payment_stress(sub, MagicMock())
    engagement_dampener(1, db_damp, now=_NOW)

    assert db_inact._added == [], "inactivity_trajectory wrote to DB"
    assert db_slope._added == [], "usage_slope wrote to DB"
    assert db_damp._added == [], "engagement_dampener wrote to DB"
