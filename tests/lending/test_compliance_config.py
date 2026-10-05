"""Spec §3.1/§3.2 values for the lending compliance floor (Wave 0)."""
from datetime import time

import pytest

from config import lending_compliance as cfg


def test_spec_values():
    assert cfg.CALL_WINDOW_START == time(8, 0)      # recipient-local FTSA floor
    assert cfg.CALL_WINDOW_END == time(20, 0)
    assert cfg.ET_WINDOW_START == time(9, 0)        # client hard stop, Eastern
    assert cfg.ET_WINDOW_END == time(19, 15)
    assert cfg.SHIFT_GROUPS == {"A": (time(9, 0), time(15, 0)), "B": (time(13, 0), time(19, 15))}
    assert cfg.MAX_ATTEMPTS_PER_PERIOD == 3
    assert cfg.ATTEMPT_PERIOD_HOURS == 24
    assert cfg.MAX_ATTEMPTS_TOTAL == 6
    assert cfg.ATTEMPT_HISTORY_BUSINESS_DAYS == 10
    assert cfg.DNC_SCRUB_MAX_AGE_DAYS == 7
    assert cfg.STOP_PROPAGATION_SLA_SECONDS == 60
    assert cfg.GEORGIA_ALLOWED_ENTITY_TYPES == frozenset({"LLC", "LP", "CORPORATION"})


def test_reason_codes_are_stable_strings():
    assert cfg.ReasonCode.NO_FRESH_SCRUB.value == "NO_FRESH_SCRUB"
    assert {c.value for c in cfg.ReasonCode} == {
        "INVALID_PHONE", "NO_FRESH_SCRUB", "SCRUB_FAILED", "NATIONAL_DNC", "STATE_DNC", "LITIGATOR",
        "SUPPRESSED", "GA_NATURAL_PERSON", "HOMESTEAD_OWNER_OCCUPIED", "OUTSIDE_CALL_WINDOW",
        "ATTEMPT_CAP_REACHED", "ATTEMPT_HISTORY_EXCEEDED",
        "BACKFLIP_CONFLICT", "BACKFLIP_FEED_STALE", "BACKFLIP_FEED_UNAVAILABLE",
    }


def test_removal_reasons_are_stable_strings():
    assert {r.value for r in cfg.RemovalReason} == {
        "opt_out", "call_window", "attempt_cap", "scrub_stale", "attempt_history",
    }


def test_validate_accepts_shipped_config():
    cfg.validate_lending_compliance_config()


@pytest.mark.parametrize("attr,value", [
    ("CALL_WINDOW_END", time(7, 0)),
    ("MAX_ATTEMPTS_PER_PERIOD", 0),
    ("DNC_SCRUB_MAX_AGE_DAYS", 0),
    ("WEEKLY_SCRUB_PERIOD_DAYS", 8),        # job less frequent than the sweep's freshness window
    ("WEEKLY_SCRUB_PERIOD_DAYS", 0),
    ("SCRUB_STALE_BREAKER_PCT", 0),
    ("ET_WINDOW_END", time(8, 0)),
])
def test_validate_rejects_bad_values(monkeypatch, attr, value):
    monkeypatch.setattr(cfg, attr, value)
    with pytest.raises(ValueError):
        cfg.validate_lending_compliance_config()


def test_weekly_rescrub_leaves_no_phone_stale_between_runs():
    """Finding 1: with the weekly cron period, every phone must be rescrubbed by the run
    before it would turn stale for the sweep. Simulates the job's and the sweep's exact
    cutoffs over four weekly runs, with the scrub landing a few minutes after the job starts."""
    from datetime import datetime, timedelta, timezone
    period = timedelta(days=7)
    scrub_lag = timedelta(minutes=3)
    start = datetime(2026, 10, 5, 6, 0, tzinfo=timezone.utc)   # a Monday, the cron time
    rescrub_after = timedelta(days=cfg.WEEKLY_RESCRUB_AFTER_DAYS)
    max_age = timedelta(days=cfg.DNC_SCRUB_MAX_AGE_DAYS)

    # Worst-case phone: scrubbed 1h before the first run, so it is just young enough to skip it.
    checked_at = start - timedelta(hours=1)
    for week in range(4):
        run_at = start + week * period
        if checked_at < run_at - rescrub_after:
            checked_at = run_at + scrub_lag
        # sample the sweep hourly until the next run's job has had time to finish
        for hour in range(7 * 24):
            moment = run_at + scrub_lag + timedelta(hours=hour)
            if moment >= run_at + period:
                break
            assert not checked_at < moment - max_age, f"stale at {moment} (checked {checked_at})"


def test_stale_scrub_breaker_trips_only_on_a_large_share_of_a_sizeable_pool():
    from src.lending.compliance import _scrub_stale_breaker_tripped
    assert _scrub_stale_breaker_tripped(stale=6, pool=20) is True       # 30% of a 20-phone pool
    assert _scrub_stale_breaker_tripped(stale=5, pool=20) is False      # exactly 25%: not over
    assert _scrub_stale_breaker_tripped(stale=19, pool=19) is False     # below the minimum pool
    assert _scrub_stale_breaker_tripped(stale=0, pool=500) is False
