"""
Unit tests for src.services.cell_allocation.decide().

Pure-function tests — no DB, no fixtures required.
"""
import pytest

from config.cell_allocation import (
    AMBIGUOUS_MIN_SENDS,
    KILL_REPLY_RATE_PCT,
    REVIVE_MIN_SENDS,
    REVIVE_REPLY_RATE_PCT,
    VERDICT_COOLDOWN_DAYS,
    VERDICT_HOLD,
    VERDICT_REVIVE,
    VERDICT_SKIP_COOLDOWN,
    VERDICT_SKIP_INSUFFICIENT_SAMPLE,
    VERDICT_THROTTLE,
    ZERO_REPLY_MIN_SENDS,
)
from src.services.cell_allocation import decide


# ── Fast-kill ─────────────────────────────────────────────────────────────────

def test_fast_kill_throttles_at_zero_replies_sufficient_sends():
    """0 replies after ZERO_REPLY_MIN_SENDS sends → VERDICT_THROTTLE."""
    verdict = decide(
        sends=ZERO_REPLY_MIN_SENDS,
        replies=0,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_THROTTLE


def test_fast_kill_does_not_trigger_below_min_sends():
    """0 replies but under ZERO_REPLY_MIN_SENDS is still insufficient sample."""
    verdict = decide(
        sends=ZERO_REPLY_MIN_SENDS - 1,
        replies=0,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_SKIP_INSUFFICIENT_SAMPLE


# ── Slow-kill ─────────────────────────────────────────────────────────────────

def test_slow_kill_throttles_below_rate_floor_with_full_sample():
    """5 replies / 200 sends = 2.5% — below KILL_REPLY_RATE_PCT (3%) with
    a full AMBIGUOUS_MIN_SENDS sample → VERDICT_THROTTLE."""
    sends = 200
    replies = 5
    rate = 100.0 * replies / sends  # 2.5%
    assert rate < KILL_REPLY_RATE_PCT
    assert sends >= AMBIGUOUS_MIN_SENDS

    verdict = decide(
        sends=sends,
        replies=replies,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_THROTTLE


def test_insufficient_sample_blocks_slow_kill():
    """5 replies / 100 sends — would look like a slow kill but sample is too
    small (100 < AMBIGUOUS_MIN_SENDS) → VERDICT_SKIP_INSUFFICIENT_SAMPLE."""
    sends = 100
    assert sends < AMBIGUOUS_MIN_SENDS
    replies = 5
    rate = 100.0 * replies / sends  # 5% — above 3% anyway but irrelevant here
    # Use a below-floor rate to make the slow-kill path the only blocker.
    replies_low = 2  # 2% < 3%
    verdict = decide(
        sends=sends,
        replies=replies_low,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_SKIP_INSUFFICIENT_SAMPLE


# ── Hold ──────────────────────────────────────────────────────────────────────

def test_hold_on_good_reply_rate():
    """Reply rate well above the kill floor on a full sample → VERDICT_HOLD."""
    verdict = decide(
        sends=AMBIGUOUS_MIN_SENDS,
        replies=round(AMBIGUOUS_MIN_SENDS * KILL_REPLY_RATE_PCT / 100) + 5,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_HOLD


# ── Cooldown ─────────────────────────────────────────────────────────────────

def test_cooldown_blocks_throttle():
    """A verdict issued within VERDICT_COOLDOWN_DAYS → VERDICT_SKIP_COOLDOWN."""
    verdict = decide(
        sends=ZERO_REPLY_MIN_SENDS,
        replies=0,
        is_throttled=False,
        days_since_last_verdict=VERDICT_COOLDOWN_DAYS - 1,
    )
    assert verdict == VERDICT_SKIP_COOLDOWN


def test_cooldown_does_not_block_after_elapsed():
    """A verdict issued VERDICT_COOLDOWN_DAYS ago is no longer in cooldown."""
    verdict = decide(
        sends=ZERO_REPLY_MIN_SENDS,
        replies=0,
        is_throttled=False,
        days_since_last_verdict=float(VERDICT_COOLDOWN_DAYS),
    )
    assert verdict == VERDICT_THROTTLE


def test_no_cooldown_when_no_prior_verdict():
    """days_since_last_verdict=None means no previous verdict — no cooldown."""
    verdict = decide(
        sends=ZERO_REPLY_MIN_SENDS,
        replies=0,
        is_throttled=False,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_THROTTLE


# ── Revival ───────────────────────────────────────────────────────────────────

def test_revive_throttled_cell_with_good_rate():
    """Throttled cell + rate >= REVIVE_REPLY_RATE_PCT + enough sends → VERDICT_REVIVE."""
    sends = REVIVE_MIN_SENDS
    # Make the rate clearly above the revival threshold.
    replies = round(sends * (REVIVE_REPLY_RATE_PCT / 100.0)) + 2
    verdict = decide(
        sends=sends,
        replies=replies,
        is_throttled=True,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_REVIVE


def test_throttled_cell_insufficient_revival_sample_holds():
    """Throttled cell with a good rate but < REVIVE_MIN_SENDS → VERDICT_HOLD.
    It should not re-throttle a cell that is already throttled."""
    sends = REVIVE_MIN_SENDS - 1
    replies = round(sends * (REVIVE_REPLY_RATE_PCT / 100.0)) + 2
    verdict = decide(
        sends=sends,
        replies=replies,
        is_throttled=True,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_HOLD


def test_throttled_cell_bad_rate_holds_not_rethrottled():
    """Throttled cell with still-bad rate → VERDICT_HOLD (no re-throttle)."""
    sends = REVIVE_MIN_SENDS
    replies = 0  # would normally fast-kill an un-throttled cell
    verdict = decide(
        sends=sends,
        replies=replies,
        is_throttled=True,
        days_since_last_verdict=None,
    )
    assert verdict == VERDICT_HOLD
