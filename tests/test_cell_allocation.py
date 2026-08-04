"""
Unit tests for src.services.cell_allocation.decide() and the config
classifiers that resolve its per-cell thresholds.

Pure-function tests — no DB, no fixtures required.
"""
from config.cell_allocation import (
    AMBIGUOUS_MIN_SENDS,
    HIGH_STAKES_MIN_SENDS,
    KILL_REPLY_RATE_PCT_COLD,
    KILL_REPLY_RATE_PCT_WARM,
    REVIVE_MIN_SENDS,
    VERDICT_COOLDOWN_DAYS,
    VERDICT_HOLD,
    VERDICT_REVIVE,
    VERDICT_SKIP_COOLDOWN,
    VERDICT_SKIP_INSUFFICIENT_SAMPLE,
    VERDICT_THROTTLE,
    ZERO_REPLY_MIN_SENDS,
    kill_rate_for_cell,
    zero_reply_min_sends_for_cell,
)
from src.services.cell_allocation import decide


def _decide(
    sends,
    replies,
    *,
    is_throttled=False,
    days_since_last_verdict=None,
    kill_rate_pct=KILL_REPLY_RATE_PCT_COLD,
    zero_reply_min_sends=ZERO_REPLY_MIN_SENDS,
):
    """Test wrapper: defaults the resolved thresholds to the cold / non-high-
    stakes tier so each test only overrides the one it exercises."""
    return decide(
        sends,
        replies,
        is_throttled=is_throttled,
        days_since_last_verdict=days_since_last_verdict,
        kill_rate_pct=kill_rate_pct,
        zero_reply_min_sends=zero_reply_min_sends,
    )


# ── Fast-kill ─────────────────────────────────────────────────────────────────

def test_fast_kill_throttles_at_zero_replies_sufficient_sends():
    """0 replies after zero_reply_min_sends → VERDICT_THROTTLE."""
    assert _decide(sends=ZERO_REPLY_MIN_SENDS, replies=0) == VERDICT_THROTTLE


def test_fast_kill_does_not_trigger_below_min_sends():
    """0 replies but under zero_reply_min_sends is still insufficient sample."""
    assert _decide(sends=ZERO_REPLY_MIN_SENDS - 1, replies=0) == VERDICT_SKIP_INSUFFICIENT_SAMPLE


# ── High-stakes fast-kill bar (spec line 148: "60 for high-stakes kills") ──────

def test_high_stakes_fast_kill_needs_sixty_sends():
    """A high-stakes cell with 0 replies at 30 sends must NOT throttle — its
    zero-reply floor is 60, so 30 is still insufficient sample."""
    assert HIGH_STAKES_MIN_SENDS == 60
    verdict = _decide(
        sends=30, replies=0, zero_reply_min_sends=HIGH_STAKES_MIN_SENDS
    )
    assert verdict == VERDICT_SKIP_INSUFFICIENT_SAMPLE


def test_high_stakes_fast_kill_fires_at_sixty():
    """0 replies at 60 sends throttles a high-stakes cell."""
    verdict = _decide(
        sends=HIGH_STAKES_MIN_SENDS, replies=0, zero_reply_min_sends=HIGH_STAKES_MIN_SENDS
    )
    assert verdict == VERDICT_THROTTLE


# ── Slow-kill ─────────────────────────────────────────────────────────────────

def test_slow_kill_throttles_below_cold_bar_with_full_sample():
    """1 reply / 200 sends = 0.5% — below the cold bar (1.5%) with a full
    AMBIGUOUS_MIN_SENDS sample → VERDICT_THROTTLE."""
    sends = AMBIGUOUS_MIN_SENDS
    replies = 1
    rate = 100.0 * replies / sends
    assert rate < KILL_REPLY_RATE_PCT_COLD
    assert _decide(sends=sends, replies=replies) == VERDICT_THROTTLE


def test_insufficient_sample_blocks_slow_kill():
    """Below-bar but NONZERO rate with a sample too small (< AMBIGUOUS_MIN_SENDS)
    → VERDICT_SKIP_INSUFFICIENT_SAMPLE. (A zero-reply cell would take the fast
    path instead, so a nonzero reply is required to exercise the slow gate.)"""
    sends = AMBIGUOUS_MIN_SENDS - 1
    replies = 1  # ~0.5% cold-below-bar, but nonzero so the fast path is skipped
    assert _decide(sends=sends, replies=replies) == VERDICT_SKIP_INSUFFICIENT_SAMPLE


# ── Warm vs cold bar (spec line 264: warm >=8%, cold >=1.5%) ──────────────────

def test_cold_cell_at_two_pct_is_healthy_not_throttled():
    """2% reply is healthy for a cold cell (bar 1.5%) — must HOLD, not throttle.
    This is the exact case the flat 3% bar got wrong."""
    sends = AMBIGUOUS_MIN_SENDS
    replies = round(sends * 0.02)  # 2%
    verdict = _decide(sends=sends, replies=replies, kill_rate_pct=KILL_REPLY_RATE_PCT_COLD)
    assert verdict == VERDICT_HOLD


def test_warm_cell_at_four_pct_is_failing_and_throttled():
    """4% reply is failing for a warm cell (bar 5%) — must THROTTLE.
    The flat 3% bar wrongly spared this cell."""
    sends = AMBIGUOUS_MIN_SENDS
    replies = round(sends * 0.04)  # 4%
    verdict = _decide(sends=sends, replies=replies, kill_rate_pct=KILL_REPLY_RATE_PCT_WARM)
    assert verdict == VERDICT_THROTTLE


# ── Hold ──────────────────────────────────────────────────────────────────────

def test_hold_on_good_reply_rate():
    """Reply rate well above the cold bar on a full sample → VERDICT_HOLD."""
    sends = AMBIGUOUS_MIN_SENDS
    replies = round(sends * (KILL_REPLY_RATE_PCT_COLD / 100.0)) + 5
    assert _decide(sends=sends, replies=replies) == VERDICT_HOLD


# ── Cooldown ─────────────────────────────────────────────────────────────────

def test_cooldown_blocks_throttle():
    """A verdict issued within VERDICT_COOLDOWN_DAYS → VERDICT_SKIP_COOLDOWN."""
    verdict = _decide(
        sends=ZERO_REPLY_MIN_SENDS, replies=0,
        days_since_last_verdict=VERDICT_COOLDOWN_DAYS - 1,
    )
    assert verdict == VERDICT_SKIP_COOLDOWN


def test_cooldown_does_not_block_after_elapsed():
    """A verdict issued VERDICT_COOLDOWN_DAYS ago is no longer in cooldown."""
    verdict = _decide(
        sends=ZERO_REPLY_MIN_SENDS, replies=0,
        days_since_last_verdict=float(VERDICT_COOLDOWN_DAYS),
    )
    assert verdict == VERDICT_THROTTLE


def test_no_cooldown_when_no_prior_verdict():
    """days_since_last_verdict=None means no previous verdict — no cooldown."""
    assert _decide(sends=ZERO_REPLY_MIN_SENDS, replies=0) == VERDICT_THROTTLE


# ── Revival ───────────────────────────────────────────────────────────────────

def test_revive_throttled_cell_with_good_rate():
    """Throttled cell + rate >= kill bar + enough sends → VERDICT_REVIVE."""
    sends = REVIVE_MIN_SENDS
    replies = round(sends * (KILL_REPLY_RATE_PCT_COLD / 100.0)) + 2
    verdict = _decide(sends=sends, replies=replies, is_throttled=True)
    assert verdict == VERDICT_REVIVE


def test_throttled_cell_insufficient_revival_sample_holds():
    """Throttled cell with a good rate but < REVIVE_MIN_SENDS → VERDICT_HOLD."""
    sends = REVIVE_MIN_SENDS - 1
    replies = round(sends * (KILL_REPLY_RATE_PCT_COLD / 100.0)) + 2
    verdict = _decide(sends=sends, replies=replies, is_throttled=True)
    assert verdict == VERDICT_HOLD


def test_throttled_cell_bad_rate_holds_not_rethrottled():
    """Throttled cell with still-bad rate → VERDICT_HOLD (no re-throttle)."""
    verdict = _decide(sends=REVIVE_MIN_SENDS, replies=0, is_throttled=True)
    assert verdict == VERDICT_HOLD


def test_revival_is_temperature_aware():
    """A warm throttled cell must clear the WARM bar to revive, not the cold one.
    A rate between the cold and warm bars revives a cold cell but holds a warm one."""
    sends = REVIVE_MIN_SENDS
    replies = round(sends * 0.03)  # 3% — above cold 1.5%, below warm 5%
    assert _decide(sends=sends, replies=replies, is_throttled=True,
                   kill_rate_pct=KILL_REPLY_RATE_PCT_COLD) == VERDICT_REVIVE
    assert _decide(sends=sends, replies=replies, is_throttled=True,
                   kill_rate_pct=KILL_REPLY_RATE_PCT_WARM) == VERDICT_HOLD


# ── Config classifiers (resolve thresholds from the real cell grid) ───────────

def test_founder_tier_cell_is_high_stakes():
    """founder_tier_blitz is a founder-tier offer → 60-send fast-kill floor."""
    assert zero_reply_min_sends_for_cell("founder_tier_blitz") == HIGH_STAKES_MIN_SENDS


def test_non_founder_cell_is_default_stakes():
    """auction_fast_follow is core_subscription → default 30-send floor."""
    assert zero_reply_min_sends_for_cell("auction_fast_follow") == ZERO_REPLY_MIN_SENDS


def test_win_back_cell_is_warm():
    """win_back uses the win_back_offer angle → warm bar."""
    assert kill_rate_for_cell("win_back") == KILL_REPLY_RATE_PCT_WARM


def test_founder_blitz_cell_is_cold():
    """founder_tier_blitz (scarcity_seat_number angle) is cold outreach → cold bar."""
    assert kill_rate_for_cell("founder_tier_blitz") == KILL_REPLY_RATE_PCT_COLD


def test_unknown_cell_defaults_cold_and_default_stakes():
    """An unknown cell_id is treated as cold, default stakes — the conservative
    choice that throttles fewer cells."""
    assert kill_rate_for_cell("no_such_cell") == KILL_REPLY_RATE_PCT_COLD
    assert zero_reply_min_sends_for_cell("no_such_cell") == ZERO_REPLY_MIN_SENDS
