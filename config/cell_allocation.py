"""
LEARN-v2.2 Layer 3 — cell allocation (the downward half).

Spec §9.4: "Adaptive allocation (bandit-style): winning angles gradually earn
more volume; challengers keep a protected floor." Spec §1.5 line 188 states the
mechanics numerically: per cell (offer x avenue x angle), "kill (<3% reply after
30 sends) and double (>8%)".

CLONE-v2.2's CL4 already shipped the DOUBLE half live — see
config/venture_ladder.py's auto-double block and
src/services/venture_ladder.py:maybe_auto_double_cell. This module is the
missing KILL half and deliberately reuses CL4's window, cooldown and
stage-eligibility rather than declaring its own: two engines disagreeing about
"is this cell winning" is the fragmentation LEARN Layer 4 already refused once.

FOUR DECISIONS ARE LOAD-BEARING HERE. Changing a number is cheap; changing one
of these changes what the engine does.

1. A KILL IS A THROTTLE, NEVER A ZERO.
   A cell producing no drafts accumulates no replies, so it can never clear the
   threshold that would revive it — a hard zero is permanent in practice, and
   the fast trigger below guarantees some of those kills are wrong. Throttling
   to a floor keeps a thin evidence stream so a bad call self-corrects. This
   also matches spec line 238's reserved challenger capacity and line 128's
   "degradation threshold AND review date": a sunset cell is revisited, not
   deleted.

2. THE FAST TRIGGER REQUIRES *ZERO* REPLIES, NOT MERELY A LOW RATE.
   Spec says kill at <3% after 30 sends. Taken literally, 30 sends is a small
   sample: a cell whose true rate is 5% returns zero replies in 30 sends about
   21% of the time. Rather than pick between the spec's speed and a
   statistically safe sample, the rule is split — a cell with literally nothing
   to show after ZERO_REPLY_MIN_SENDS is throttled immediately, while an
   ambiguous cell (some replies, but under the floor) waits for
   AMBIGUOUS_MIN_SENDS. The obvious duds stop wasting capacity fast; the
   arguable ones get real evidence.

3. THE KILL NEEDS AT LEAST AS MUCH EVIDENCE AS THE DOUBLE, NOT LESS.
   A wrong double wastes some sends. A wrong kill suppresses an angle, and a
   suppressed angle stops generating the data that would exonerate it. Hence
   AMBIGUOUS_MIN_SENDS matches CL4's AUTO_DOUBLE_MIN_SAMPLE exactly, and the
   asymmetric fast path is gated on the one signal that is unambiguous at low
   volume: nothing at all.

4. REVIVAL IS AUTOMATIC AND SYMMETRIC.
   A throttled cell that climbs back over the floor returns to full production
   without a human step, because the whole point of (1) is that a false throttle
   repairs itself. The cooldown prevents ping-pong across a threshold boundary.

NOT IN THIS MODULE, deliberately: calls and dollars. Spec line 188 lists four
scoreboard columns, but `booking.created` and `payment.received` are declared in
src/services/fleet_event_bus.py:FLEET_EVENT_TYPES and emitted by nothing — the
bus's only producer today is stripe_webhooks.py's `subscription.cancelled`,
which carries subscriber_id and no opportunity_thread_id. Every threshold the
spec actually specifies is reply-based, so the decision inputs are complete;
the two reporting columns wait on emitters.

Usage:
    from config.cell_allocation import decide_thresholds, validate_allocation_config
"""

from config.cora_cell_grid import get_cell
from config.venture_ladder import (
    AUTO_DOUBLE_COOLDOWN_DAYS,
    AUTO_DOUBLE_ELIGIBLE_STAGES,
    AUTO_DOUBLE_MIN_SAMPLE,
    AUTO_DOUBLE_REPLY_RATE_PCT,
    AUTO_DOUBLE_WINDOW_DAYS,
)

# Trailing window for sends/replies. Same window as the double, so the two
# halves of the rule can never disagree about the same cell on the same day.
WINDOW_DAYS = AUTO_DOUBLE_WINDOW_DAYS

# 5. THE KILL BAR IS TEMPERATURE-AWARE, NOT A FLAT NUMBER.
#    Spec line 148: "warm and cold judged on different bars"; line 264 pins the
#    send-to-reply benchmarks at warm >=8%, cold >=1.5%. A single flat bar
#    throttles a healthy cold cell (2% is fine cold, failing warm) and spares a
#    failing warm one (4% is a disaster against an 8% benchmark). A cell's
#    temperature is a property of the relationship, read here from its angle:
#    win-back / auction-congrats / post-call imply a prior touch (warm);
#    everything else is net-new cold outreach.
KILL_REPLY_RATE_PCT_COLD = 1.5
# Warm cells are held to a higher bar, but the bar must stay strictly below the
# double's AUTO_DOUBLE_REPLY_RATE_PCT (8%) or a warm cell could qualify to be
# throttled and doubled on the same run. 5% is a clear-failure line against the
# 8% warm benchmark while preserving that gap.
KILL_REPLY_RATE_PCT_WARM = 5.0

# Retained as the cold default so config_snapshot / older callers still resolve
# a scalar; the live decision uses kill_rate_for_cell().
KILL_REPLY_RATE_PCT = KILL_REPLY_RATE_PCT_COLD

# Angles that imply a prior relationship — everything else is cold. Kept as a
# set over angle (not a per-cell flag) so a new cell inherits the right bar from
# its angle without a second place to edit.
WARM_ANGLES = frozenset({"win_back_offer", "auction_congrats", "post_call_recap"})

# Fast path (decision 2 above): this many sends with ZERO replies throttles now.
ZERO_REPLY_MIN_SENDS = 30

# Spec line 148: "60 for high-stakes kills." A high-stakes cell needs more
# evidence before even the unambiguous zero-reply path fires, because a wrong
# throttle on a whale cell is the single most expensive false verdict in the
# corpus. Founder-tier is the named whale offer (config/cora_cell_grid.py).
HIGH_STAKES_MIN_SENDS = 60
HIGH_STAKES_OFFERS = frozenset({"founder_tier"})

# Slow path: some replies but under the kill bar needs a real sample.
# Deliberately identical to the double's sample floor (decision 3 above). Note
# 200 already exceeds HIGH_STAKES_MIN_SENDS, so the slow path needs no separate
# high-stakes tier — only the fast zero-reply path does.
AMBIGUOUS_MIN_SENDS = AUTO_DOUBLE_MIN_SAMPLE


def kill_rate_for_cell(cell_id: str) -> float:
    """The kill/revive reply-rate bar for a cell, by its temperature.

    An unknown cell_id (not in the grid) is treated as cold — the conservative
    choice, since a lower bar throttles fewer cells.
    """
    cell = get_cell(cell_id)
    if cell is not None and cell.get("angle") in WARM_ANGLES:
        return KILL_REPLY_RATE_PCT_WARM
    return KILL_REPLY_RATE_PCT_COLD


def zero_reply_min_sends_for_cell(cell_id: str) -> int:
    """The zero-reply fast-path sample floor for a cell. High-stakes (whale)
    cells demand the spec's 60 sends; everything else the default 30."""
    cell = get_cell(cell_id)
    if cell is not None and cell.get("offer") in HIGH_STAKES_OFFERS:
        return HIGH_STAKES_MIN_SENDS
    return ZERO_REPLY_MIN_SENDS

# What a throttled cell produces, as a percentage of its normal target count.
# 25%, not 10%: the fast trigger is only defensible because a wrong throttle
# repairs itself, and at 10% a wrongly-throttled cell needs roughly a dozen
# passes to re-earn a verdict — long enough that "self-correcting" stops being
# true in any useful sense.
THROTTLE_FLOOR_PCT = 25

# Revival threshold. Clearing the same rate the kill is measured against — so
# revival is temperature-aware too, via kill_rate_for_cell(). REVIVE_REPLY_RATE_PCT
# is the cold default retained for config_snapshot / older scalar callers.
REVIVE_REPLY_RATE_PCT = KILL_REPLY_RATE_PCT
REVIVE_MIN_SENDS = ZERO_REPLY_MIN_SENDS

# No second verdict on the same cell inside this many days. Prevents a cell
# sitting on the boundary from throttling and reviving on alternate runs, which
# would make the audit log useless and the production count unstable.
VERDICT_COOLDOWN_DAYS = AUTO_DOUBLE_COOLDOWN_DAYS

# Stage/active gating is CL4's, unchanged: a venture that may not double may not
# be throttled either. Reused rather than redeclared.
ELIGIBLE_STAGES = AUTO_DOUBLE_ELIGIBLE_STAGES

# Blast-radius rail, mirroring the lesson-hygiene sweep's: one run may not
# throttle more than this many cells. A feed break that zeroes every reply count
# looks exactly like every cell failing at once, and the fast path would act on
# all of them in a single pass.
MAX_THROTTLES_PER_RUN = 3

# Verdicts. Only `throttle` and `revive` mutate anything.
VERDICT_THROTTLE = "throttle"
VERDICT_REVIVE = "revive"
VERDICT_HOLD = "hold"
VERDICT_SKIP_INSUFFICIENT_SAMPLE = "skip_insufficient_sample"
VERDICT_SKIP_COOLDOWN = "skip_cooldown"

MUTATING_VERDICTS = frozenset({VERDICT_THROTTLE, VERDICT_REVIVE})

ALL_VERDICTS = frozenset({
    VERDICT_THROTTLE,
    VERDICT_REVIVE,
    VERDICT_HOLD,
    VERDICT_SKIP_INSUFFICIENT_SAMPLE,
    VERDICT_SKIP_COOLDOWN,
})

# venture_ladder_events.decision values this module writes. Both are added to
# the table's CHECK by migrations/apply_cell_allocation_events.py.
DECISION_THROTTLE = "auto_throttle"
DECISION_REVIVE = "auto_revive"


def validate_allocation_config() -> list[str]:
    """Return a list of configuration errors; empty means valid.

    Called by the sweep before it touches the DB, in the same spirit as
    config/venture_ladder.py:validate_ladder_config() — a threshold set that
    cannot express a coherent decision should fail at startup, not silently
    throttle everything.
    """
    errors: list[str] = []

    if not 0 < THROTTLE_FLOOR_PCT < 100:
        errors.append(
            f"THROTTLE_FLOOR_PCT must be strictly between 0 and 100, got {THROTTLE_FLOOR_PCT} "
            "(0 would make a throttle a permanent kill; >=100 would make it a no-op)"
        )

    # Both temperature bars must stay below the double, or a cell of that
    # temperature could qualify to be throttled and doubled in the same run.
    for name, bar in (
        ("KILL_REPLY_RATE_PCT_COLD", KILL_REPLY_RATE_PCT_COLD),
        ("KILL_REPLY_RATE_PCT_WARM", KILL_REPLY_RATE_PCT_WARM),
    ):
        if bar >= AUTO_DOUBLE_REPLY_RATE_PCT:
            errors.append(
                f"{name} ({bar}) must be below the double's AUTO_DOUBLE_REPLY_RATE_PCT "
                f"({AUTO_DOUBLE_REPLY_RATE_PCT}) — otherwise a cell can qualify to be "
                "throttled and doubled in the same run"
            )

    if KILL_REPLY_RATE_PCT_COLD > KILL_REPLY_RATE_PCT_WARM:
        errors.append(
            f"KILL_REPLY_RATE_PCT_COLD ({KILL_REPLY_RATE_PCT_COLD}) above the warm bar "
            f"({KILL_REPLY_RATE_PCT_WARM}) inverts the spec: cold cells are held to a "
            "lower reply bar, not a higher one"
        )

    if HIGH_STAKES_MIN_SENDS < ZERO_REPLY_MIN_SENDS:
        errors.append(
            f"HIGH_STAKES_MIN_SENDS ({HIGH_STAKES_MIN_SENDS}) below ZERO_REPLY_MIN_SENDS "
            f"({ZERO_REPLY_MIN_SENDS}) would demand less evidence from the higher-stakes kill"
        )

    if AMBIGUOUS_MIN_SENDS < HIGH_STAKES_MIN_SENDS:
        errors.append(
            f"AMBIGUOUS_MIN_SENDS ({AMBIGUOUS_MIN_SENDS}) below HIGH_STAKES_MIN_SENDS "
            f"({HIGH_STAKES_MIN_SENDS}) would let the slow path fire on less evidence than "
            "the fast high-stakes path"
        )

    if VERDICT_COOLDOWN_DAYS <= 0:
        errors.append(
            f"VERDICT_COOLDOWN_DAYS must be positive, got {VERDICT_COOLDOWN_DAYS} — "
            "without it a boundary cell flips verdict on every run"
        )

    if MAX_THROTTLES_PER_RUN < 1:
        errors.append(
            f"MAX_THROTTLES_PER_RUN must be at least 1, got {MAX_THROTTLES_PER_RUN}"
        )

    return errors


def config_snapshot() -> dict[str, object]:
    """The thresholds in force, frozen into every audit row.

    A verdict is only reconstructable months later if the numbers it was
    measured against travel with it — CL4's gate_results does the same thing.
    """
    return {
        "window_days": WINDOW_DAYS,
        "kill_reply_rate_pct_cold": KILL_REPLY_RATE_PCT_COLD,
        "kill_reply_rate_pct_warm": KILL_REPLY_RATE_PCT_WARM,
        "zero_reply_min_sends": ZERO_REPLY_MIN_SENDS,
        "high_stakes_min_sends": HIGH_STAKES_MIN_SENDS,
        "ambiguous_min_sends": AMBIGUOUS_MIN_SENDS,
        "throttle_floor_pct": THROTTLE_FLOOR_PCT,
        "revive_min_sends": REVIVE_MIN_SENDS,
        "verdict_cooldown_days": VERDICT_COOLDOWN_DAYS,
        "max_throttles_per_run": MAX_THROTTLES_PER_RUN,
    }
