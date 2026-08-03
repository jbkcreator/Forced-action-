"""
Autonomous venture ladder — CLONE-v2.2 / CL4.

CL3 made a second venture *configurable*. This module is the first half of
making it *decidable*: the seven rungs a venture climbs, what must be true to
climb each one, and the constants the auto-double rule scales on.

    radar -> probe -> pilot -> unit_economics -> cell -> spin_up -> portfolio

Pure config. Nothing here reads the DB, Redis or settings —
src/services/venture_ladder.py computes the values, this file says what they
must be. Same split as config/scoring.py vs src/services/cds_engine.py.

WHY GATES CARRY `no_metric_behavior`. The platform already has a gate machine
(config/lifecycle_guardrails.py:EXPANSION_GATES, driven by
src/tasks/county_launch_evaluator.py). Per docs/adr/0006 it is inert: its
`free_tier_cost_ratio` gate is hardcoded to None, `_gate_color(None)` returns
"red", `_all_green()` requires every gate green, and so no county launch can
ever fire. That machine's shape is worth copying; its silence about missing
metrics is not. Every gate below therefore declares what a missing value
means, and src/services/venture_ladder.py honours it rather than defaulting
to red. A gate whose metric cannot be computed yet must say
`no_metric_behavior: "green"` on purpose, or the ladder ships dead on day one.

The gate keys are per-FROM-stage because the ladder is linear: one stage has
exactly one successor, so "the gates to leave `probe`" and "the gates to enter
`pilot`" are the same set. `portfolio` is terminal and has none.
"""
from __future__ import annotations

from typing import Any, Literal

# ── Stages ───────────────────────────────────────────────────────────────────
# Order is the ladder. Index in this tuple IS the rung number; nothing else
# encodes progression, so inserting a stage here is the only edit needed to
# add a rung (plus its STAGE_GATES entry and the models.py CHECK constraint).

LADDER_STAGES: tuple[str, ...] = (
    "radar",           # scored candidate; ventures row exists but is_active=false
    "probe",           # sources reachable, county_sources resolvable
    "pilot",           # real sends through Relay, one cell, small ceiling
    "unit_economics",  # CAC / contribution margin / cost-per-reply hold up
    "cell",            # expanded from one cell to the grid; auto-double lives here
    "spin_up",         # full Clone-Pack applied and verified end to end
    "portfolio",       # steady state, folded into fleet reporting
)

TERMINAL_STAGE = "portfolio"
INITIAL_STAGE = "radar"

GateColor = Literal["green", "yellow", "red"]


def next_stage(stage: str) -> str | None:
    """The rung above `stage`, or None at the top / for an unknown stage."""
    try:
        index = LADDER_STAGES.index(stage)
    except ValueError:
        return None
    if index + 1 >= len(LADDER_STAGES):
        return None
    return LADDER_STAGES[index + 1]


# ── Evidence types ───────────────────────────────────────────────────────────
# The `evidence_type` values venture_ladder_evidence rows carry. Declared here
# so the service, the harness and the Stripe webhook all spell them the same
# way (the reason config/cora_cell_grid.py exists, one level up).

EVIDENCE_MARKET_SCORE = "market_score"
EVIDENCE_SCRAPE_SAMPLE = "scrape_sample"
EVIDENCE_PRESELL_COMMITMENT = "presell_commitment"
EVIDENCE_HARNESS_RESULT = "harness_result"

# There is deliberately NO cron_registered evidence type. Whether a venture has
# its own Relay sweep cron line is checked live by
# clone_pack.cron_line_present() against scripts/cron/crontab.txt — the line
# either exists or it does not, and a recorded claim about it could go stale
# the moment someone edits the crontab.

# ── Clone-Pack wiring ────────────────────────────────────────────────────────
# What each rung CONSUMES from the Clone-Pack and what it PRODUCES into it.
# Documentation with teeth: test_venture_ladder asserts every stage appears
# here, so a new rung cannot be added without saying how it relates to the
# Clone-Pack. The assembled object itself is src/services/clone_pack.py:
# ClonePack; `source_coverage` and `relay_ready`/`cron_line_present` are the
# fields the gates below actually read.

CLONE_PACK_IO: dict[str, dict[str, tuple[str, ...]]] = {
    "radar": {
        "inputs": ("venture.state", "venture.bankruptcy_court_code", "counties"),
        "outputs": ("ventures row (is_active=false)", "market_score evidence"),
    },
    "probe": {
        "inputs": ("source_coverage", "venture.template_county_id"),
        "outputs": ("county_sources with county-specific URLs", "scrape_sample evidence"),
    },
    "pilot": {
        "inputs": ("relay_ready", "venture.kill_switch_feature"),
        "outputs": ("dispatched relay_approval_queue rows",),
    },
    "unit_economics": {
        "inputs": ("counties", "attributed revenue and cost"),
        "outputs": ("proven contribution margin",),
    },
    "cell": {
        "inputs": ("venture.relay_daily_ceiling", "per-cell reply rates"),
        "outputs": ("raised ceiling", "cell production multipliers"),
    },
    "spin_up": {
        "inputs": ("the COMPLETE ClonePack", "cron_line_present", "harness_result evidence"),
        "outputs": ("a venture that can run unattended",),
    },
    TERMINAL_STAGE: {
        "inputs": ("the complete ClonePack",),
        "outputs": ("fleet reporting",),
    },
}

# ── Presell gate ─────────────────────────────────────────────────────────────
# Spin-up costs real money before it earns any: a domain, mailbox warmup, an
# Instantly seat, proxies, scraper compute, Claude tokens. The presell gate
# exists so that spend is authorised by evidence of demand rather than by
# enthusiasm.
#
# WHAT COUNTS: a refundable Stripe deposit. It is real money (so it is real
# evidence), refundable (so people will actually agree to one — a gate nobody
# can pass is the same as no venture), a Stripe object (so a webhook verifies
# it with no human in the loop, which is what "autonomous" has to mean), and
# it carries an amount, so the threshold can be "N people AND $X" rather than
# a bare count.
#
# `PRESELL_ACCEPTED_KINDS` is the escape hatch: each evidence payload carries a
# `kind`, and widening what counts as demand is a one-line edit here rather
# than a migration. Build for deposits, leave the door open.

PRESELL_ACCEPTED_KINDS: frozenset[str] = frozenset({"deposit"})

# Kinds understood by the payload validator but not currently accepted by the
# gate. Listed so a rejected row reports "kind not accepted" instead of the
# less useful "unknown kind".
PRESELL_KNOWN_KINDS: frozenset[str] = frozenset({
    "deposit",       # refundable Stripe deposit — the real evidence
    "first_month",   # full first month up front; stronger, rarely collectable pre-launch
    "saved_card",    # card on file, no charge; weaker evidence, more Stripe plumbing
    "letter_of_intent",  # needs a human to verify, which defeats the purpose
})

PRESELL_MIN_COMMITMENTS = 5
PRESELL_MIN_AMOUNT_CENTS = 250_000  # $2,500 across all verified commitments

# Transitions the presell gate applies to, as (from_stage, to_stage). Checked
# twice on purpose: once before the venture costs anything real (probe->pilot),
# and again before full spin-up, because a commitment collected months earlier
# is not evidence that demand still exists.
PRESELL_GATED_TRANSITIONS: frozenset[tuple[str, str]] = frozenset({
    ("probe", "pilot"),
    ("cell", "spin_up"),
})

# A commitment older than this stops counting toward the gate. Stale demand is
# not demand.
PRESELL_MAX_AGE_DAYS = 120

# ── Auto-double ──────────────────────────────────────────────────────────────
# Reply rate above the threshold doubles sending volume. The 8% figure is the
# same number config/lifecycle_guardrails.py:KILL_SWITCH["sms_reply_rate"]
# already treats as green — that guardrail only ever throttles DOWN on
# failure (falling back to static copy); this is the missing upward response.
#
# TWO LEVELS, TWO DIFFERENT KNOBS — do not conflate them:
#
#   venture-level  doubles ventures.relay_daily_ceiling, the real cap, which
#                  src/services/relay/guards.py:reserve_daily_slot() enforces
#                  through the Redis counter relay_daily_sent:{venture}:...
#
#   cell-level     there is no per-cell send allocation in this system, so
#                  "doubling a cell" cannot mean doubling a ceiling. It doubles
#                  that cell's target production count (how many drafts get
#                  made for it), shifting the mix toward what is working. The
#                  venture ceiling still caps total sends.

AUTO_DOUBLE_REPLY_RATE_PCT = 8.0

# Below this many sends in the window the rate is noise, not signal. 8% of 12
# sends is one reply.
AUTO_DOUBLE_MIN_SAMPLE = 200

AUTO_DOUBLE_WINDOW_DAYS = 14

# Days since the last auto-double before another may fire. NOT optional:
# doubling cold-email volume on a warming domain is how a sender gets
# blacklisted, and that failure is not reversible by lowering the number back.
AUTO_DOUBLE_COOLDOWN_DAYS = 7

# Hard ceiling on the ceiling. A runaway loop of doublings is the failure mode
# this bounds; 2000/day/channel is well above any planned venture volume.
AUTO_DOUBLE_MAX_CEILING = 2000

AUTO_DOUBLE_MULTIPLIER = 2

# Reputation kill-switch on the scale-up. A high reply rate alongside a high
# failure rate means the list is dirty, not that the copy is good.
#
# NAMED FOR WHAT IT MEASURES. This is the share of dispatched Relay items that
# came back 'failed', which is a PROXY for deliverability, not a true bounce
# rate — nothing in this repo ingests Instantly bounce webhooks per venture
# (email_campaign_snapshots.bounces belongs to the DBPR contractor campaigns,
# a different sending system). ADR 0006's second finding was a gate reporting
# a value that did not mean what its name said, so this one is not called
# bounce_pct. Wiring a real per-venture bounce feed is follow-up work, and
# when it lands this threshold is where it plugs in.
AUTO_DOUBLE_MAX_SEND_FAILURE_PCT = 2.0

# Per-cell production multiplier bounds, for the cell-level rule.
AUTO_DOUBLE_CELL_MAX_MULTIPLIER = 4

# ── Gate thresholds ──────────────────────────────────────────────────────────
# Shape mirrors EXPANSION_GATES, extended with the two fields that machine
# lacks:
#
#   direction            'higher_is_better' | 'lower_is_better' | 'binary'
#                        ('binary' = value >= 1.0 is green, anything else red)
#   no_metric_behavior   colour to use when the value is None. See the module
#                        docstring — this is the ADR-0006 lesson.
#
# `yellow_floor` is optional and only meaningful for a graded gate: a value
# between it and `threshold` reports yellow rather than red. Yellow never
# advances a venture (only all-green does), but it is the difference between
# "nearly there" and "nowhere near" in the Slack digest.

STAGE_GATES: dict[str, dict[str, dict[str, Any]]] = {
    "radar": {
        "market_score": {
            "threshold": 60.0,
            "yellow_floor": 45.0,
            "direction": "higher_is_better",
            # No recorded market score means nobody has assessed this market.
            # That is precisely the state the gate exists to block.
            "no_metric_behavior": "red",
            "description": ">=60 composite market score recorded as evidence",
        },
        "geography_resolvable": {
            "threshold": 1.0,
            "direction": "binary",
            "no_metric_behavior": "red",
            "description": "state + bankruptcy court resolve, and >=1 county is attached",
        },
        "county_overlap": {
            "threshold": 0.0,
            "direction": "lower_is_better",
            # Zero overlapping counties is the good state, and no rows found
            # IS zero overlap.
            "no_metric_behavior": "green",
            "description": "0 counties already claimed by another active venture",
        },
    },
    "probe": {
        "source_coverage_pct": {
            "threshold": 100.0,
            "yellow_floor": 80.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": "100% of REQUIRED_SIGNAL_TYPES have a county-specific URL",
        },
        "scrape_sample_count": {
            "threshold": 1.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": ">=1 verified non-empty scrape sample recorded",
        },
    },
    "pilot": {
        "relay_ready": {
            "threshold": 1.0,
            "direction": "binary",
            "no_metric_behavior": "red",
            "description": "Instantly campaign, sender, Slack channel and kill switch all resolved",
        },
        "dispatched_count": {
            "threshold": 25.0,
            "yellow_floor": 10.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": ">=25 items actually dispatched through Relay",
        },
        "compliance_block_pct": {
            "threshold": 10.0,
            "yellow_floor": 20.0,
            "direction": "lower_is_better",
            # No recorded blocks means nothing was blocked.
            "no_metric_behavior": "green",
            "description": "<=10% of queued items rejected or failed",
        },
    },
    "unit_economics": {
        # NOT "cac_usd". Blended CAC would have to include ad spend, and
        # marketing_spend is keyed by channel (see its docstring: the channel
        # vocabulary is COALESCE(utm_source, signup_source)) with no venture
        # dimension at all — there is no correct way to split a channel's
        # spend across ventures. This gate measures the platform cost the
        # ledger CAN attribute per acquisition, and is named for that.
        "platform_cost_per_acquisition_usd": {
            "threshold": 400.0,
            "yellow_floor": 600.0,
            "direction": "lower_is_better",
            "no_metric_behavior": "red",
            "description": "<=$400 attributed platform cost per new paying account",
        },
        "contribution_margin_usd": {
            "threshold": 0.01,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": "attributed revenue minus attributed cost is positive",
        },
        "cost_per_reply_usd": {
            "threshold": 25.0,
            "yellow_floor": 40.0,
            "direction": "lower_is_better",
            "no_metric_behavior": "red",
            "description": "<=$25 attributed cost per reply in the window",
        },
    },
    "cell": {
        "cells_above_floor": {
            "threshold": 2.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": ">=2 cells at or above the reply-rate floor",
        },
        "clean_auto_double_count": {
            "threshold": 1.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": ">=1 auto-double fired and held without a bounce breach",
        },
        "send_failure_pct": {
            "threshold": AUTO_DOUBLE_MAX_SEND_FAILURE_PCT,
            "yellow_floor": 4.0,
            "direction": "lower_is_better",
            # No dispatch failures recorded means nothing failed. The
            # dispatched_count gate at the pilot rung is what guarantees there
            # was a real sample to fail in the first place.
            "no_metric_behavior": "green",
            "description": f"<={AUTO_DOUBLE_MAX_SEND_FAILURE_PCT}% of dispatched items failed",
        },
    },
    "spin_up": {
        # COMPUTED LIVE from src/services/clone_pack.py:assemble(), not read from
        # a recorded claim. This is the rung where the Clone-Pack has to actually
        # be complete — counties attached, every required signal type on a
        # county-specific URL, the venture's own Relay identity resolved on the
        # row, and its own sweep cron line present. A self-reported evidence row
        # would let a venture assert readiness it does not have.
        "clone_pack_complete": {
            "threshold": 1.0,
            "direction": "binary",
            "no_metric_behavior": "red",
            "description": "Clone-Pack assembles with zero gaps",
        },
        "harness_pass": {
            "threshold": 1.0,
            "direction": "binary",
            "no_metric_behavior": "red",
            "description": "scripts/harness/venture_spinup_acceptance.py returned PASS",
        },
        "clean_sweep_count": {
            "threshold": 3.0,
            "direction": "higher_is_better",
            "no_metric_behavior": "red",
            "description": ">=3 consecutive Relay sweeps with no failures",
        },
    },
    TERMINAL_STAGE: {},
}

# Reply rate a cell must clear to count toward `cells_above_floor`. Lower than
# the auto-double trigger: a cell can be worth keeping without being worth
# doubling.
CELL_REPLY_RATE_FLOOR_PCT = 4.0
CELL_MIN_SAMPLE = 40

# Trailing window for the pilot / unit-economics / cell metrics.
METRIC_WINDOW_DAYS = 30

# Minimum share of a venture's cost that must be attributable before the
# unit-economics gates are trusted. api_usage_logs carries subscriber_id, not
# venture_key, so cost is attributed via subscribers.county_id ->
# counties.venture_key and rows with subscriber_id IS NULL (shared/system
# cost: scraping, batch jobs) cannot be attributed at all — see docs/adr/0006.
# A cost gate that silently ignores most of the spend is worse than no gate,
# so below this share the metric reports None and the gate's declared
# no_metric_behavior (red) applies.
MIN_COST_ATTRIBUTION_RATIO = 0.4


def gate_defs(stage: str) -> dict[str, dict[str, Any]]:
    """Gates that must be green to leave `stage`. Empty for the terminal
    stage and for any unknown stage."""
    return STAGE_GATES.get(stage, {})


def presell_required(from_stage: str, to_stage: str) -> bool:
    return (from_stage, to_stage) in PRESELL_GATED_TRANSITIONS


def validate_ladder_config() -> list[str]:
    """Return configuration inconsistencies, empty if sound.

    Called by the acceptance harness and by tests/test_venture_ladder.py. The
    ADR-0006 failure was a config problem that only showed up as silence in
    production, so the config checks itself: every stage has a gate entry,
    every gate declares a direction and a no_metric_behavior, and no gate is
    left in a state where it can never report green.
    """
    problems: list[str] = []

    for stage in LADDER_STAGES:
        if stage not in STAGE_GATES:
            problems.append(f"{stage}: no STAGE_GATES entry")
        io = CLONE_PACK_IO.get(stage)
        if not io:
            problems.append(f"{stage}: no CLONE_PACK_IO entry — say what it consumes and produces")
        elif not io.get("inputs") or not io.get("outputs"):
            problems.append(f"{stage}: CLONE_PACK_IO needs non-empty inputs and outputs")

    unknown_stages = set(STAGE_GATES) - set(LADDER_STAGES)
    if unknown_stages:
        problems.append(f"STAGE_GATES has unknown stage(s): {', '.join(sorted(unknown_stages))}")

    if STAGE_GATES.get(TERMINAL_STAGE):
        problems.append(f"{TERMINAL_STAGE} is terminal and must declare no gates")

    valid_directions = {"higher_is_better", "lower_is_better", "binary"}
    for stage, gates in STAGE_GATES.items():
        for name, cfg in gates.items():
            where = f"{stage}.{name}"
            direction = cfg.get("direction")
            if direction not in valid_directions:
                problems.append(f"{where}: direction must be one of {sorted(valid_directions)}")
            if cfg.get("no_metric_behavior") not in {"green", "yellow", "red"}:
                problems.append(
                    f"{where}: no_metric_behavior must be green/yellow/red — see docs/adr/0006"
                )
            if not isinstance(cfg.get("threshold"), (int, float)):
                problems.append(f"{where}: threshold must be numeric")
            if not cfg.get("description"):
                problems.append(f"{where}: description is required")

            floor = cfg.get("yellow_floor")
            if floor is not None:
                if direction == "binary":
                    problems.append(f"{where}: a binary gate cannot have a yellow_floor")
                elif direction == "higher_is_better" and floor >= cfg["threshold"]:
                    problems.append(
                        f"{where}: yellow_floor must be below threshold when higher is better"
                    )
                elif direction == "lower_is_better" and floor <= cfg["threshold"]:
                    problems.append(
                        f"{where}: yellow_floor must be above threshold when lower is better"
                    )

    for transition in PRESELL_GATED_TRANSITIONS:
        from_stage, to_stage = transition
        if next_stage(from_stage) != to_stage:
            problems.append(
                f"PRESELL_GATED_TRANSITIONS {from_stage}->{to_stage} is not a real ladder step"
            )

    if not PRESELL_ACCEPTED_KINDS:
        problems.append("PRESELL_ACCEPTED_KINDS is empty — the presell gate could never pass")
    unknown_kinds = PRESELL_ACCEPTED_KINDS - PRESELL_KNOWN_KINDS
    if unknown_kinds:
        problems.append(
            f"PRESELL_ACCEPTED_KINDS has kinds missing from PRESELL_KNOWN_KINDS: "
            f"{', '.join(sorted(unknown_kinds))}"
        )

    if AUTO_DOUBLE_MULTIPLIER < 2:
        problems.append("AUTO_DOUBLE_MULTIPLIER below 2 is not a doubling")
    if AUTO_DOUBLE_MIN_SAMPLE < 1:
        problems.append("AUTO_DOUBLE_MIN_SAMPLE must be >=1 or the rate is meaningless")
    if AUTO_DOUBLE_COOLDOWN_DAYS < 1:
        problems.append(
            "AUTO_DOUBLE_COOLDOWN_DAYS below 1 allows repeated same-day doublings "
            "on a warming domain"
        )

    return problems
