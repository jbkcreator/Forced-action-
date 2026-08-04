"""
Lesson-hygiene sweep thresholds (LEARN-v2.2 Layer 4, Step 12).

Pure configuration for src/services/learning_hygiene.py. No DB, no imports
from src/ — same standalone contract as config/venture_ladder.py and
config/scoring.py.

WHY THESE NUMBERS ARE NOT FREELY TUNABLE
----------------------------------------
`CONTRADICTION_MIN_COUNT = 3` is not a preference. The fleet constitutions
(docs/constitutions/vera.md, cora.md, hunter.md — identical text) state
"Playbooks at 3+ proofs; anti-playbooks at 3+ failures; inherited at birth",
and src/services/playbook_writer.py:mark_contradicted cites that rule as its
authority. Changing 3 amends a constitution; it is not a config tweak.

The same source gives "recent outweighs old" (correction taxonomy), which is
why the count is windowed (CONTRADICTION_WINDOW_DAYS) rather than all-time —
three failures last week and three failures spread over a year are not the
same evidence.

WHY STALENESS DOES NOT REUSE VERA'S FRESHNESS TABLE
---------------------------------------------------
src/agents/vera/config.py:FRESHNESS_MAX_AGE_HOURS (revenue 24h / deed 90d /
market 30d) governs *facts*, not lessons. Applying revenue-24h to a lesson
would expire it daily, which is meaningless — a lesson is a conclusion drawn
*from* facts and outlives any single observation. src.agents.vera.facts.
is_stale is also unusable here for a second reason: it calls
datetime.now(timezone.utc) internally, so it cannot be driven by an injected
clock and cannot be unit-tested at a boundary. Staleness here is therefore
its own per-source_type table with a stated default.

The constitutions' "expired = 'unknown because stale'" is honoured
structurally: staleness never produces a mutation. See VERDICT_REPORT_STALE.
"""
from __future__ import annotations

from typing import Any

# ── Identity ──────────────────────────────────────────────────────────────
# graph_name written on this job's own agent_decisions audit rows.
#
# LOAD-BEARING: agent_decisions.playbook_id is the evidence feed AND the
# audit sink. Without filtering this name out of the evidence query, the
# sweep's own audit row (terminal_status='completed') would count as
# supporting evidence for the very lesson it just retired — a self-poisoning
# feedback loop that grows one support row per run, per lesson.
HYGIENE_GRAPH_NAME = "learning_hygiene"

# ── Verdicts ──────────────────────────────────────────────────────────────
# Only two verdicts mutate. Everything else is named and reported, because
# the two available tools (supersede_recommendation, mark_contradicted) can
# express exactly two terminal states and a lesson can be unhealthy in ways
# neither describes.
VERDICT_CONTRADICT = "contradict"            # mutates: mark_contradicted
VERDICT_SUPERSEDE = "supersede"              # mutates: supersede_recommendation
VERDICT_KEEP = "keep"
VERDICT_REPORT_STALE = "report_stale"        # old != wrong; never mutates
VERDICT_SKIP_UNTESTED = "skip_untested"
VERDICT_SKIP_UNMEASURABLE = "skip_unmeasurable"
VERDICT_SKIP_ORPHANED_SOURCE = "skip_orphaned_source"
VERDICT_SKIP_EXCLUDED_KIND = "skip_excluded_kind"
VERDICT_SKIP_NOT_ACTIONABLE = "skip_not_actionable"

ACTING_VERDICTS: frozenset[str] = frozenset({VERDICT_CONTRADICT, VERDICT_SUPERSEDE})

ALL_VERDICTS: frozenset[str] = frozenset({
    VERDICT_CONTRADICT, VERDICT_SUPERSEDE, VERDICT_KEEP, VERDICT_REPORT_STALE,
    VERDICT_SKIP_UNTESTED, VERDICT_SKIP_UNMEASURABLE,
    VERDICT_SKIP_ORPHANED_SOURCE, VERDICT_SKIP_EXCLUDED_KIND,
    VERDICT_SKIP_NOT_ACTIONABLE,
})

# Run-level outcomes — global refusals, evaluated before any lesson is read.
RUN_SCHEMA_NOT_READY = "schema_not_ready"
RUN_EVIDENCE_UNAVAILABLE = "evidence_unavailable"
RUN_BLAST_RADIUS_EXCEEDED = "blast_radius_exceeded"
RUN_OK = "ok"

# ── Contradiction rule ────────────────────────────────────────────────────
CONTRADICTION_MIN_COUNT = 3          # constitutions; not tunable
CONTRADICTION_WINDOW_DAYS = 30       # "recent outweighs old"

# Disabled by default (0.0) so behaviour matches the constitutions exactly.
# Exists because 3-contradictions-out-of-300 is a lesson worth protecting,
# and a lead may reasonably want that guard on. Enabling it makes the rule
# strictly harder to trip, never easier — it can only spare lessons.
CONTRADICTION_MIN_RATE_PCT = 0.0

# Emergency cliff: overwhelming disagreement retires immediately rather than
# waiting for the windowed count. Both conditions required.
EMERGENCY_RATE_PCT = 70.0
EMERGENCY_MIN_SAMPLE = 20

# Constitutions: "LOCAL until 2+ instances" — below this a lesson has not
# been exercised enough for any verdict, in either direction.
MIN_INSTANCES_FOR_VERDICT = 2

# ── Staleness rule (report-only) ──────────────────────────────────────────
STALENESS_DAYS_DEFAULT = 45
STALENESS_DAYS_BY_SOURCE_TYPE: dict[str, int] = {
    "ab_test": 45,
    "holdout_test": 45,
    "self_healing_kill": 30,   # a kill recommendation ages faster than a copy win
}

# ── Population filters ────────────────────────────────────────────────────
# anti_playbook is excluded rather than inverted. An anti_playbook row is
# already the record of 3+ failures; counter-evidence against it means the
# failure STOPPED reproducing, which makes the warning obsolete, not false —
# a different terminal state, not an inverted threshold. Getting it wrong
# retires a warning and the fleet resumes doing what it learned not to do,
# which is the most expensive false positive available in this corpus.
# Removing this entry is deliberately not sufficient to enable the path:
# decide() has no inverted branch to fall into.
HYGIENE_EXCLUDED_KINDS: tuple[str, ...] = ("anti_playbook",)

# Only 'lifecycle' has a writer today (ab_engine, lifecycle_self_healing,
# lifecycle_holdout_check all leave upsert_recommendation's default). The
# other domains in the agent_domain vocabulary are namespace-reserved
# forward-provisioning with no writer and no evidence feed, so an age rule
# would flag 100% of them on every run, forever — a false positive by
# construction, not a mistuned threshold. Building their feed is a
# precondition for sweeping them, not optional polish.
HYGIENE_MEASURABLE_DOMAINS: tuple[str, ...] = ("lifecycle",)

# What the two tools accept. Both are UPDATE ... WHERE status IN
# ('recommended','adopted'), so anything else is already immune and asking
# is pointless work.
HYGIENE_ACTIONABLE_STATUSES: tuple[str, ...] = ("recommended", "adopted")

# ── Evidence vocabulary (default-deny over a closed set) ──────────────────
# agent_decisions constrains both columns with a CHECK, so these sets are
# exhaustive rather than best-effort. tests assert full coverage against the
# live constraint, so a value added later cannot silently fall through as
# "unclassified" and be quietly ignored.
#
# The critical distinction: a decision that FAILED produced no outcome. It is
# absence of evidence, not evidence against — counting it would let an
# infrastructure outage retire every lesson in the corpus. Non-evidence rows
# are excluded from BOTH numerator and denominator; counting them as support
# would be the mirror error, diluting real contradiction rates toward zero
# and making the gate unreachable.
CONTRADICTION_TERMINAL_STATUSES: frozenset[str] = frozenset({"aborted"})
SUPPORT_TERMINAL_STATUSES: frozenset[str] = frozenset({"completed"})
NON_EVIDENCE_TERMINAL_STATUSES: frozenset[str] = frozenset({"failed", "escalated"})

# A human reversing a decision this lesson drove is the strongest available
# counter-evidence — stronger than any automated metric.
CONTRADICTION_AUTONOMY_CLASSES: frozenset[str] = frozenset({"overridden", "rejected"})
SUPPORT_AUTONOMY_CLASSES: frozenset[str] = frozenset({"autonomous", "approved"})
NON_EVIDENCE_AUTONOMY_CLASSES: frozenset[str] = frozenset(
    {"approval_required", "recommendation_only"}
)

# ── Successor detection (supersede path) ─────────────────────────────────
# source_key is UNIQUE, so two lessons can never share
# (source_type, source_id) — a successor necessarily has a different
# source_id, and there is no column that says "these two lessons are about
# the same thing". `scope` (LEARN's portability dimensions) is the closest
# thing to a subject key, so it is the join.
#
# SAFETY: both scopes must be non-NULL and equal. A NULL scope means "no
# declared subject"; treating two NULL-scoped lessons as the same subject
# would supersede unrelated lessons against each other. Since no caller
# populates scope yet, this path is inert today — reported, not silent.
SUCCESSOR_REQUIRE_SCOPE = True
SUCCESSOR_MIN_AGE_GAP_HOURS = 1

# ── Blast radius ─────────────────────────────────────────────────────────
# A run that wants to retire most of the corpus is a bug report, not a
# result. Percentage alone is useless at small N (5% of 7 rows is 0, so
# nothing could ever be flagged), so an absolute floor carries it now and
# the percentage carries it at scale. The denominator is the MEASURABLE
# population — unmeasurable/excluded/orphaned lessons must not inflate the
# budget for acting on lessons that do have evidence.
BLAST_RADIUS_MIN = 3
BLAST_RADIUS_PCT = 20.0

# ── Feed health ──────────────────────────────────────────────────────────
# A broken evidence feed looks exactly like universal staleness. Checked
# GLOBALLY, before any per-lesson number is trusted. Distinguishes "the feed
# has never carried a row" (no_evidence_yet — expected today) from "the feed
# carried rows and went quiet" (feed_stale — suspicious), because those
# warrant different messages even though both refuse to act.
FEED_MAX_SILENCE_DAYS = 7

# ── Schema preconditions ─────────────────────────────────────────────────
# The shared DB's live CHECK constraint permits exactly
# ('recommended','adopted','rejected','retired'). mark_contradicted writes
# 'contradicted' and would raise a constraint violation. The columns and the
# widened constraint arrive with
# migrations/apply_lifecycle_playbook_lessons_versioning.py, which belongs to
# the LEARN foundations branch's rollout — this job verifies rather than
# applies, so a not-yet-migrated environment gets a reported refusal instead
# of a crash.
REQUIRED_LESSON_COLUMNS: tuple[str, ...] = (
    "confidence", "version", "scope", "superseded_by_id",
)
REQUIRED_STATUS_VALUES: tuple[str, ...] = ("superseded", "contradicted")


def staleness_days(source_type: str | None) -> int:
    """Days without supporting evidence before a lesson is reported stale."""
    if source_type is None:
        return STALENESS_DAYS_DEFAULT
    return STALENESS_DAYS_BY_SOURCE_TYPE.get(source_type, STALENESS_DAYS_DEFAULT)


def blast_radius_cap(measurable_population: int) -> int:
    """Max lessons this run may mutate, over the measurable population only."""
    if measurable_population <= 0:
        return 0
    return max(BLAST_RADIUS_MIN, int(measurable_population * BLAST_RADIUS_PCT / 100))


def validate_hygiene_config() -> list[str]:
    """Machine-check the config's internal consistency.

    Returns a list of problems; empty means valid. Called by a test so a
    hand-edit that makes the rule unreachable fails CI rather than silently
    producing a job that never acts — the ADR 0006 failure mode.
    """
    problems: list[str] = []

    if CONTRADICTION_MIN_COUNT < 1:
        problems.append("CONTRADICTION_MIN_COUNT must be >= 1")
    if CONTRADICTION_MIN_COUNT < MIN_INSTANCES_FOR_VERDICT:
        problems.append(
            "CONTRADICTION_MIN_COUNT < MIN_INSTANCES_FOR_VERDICT — the count "
            "rule could fire on a sample too small to judge"
        )
    if not 0.0 <= CONTRADICTION_MIN_RATE_PCT <= 100.0:
        problems.append("CONTRADICTION_MIN_RATE_PCT must be within 0..100")
    if not 0.0 < EMERGENCY_RATE_PCT <= 100.0:
        problems.append("EMERGENCY_RATE_PCT must be within 0..100")
    if EMERGENCY_RATE_PCT <= CONTRADICTION_MIN_RATE_PCT:
        problems.append(
            "EMERGENCY_RATE_PCT must exceed CONTRADICTION_MIN_RATE_PCT — the "
            "emergency cliff has to be strictly harder to trip than the "
            "ordinary rule, or it is not an emergency"
        )
    if EMERGENCY_MIN_SAMPLE < MIN_INSTANCES_FOR_VERDICT:
        problems.append("EMERGENCY_MIN_SAMPLE < MIN_INSTANCES_FOR_VERDICT")
    if CONTRADICTION_WINDOW_DAYS < 1:
        problems.append("CONTRADICTION_WINDOW_DAYS must be >= 1")
    if STALENESS_DAYS_DEFAULT < CONTRADICTION_WINDOW_DAYS:
        problems.append(
            "STALENESS_DAYS_DEFAULT < CONTRADICTION_WINDOW_DAYS — a lesson "
            "would be reported stale before its evidence window closed"
        )
    for src, days in STALENESS_DAYS_BY_SOURCE_TYPE.items():
        if days < CONTRADICTION_WINDOW_DAYS:
            problems.append(
                f"STALENESS_DAYS_BY_SOURCE_TYPE[{src!r}]={days} is shorter "
                f"than CONTRADICTION_WINDOW_DAYS"
            )

    # Signal vocabularies must partition, not overlap — a value classed as
    # both support and contradiction would make the rate meaningless.
    for label, sets in (
        ("terminal_status", (
            CONTRADICTION_TERMINAL_STATUSES,
            SUPPORT_TERMINAL_STATUSES,
            NON_EVIDENCE_TERMINAL_STATUSES,
        )),
        ("autonomy_class", (
            CONTRADICTION_AUTONOMY_CLASSES,
            SUPPORT_AUTONOMY_CLASSES,
            NON_EVIDENCE_AUTONOMY_CLASSES,
        )),
    ):
        seen: set[str] = set()
        for s in sets:
            overlap = seen & s
            if overlap:
                problems.append(
                    f"{label} vocabularies overlap on {sorted(overlap)}"
                )
            seen |= s

    if BLAST_RADIUS_MIN < 1:
        problems.append("BLAST_RADIUS_MIN must be >= 1")
    if not 0.0 < BLAST_RADIUS_PCT <= 100.0:
        problems.append("BLAST_RADIUS_PCT must be within 0..100")
    if not HYGIENE_MEASURABLE_DOMAINS:
        problems.append(
            "HYGIENE_MEASURABLE_DOMAINS is empty — the sweep could never act"
        )
    if not HYGIENE_ACTIONABLE_STATUSES:
        problems.append(
            "HYGIENE_ACTIONABLE_STATUSES is empty — the sweep could never act"
        )
    if not ACTING_VERDICTS <= ALL_VERDICTS:
        problems.append("ACTING_VERDICTS contains a verdict missing from ALL_VERDICTS")

    return problems


def config_snapshot() -> dict[str, Any]:
    """The threshold set in force, frozen into every audit row.

    Without this, "why did this lesson die?" is unanswerable after someone
    edits a threshold — and the first false positive destroys trust in the
    whole job.
    """
    return {
        "contradiction_min_count": CONTRADICTION_MIN_COUNT,
        "contradiction_window_days": CONTRADICTION_WINDOW_DAYS,
        "contradiction_min_rate_pct": CONTRADICTION_MIN_RATE_PCT,
        "emergency_rate_pct": EMERGENCY_RATE_PCT,
        "emergency_min_sample": EMERGENCY_MIN_SAMPLE,
        "min_instances_for_verdict": MIN_INSTANCES_FOR_VERDICT,
        "staleness_days_default": STALENESS_DAYS_DEFAULT,
        "excluded_kinds": list(HYGIENE_EXCLUDED_KINDS),
        "measurable_domains": list(HYGIENE_MEASURABLE_DOMAINS),
        "blast_radius_min": BLAST_RADIUS_MIN,
        "blast_radius_pct": BLAST_RADIUS_PCT,
    }
