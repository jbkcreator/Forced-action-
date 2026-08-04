"""
Lesson-hygiene sweep (LEARN-v2.2 Layer 4, Step 12).

Garbage collection for the fleet's learned memory. Walks `lifecycle_playbook`
and routes each lesson to one of the two terminal states the existing tools
can express — `mark_contradicted` (proven wrong) or `supersede_recommendation`
(replaced by a newer version) — or to a named, reported non-action.

WHY THIS IS WRITTEN DEFENSIVELY
-------------------------------
A GC bug does not fail loudly. When it wrongly collects, nothing breaks — the
fleet just quietly gets dumber, and it surfaces weeks later as "why did we
stop using the approach that worked?". Under-flagging is a mild annoyance;
over-flagging destroys accumulated learning. Every ambiguous case therefore
resolves to "do nothing and say so".

Three structural rails, in evaluation order:

  1. Schema precondition. The shared DB's live CHECK on
     lifecycle_playbook.status permits four values; 'contradicted' is not one
     of them, so mark_contradicted would raise. Verified up front so an
     un-migrated environment reports a refusal instead of crashing.
  2. Global feed health. A broken evidence feed is indistinguishable from
     universal staleness at the per-lesson level. Checked once, globally,
     before any per-lesson number is trusted.
  3. Blast radius. A run that wants to retire most of the corpus is a bug
     report, not a result — it mutates nothing and alerts.

WHAT THE TOOLS' SIGNATURES DECIDED
----------------------------------
`supersede_recommendation(session, old_id, new_id)` takes a mandatory,
FK-enforced `new_id`. A timer cannot call it, because a timer has no
successor to name. So supersession is successor-driven and age is only a
filter — the signature, not a preference, settled that.

Neither tool writes an actor, a reason, or a timestamp beyond bumping
`updated_at` (which any other write also bumps). So forensics are entirely
this module's responsibility: every action writes an `agent_decisions` row
carrying the counts, the window, and the threshold set in force.

Neither tool writes `confidence`, so v1 is hard cliffs. Graduated decay would
require this module to own the score, which is well past "call the tools at
the right time" — see docs/adr/0034.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.learning_hygiene import (
    ACTING_VERDICTS,
    CONTRADICTION_AUTONOMY_CLASSES,
    CONTRADICTION_MIN_COUNT,
    CONTRADICTION_MIN_RATE_PCT,
    CONTRADICTION_TERMINAL_STATUSES,
    CONTRADICTION_WINDOW_DAYS,
    EMERGENCY_MIN_SAMPLE,
    EMERGENCY_RATE_PCT,
    FEED_MAX_SILENCE_DAYS,
    HYGIENE_ACTIONABLE_STATUSES,
    HYGIENE_EXCLUDED_KINDS,
    HYGIENE_GRAPH_NAME,
    HYGIENE_MEASURABLE_DOMAINS,
    MIN_INSTANCES_FOR_VERDICT,
    NON_EVIDENCE_AUTONOMY_CLASSES,
    NON_EVIDENCE_TERMINAL_STATUSES,
    REQUIRED_LESSON_COLUMNS,
    REQUIRED_STATUS_VALUES,
    RUN_BLAST_RADIUS_EXCEEDED,
    RUN_EVIDENCE_UNAVAILABLE,
    RUN_OK,
    RUN_SCHEMA_NOT_READY,
    SUCCESSOR_MIN_AGE_GAP_HOURS,
    SUPPORT_AUTONOMY_CLASSES,
    SUPPORT_TERMINAL_STATUSES,
    VERDICT_CONTRADICT,
    VERDICT_KEEP,
    VERDICT_REPORT_STALE,
    VERDICT_SKIP_EXCLUDED_KIND,
    VERDICT_SKIP_NOT_ACTIONABLE,
    VERDICT_SKIP_ORPHANED_SOURCE,
    VERDICT_SKIP_UNMEASURABLE,
    VERDICT_SKIP_UNTESTED,
    VERDICT_SUPERSEDE,
    blast_radius_cap,
    config_snapshot,
    staleness_days,
)
from src.services.playbook_writer import mark_contradicted, supersede_recommendation

logger = logging.getLogger(__name__)


# Source types whose source_id can be resolved to a real source row. A type
# absent from this map is NOT treated as orphaned — unresolvable-to-check is
# not evidence of orphanhood, and defaulting to "orphaned" would flag every
# self_healing_kill lesson (whose source_id is a metric name, not a row).
_RESOLVABLE_SOURCE_TYPES: tuple[str, ...] = ("ab_test", "holdout_test")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    """Normalise to tz-aware UTC. Naive DB timestamps are read as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


# ── Value objects ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SchemaReadiness:
    """Whether the LEARN Layer 4 migration has been applied here."""

    ready: bool
    missing_columns: tuple[str, ...] = ()
    missing_status_values: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "missing_columns": list(self.missing_columns),
            "missing_status_values": list(self.missing_status_values),
        }


@dataclass(frozen=True)
class FeedHealth:
    """Global health of the evidence feed (agent_decisions.playbook_id).

    `state` distinguishes a feed that has never carried a row
    ('no_evidence_yet' — the expected state until agents start attributing
    decisions to lessons) from one that carried rows and went quiet
    ('feed_stale' — an outage, and the case that would otherwise look like
    every lesson simultaneously going stale).
    """

    state: str
    linked_rows: int
    last_linked_at: Optional[datetime]

    @property
    def usable(self) -> bool:
        return self.state == "ok"

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "linked_rows": self.linked_rows,
            "last_linked_at": self.last_linked_at.isoformat() if self.last_linked_at else None,
        }


@dataclass(frozen=True)
class LessonStats:
    """Everything decide() is allowed to look at. No DB handle, no clock."""

    lesson_id: int
    name: str
    status: str
    entry_kind: str
    agent_domain: str
    source_type: Optional[str]
    source_id: Optional[str]
    authored_at: datetime
    confidence: Optional[int]
    source_resolved: bool
    contradictions: int
    supports: int
    non_evidence: int
    last_evidence_at: Optional[datetime]
    successor_id: Optional[int]

    @property
    def measured(self) -> int:
        """Decisions that produced a real outcome.

        Non-evidence rows (execution failures, escalations, still-running)
        are excluded from BOTH sides: counting them as contradictions lets an
        outage retire the corpus, counting them as support dilutes real
        contradiction rates toward zero and makes the rule unreachable.
        """
        return self.contradictions + self.supports

    @property
    def contradiction_rate_pct(self) -> float:
        if self.measured == 0:
            return 0.0
        return round(self.contradictions * 100.0 / self.measured, 2)


@dataclass(frozen=True)
class Verdict:
    verdict: str
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def acts(self) -> bool:
        return self.verdict in ACTING_VERDICTS


@dataclass
class HygieneReport:
    run_status: str
    dry_run: bool
    schema: SchemaReadiness
    feed: FeedHealth
    counts: dict[str, int] = field(default_factory=dict)
    actions: list[dict[str, Any]] = field(default_factory=list)
    measurable_population: int = 0
    cap: int = 0
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_status": self.run_status,
            "dry_run": self.dry_run,
            "schema": self.schema.as_dict(),
            "feed": self.feed.as_dict(),
            "counts": dict(sorted(self.counts.items())),
            "actions": self.actions,
            "measurable_population": self.measurable_population,
            "cap": self.cap,
            "notes": self.notes,
        }


# ── Rail 1: schema precondition ──────────────────────────────────────────


def check_schema_readiness(db: Session) -> SchemaReadiness:
    """Verify the LEARN Layer 4 migration ran here.

    Without it `mark_contradicted` raises a CHECK violation, which would
    surface as this job's bug rather than as a missing migration.
    """
    present = {
        r[0]
        for r in db.execute(sa_text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'lifecycle_playbook'
        """))
    }
    missing_cols = tuple(c for c in REQUIRED_LESSON_COLUMNS if c not in present)

    constraint = db.execute(sa_text("""
        SELECT pg_get_constraintdef(oid) AS cdef
        FROM pg_constraint
        WHERE conrelid = 'lifecycle_playbook'::regclass
          AND contype = 'c'
          AND conname = 'check_lifecycle_playbook_status'
    """)).scalar()
    cdef = constraint or ""
    missing_status = tuple(v for v in REQUIRED_STATUS_VALUES if f"'{v}'" not in cdef)

    return SchemaReadiness(
        ready=not missing_cols and not missing_status,
        missing_columns=missing_cols,
        missing_status_values=missing_status,
    )


# ── Rail 2: global feed health ───────────────────────────────────────────


def check_feed_health(db: Session, *, now: Optional[datetime] = None) -> FeedHealth:
    """Is the evidence feed trustworthy enough to read per-lesson numbers?

    Excludes this job's own audit rows — they carry `playbook_id`, so
    counting them would make the feed look healthy purely because the sweep
    has run before.
    """
    now = now or _utcnow()
    row = db.execute(sa_text("""
        SELECT count(*) AS linked, max(started_at) AS last_at
        FROM agent_decisions
        WHERE playbook_id IS NOT NULL
          AND graph_name <> :graph
    """), {"graph": HYGIENE_GRAPH_NAME}).first()

    linked = int(row.linked or 0)
    last_at = _aware(row.last_at)

    if linked == 0:
        return FeedHealth("no_evidence_yet", 0, None)
    if last_at is not None and now - last_at > timedelta(days=FEED_MAX_SILENCE_DAYS):
        return FeedHealth("feed_stale", linked, last_at)
    return FeedHealth("ok", linked, last_at)


# ── The decision, pure ───────────────────────────────────────────────────


def decide(stats: LessonStats, *, now: datetime) -> Verdict:
    """Route one lesson to a verdict. No DB, no ambient clock, no I/O.

    Order is load-bearing. Contradiction is evaluated before staleness
    because a lesson can be both, and superseding first would attach the
    stronger, more informative evidence to an already-retired row.
    """
    base: dict[str, Any] = {
        "contradictions": stats.contradictions,
        "supports": stats.supports,
        "non_evidence": stats.non_evidence,
        "measured": stats.measured,
        "contradiction_rate_pct": stats.contradiction_rate_pct,
        "window_days": CONTRADICTION_WINDOW_DAYS,
    }

    # Cheap structural exclusions first — no point computing evidence for a
    # lesson the tools would refuse anyway.
    if stats.status not in HYGIENE_ACTIONABLE_STATUSES:
        return Verdict(
            VERDICT_SKIP_NOT_ACTIONABLE,
            f"status {stats.status!r} is already terminal for both tools",
            base,
        )

    if stats.entry_kind in HYGIENE_EXCLUDED_KINDS:
        return Verdict(
            VERDICT_SKIP_EXCLUDED_KIND,
            f"entry_kind {stats.entry_kind!r} excluded — counter-evidence "
            f"against a documented failure means the failure stopped "
            f"reproducing, which is obsolescence, not falsity",
            base,
        )

    if stats.agent_domain not in HYGIENE_MEASURABLE_DOMAINS:
        return Verdict(
            VERDICT_SKIP_UNMEASURABLE,
            f"agent_domain {stats.agent_domain!r} has no evidence feed — "
            f"an age rule would flag 100% of these on every run",
            base,
        )

    if not stats.source_resolved:
        return Verdict(
            VERDICT_SKIP_ORPHANED_SOURCE,
            f"source_id {stats.source_id!r} resolves to no "
            f"{stats.source_type!r} row — unverifiable by construction",
            base,
        )

    # Contradiction — before staleness, deliberately.
    if stats.measured >= EMERGENCY_MIN_SAMPLE and stats.contradiction_rate_pct >= EMERGENCY_RATE_PCT:
        return Verdict(
            VERDICT_CONTRADICT,
            f"emergency cliff: {stats.contradiction_rate_pct}% of "
            f"{stats.measured} measured outcomes disagree "
            f"(>= {EMERGENCY_RATE_PCT}% on >= {EMERGENCY_MIN_SAMPLE})",
            base,
        )

    if stats.measured < MIN_INSTANCES_FOR_VERDICT:
        return Verdict(
            VERDICT_SKIP_UNTESTED,
            f"{stats.measured} measured outcome(s) — below "
            f"MIN_INSTANCES_FOR_VERDICT={MIN_INSTANCES_FOR_VERDICT}; untested "
            f"is not wrong",
            base,
        )

    if stats.contradictions >= CONTRADICTION_MIN_COUNT:
        if stats.contradiction_rate_pct >= CONTRADICTION_MIN_RATE_PCT:
            return Verdict(
                VERDICT_CONTRADICT,
                f"{stats.contradictions} contradicting outcome(s) in "
                f"{CONTRADICTION_WINDOW_DAYS}d (>= {CONTRADICTION_MIN_COUNT}), "
                f"rate {stats.contradiction_rate_pct}%",
                base,
            )
        return Verdict(
            VERDICT_KEEP,
            f"{stats.contradictions} contradictions met the count but rate "
            f"{stats.contradiction_rate_pct}% is below "
            f"CONTRADICTION_MIN_RATE_PCT={CONTRADICTION_MIN_RATE_PCT}%",
            base,
        )

    # Supersession — successor-driven, because the tool demands a new_id.
    if stats.successor_id is not None:
        return Verdict(
            VERDICT_SUPERSEDE,
            f"lesson {stats.successor_id} covers the same declared scope and "
            f"is newer",
            {**base, "successor_id": stats.successor_id},
        )

    # Staleness — report only. "expired = unknown because stale", never wrong.
    max_age = staleness_days(stats.source_type)
    reference = stats.last_evidence_at or _aware(stats.authored_at)
    age_days = (now - reference).days if reference else None
    if age_days is not None and age_days > max_age:
        return Verdict(
            VERDICT_REPORT_STALE,
            f"no supporting evidence for {age_days}d (limit {max_age}d) and no "
            f"successor exists — reported, not retired: old is not wrong, and "
            f"retiring it would leave no guidance where imperfect guidance stood",
            {**base, "age_days": age_days, "staleness_limit_days": max_age},
        )

    return Verdict(
        VERDICT_KEEP,
        f"{stats.supports} supporting vs {stats.contradictions} contradicting "
        f"outcome(s); within freshness limit",
        base,
    )


# ── Data access ──────────────────────────────────────────────────────────


_LESSON_QUERY = sa_text("""
WITH classified AS (
    -- Each decision lands in EXACTLY ONE bucket. A CASE rather than three
    -- independent FILTERs because overlapping predicates double-count: a
    -- 'failed' decision whose autonomy_class is 'autonomous' matched both
    -- non-evidence AND support under an OR, so an infrastructure outage read
    -- as a stream of confirmations.
    --
    -- Precedence is deliberate:
    --   1. An explicit human reversal is unambiguous counter-evidence.
    --   2. Non-evidence outranks contradiction. A decision that never
    --      produced an outcome must not be held against the lesson on any
    --      other dimension — this is the rail that stops an outage from
    --      retiring the corpus.
    --   3. Support is checked last, and anything unclassified falls through
    --      to non-evidence (default deny), so a value added to either CHECK
    --      constraint later cannot silently become evidence.
    SELECT d.playbook_id,
           d.started_at,
           CASE
               WHEN d.overridden_at IS NOT NULL THEN 'contradiction'
               WHEN d.terminal_status IS NULL
                    OR d.terminal_status = ANY(CAST(:non_evidence_ts AS text[]))
                    OR d.autonomy_class  = ANY(CAST(:non_evidence_ac AS text[]))
                   THEN 'non_evidence'
               WHEN d.terminal_status = ANY(CAST(:contradiction_ts AS text[]))
                    OR d.autonomy_class  = ANY(CAST(:contradiction_ac AS text[]))
                   THEN 'contradiction'
               WHEN d.terminal_status = ANY(CAST(:support_ts AS text[]))
                    OR d.autonomy_class  = ANY(CAST(:support_ac AS text[]))
                   THEN 'support'
               ELSE 'non_evidence'
           END AS bucket
    FROM agent_decisions d
    WHERE d.playbook_id IS NOT NULL
      AND d.graph_name <> :hygiene_graph
),
windowed AS (
    SELECT playbook_id,
           count(*) FILTER (WHERE bucket = 'contradiction') AS contradictions,
           count(*) FILTER (WHERE bucket = 'support')       AS supports,
           count(*) FILTER (WHERE bucket = 'non_evidence')  AS non_evidence
    FROM classified
    WHERE started_at >= :window_start
    GROUP BY playbook_id
),
all_time AS (
    -- Staleness measures time since the last SUPPORTING outcome, all-time.
    -- Contradictions and failures must not reset the freshness clock: a
    -- lesson failing repeatedly is not thereby fresh.
    SELECT playbook_id, max(started_at) AS last_evidence_at
    FROM classified
    WHERE bucket = 'support'
    GROUP BY playbook_id
)
SELECT p.id,
       p.name,
       p.status,
       p.entry_kind,
       p.agent_domain,
       p.source_type,
       p.source_id,
       p.authored_at,
       p.confidence,
       COALESCE(w.contradictions, 0) AS contradictions,
       COALESCE(w.supports, 0)       AS supports,
       COALESCE(w.non_evidence, 0)   AS non_evidence,
       a.last_evidence_at,
       succ.id AS successor_id,
       CASE
           WHEN p.source_type = ANY(CAST(:resolvable_types AS text[]))
               THEN EXISTS (SELECT 1 FROM ab_tests t WHERE t.test_name = p.source_id)
           ELSE true
       END AS source_resolved
FROM lifecycle_playbook p
LEFT JOIN windowed w ON w.playbook_id = p.id
LEFT JOIN all_time a ON a.playbook_id = p.id
LEFT JOIN LATERAL (
    SELECT s.id
    FROM lifecycle_playbook s
    WHERE s.id <> p.id
      AND s.agent_domain = p.agent_domain
      AND s.entry_kind   = p.entry_kind
      AND s.status       = ANY(CAST(:actionable_statuses AS text[]))
      AND p.scope IS NOT NULL
      AND s.scope IS NOT NULL
      AND s.scope = p.scope
      AND s.authored_at > p.authored_at + (CAST(:gap_hours AS int) * interval '1 hour')
      AND s.superseded_by_id IS DISTINCT FROM p.id
    ORDER BY s.authored_at DESC
    LIMIT 1
) succ ON true
ORDER BY p.id
""")


def load_lesson_stats(db: Session, *, now: Optional[datetime] = None) -> list[LessonStats]:
    """One aggregate query for the whole corpus — never a query per lesson."""
    now = now or _utcnow()
    rows = db.execute(_LESSON_QUERY, {
        "contradiction_ts": list(CONTRADICTION_TERMINAL_STATUSES),
        "contradiction_ac": list(CONTRADICTION_AUTONOMY_CLASSES),
        "support_ts": list(SUPPORT_TERMINAL_STATUSES),
        "support_ac": list(SUPPORT_AUTONOMY_CLASSES),
        "non_evidence_ts": list(NON_EVIDENCE_TERMINAL_STATUSES),
        "non_evidence_ac": list(NON_EVIDENCE_AUTONOMY_CLASSES),
        "hygiene_graph": HYGIENE_GRAPH_NAME,
        "window_start": now - timedelta(days=CONTRADICTION_WINDOW_DAYS),
        "resolvable_types": list(_RESOLVABLE_SOURCE_TYPES),
        "actionable_statuses": list(HYGIENE_ACTIONABLE_STATUSES),
        "gap_hours": SUCCESSOR_MIN_AGE_GAP_HOURS,
    }).fetchall()

    return [
        LessonStats(
            lesson_id=int(r.id),
            name=r.name,
            status=r.status,
            entry_kind=r.entry_kind,
            agent_domain=r.agent_domain,
            source_type=r.source_type,
            source_id=r.source_id,
            authored_at=_aware(r.authored_at),
            confidence=r.confidence,
            source_resolved=bool(r.source_resolved),
            contradictions=int(r.contradictions),
            supports=int(r.supports),
            non_evidence=int(r.non_evidence),
            last_evidence_at=_aware(r.last_evidence_at),
            successor_id=int(r.successor_id) if r.successor_id is not None else None,
        )
        for r in rows
    ]


# ── Audit ────────────────────────────────────────────────────────────────


def _write_audit(
    db: Session,
    stats: LessonStats,
    verdict: Verdict,
    *,
    actor: str,
    applied: bool,
    now: datetime,
) -> str:
    """Record the numbers that justified an action.

    Neither tool stores an actor, a reason, or a terminal timestamp, so
    without this row "why did this lesson die?" cannot be answered — and the
    first false positive would destroy trust in the whole job.

    Written with graph_name=HYGIENE_GRAPH_NAME, which load_lesson_stats
    filters out of the evidence feed. Skipping that filter would make each
    run's audit row supporting evidence for the lesson it just judged.
    """
    decision_id = str(uuid.uuid4())
    db.execute(sa_text("""
        INSERT INTO agent_decisions (
            decision_id, graph_name, event_type, started_at, completed_at,
            terminal_status, tokens_used, cost_usd, summary,
            autonomy_class, was_autonomous, requires_approval, playbook_id
        ) VALUES (
            :decision_id, :graph_name, :event_type, :now, :now,
            'completed', 0, 0, CAST(:summary AS jsonb),
            'autonomous', true, false, :playbook_id
        )
    """), {
        "decision_id": decision_id,
        "graph_name": HYGIENE_GRAPH_NAME,
        "event_type": verdict.verdict,
        "now": now,
        "summary": _json_dumps({
            "lesson_id": stats.lesson_id,
            "lesson_name": stats.name,
            "verdict": verdict.verdict,
            "reason": verdict.reason,
            "evidence": verdict.evidence,
            "applied": applied,
            "actor": actor,
            "thresholds": config_snapshot(),
        }),
        "playbook_id": stats.lesson_id,
    })
    return decision_id


def _json_dumps(payload: dict[str, Any]) -> str:
    """default=str so a datetime in the evidence bag can never break an audit write."""
    return json.dumps(payload, default=str)


# ── The sweep ────────────────────────────────────────────────────────────


def sweep(
    db: Session,
    *,
    now: Optional[datetime] = None,
    dry_run: bool = True,
    limit: Optional[int] = None,
    actor: str = "learning_hygiene_sweep",
) -> HygieneReport:
    """Evaluate every lesson; mutate only what the rails permit.

    dry_run defaults to True. This job mutates learned state and neither tool
    has an inverse, so running it live by default is the one avoidable
    mistake.
    """
    now = now or _utcnow()

    schema = check_schema_readiness(db)
    feed = check_feed_health(db, now=now)

    if not schema.ready:
        logger.error(
            "[LessonHygiene] schema not ready — missing columns=%s status values=%s; "
            "run migrations/apply_lifecycle_playbook_lessons_versioning.py",
            schema.missing_columns, schema.missing_status_values,
        )
        return HygieneReport(
            run_status=RUN_SCHEMA_NOT_READY,
            dry_run=dry_run,
            schema=schema,
            feed=feed,
            notes=[
                "mark_contradicted would raise a CHECK violation here — "
                "refusing to touch anything.",
            ],
        )

    stats_list = load_lesson_stats(db, now=now)
    verdicts: list[tuple[LessonStats, Verdict]] = [
        (s, decide(s, now=now)) for s in stats_list
    ]

    counts: dict[str, int] = {}
    for _, v in verdicts:
        counts[v.verdict] = counts.get(v.verdict, 0) + 1

    # Denominator is the measurable population only — unmeasurable, excluded
    # and orphaned lessons must not inflate the budget for acting on lessons
    # that actually have evidence.
    unmeasurable_verdicts = {
        VERDICT_SKIP_UNMEASURABLE, VERDICT_SKIP_EXCLUDED_KIND,
        VERDICT_SKIP_ORPHANED_SOURCE, VERDICT_SKIP_NOT_ACTIONABLE,
    }
    measurable = sum(1 for _, v in verdicts if v.verdict not in unmeasurable_verdicts)
    cap = blast_radius_cap(measurable)

    report = HygieneReport(
        run_status=RUN_OK,
        dry_run=dry_run,
        schema=schema,
        feed=feed,
        counts=counts,
        measurable_population=measurable,
        cap=cap,
    )

    acting = [(s, v) for s, v in verdicts if v.acts]

    if not feed.usable and acting:
        # Cannot happen with the current vocabulary (no feed rows means no
        # contradictions), but a future signal that does not read the feed
        # must not slip past the rail.
        logger.warning(
            "[LessonHygiene] feed state=%s — refusing %d action(s)",
            feed.state, len(acting),
        )
        report.run_status = RUN_EVIDENCE_UNAVAILABLE
        report.notes.append(
            f"evidence feed state={feed.state}; {len(acting)} action(s) withheld"
        )
        return report

    if len(acting) > cap:
        logger.error(
            "[LessonHygiene] blast radius exceeded: %d action(s) > cap %d "
            "(measurable population %d) — mutating nothing",
            len(acting), cap, measurable,
        )
        report.run_status = RUN_BLAST_RADIUS_EXCEEDED
        report.notes.append(
            f"{len(acting)} action(s) exceeds cap {cap} over {measurable} "
            f"measurable lessons — a run this large is a bug report, not a "
            f"result. Nothing was changed."
        )
        return report

    if limit is not None:
        withheld = max(0, len(acting) - limit)
        if withheld:
            report.notes.append(f"--limit {limit}: {withheld} action(s) not attempted")
        acting = acting[:limit]

    for stats, verdict in acting:
        report.actions.append(_apply(db, stats, verdict, dry_run=dry_run, actor=actor, now=now))

    for stats, verdict in verdicts:
        if verdict.verdict == VERDICT_REPORT_STALE:
            logger.info(
                "[LessonHygiene] lesson %d %r stale: %s",
                stats.lesson_id, stats.name, verdict.reason,
            )

    logger.info(
        "[LessonHygiene] run_status=%s dry_run=%s counts=%s measurable=%d cap=%d "
        "feed=%s",
        report.run_status, dry_run, counts, measurable, cap, feed.state,
    )
    return report


def _apply(
    db: Session,
    stats: LessonStats,
    verdict: Verdict,
    *,
    dry_run: bool,
    actor: str,
    now: datetime,
) -> dict[str, Any]:
    """Re-check under lock, then call the tool. One savepoint per lesson.

    The learning system writes decisions while this sweep reads, so a lesson
    can gain a confirming outcome between the aggregate read and this write.
    Re-reading the row FOR UPDATE and re-verifying the status closes that gap;
    a savepoint keeps one lesson's failure from aborting the whole run's
    transaction.
    """
    outcome: dict[str, Any] = {
        "lesson_id": stats.lesson_id,
        "lesson_name": stats.name,
        "verdict": verdict.verdict,
        "reason": verdict.reason,
        "evidence": verdict.evidence,
        "applied": False,
    }

    if dry_run:
        outcome["skipped_reason"] = "dry_run"
        logger.info(
            "[LessonHygiene] DRY-RUN would %s lesson %d %r — %s",
            verdict.verdict, stats.lesson_id, stats.name, verdict.reason,
        )
        return outcome

    try:
        with db.begin_nested():
            locked = db.execute(sa_text("""
                SELECT id, status FROM lifecycle_playbook
                WHERE id = :id
                FOR UPDATE SKIP LOCKED
            """), {"id": stats.lesson_id}).first()

            if locked is None:
                outcome["skipped_reason"] = "row_locked_or_gone"
                return outcome

            if locked.status not in HYGIENE_ACTIONABLE_STATUSES:
                outcome["skipped_reason"] = f"status changed to {locked.status!r}"
                return outcome

            if verdict.verdict == VERDICT_CONTRADICT:
                changed = mark_contradicted(db, stats.lesson_id)
            else:
                successor = verdict.evidence.get("successor_id")
                if successor is None:
                    outcome["skipped_reason"] = "no successor id on verdict"
                    return outcome
                changed = supersede_recommendation(db, stats.lesson_id, int(successor))

            outcome["applied"] = bool(changed)
            if not changed:
                # Both tools guard on status, so False means someone else got
                # there first. Idempotent by construction.
                outcome["skipped_reason"] = "tool reported no change (already marked)"

            outcome["decision_id"] = _write_audit(
                db, stats, verdict, actor=actor, applied=bool(changed), now=now,
            )
    except Exception as exc:
        logger.error(
            "[LessonHygiene] failed to apply %s to lesson %d: %s",
            verdict.verdict, stats.lesson_id, exc, exc_info=True,
        )
        outcome["error"] = str(exc)
        return outcome

    if outcome["applied"]:
        logger.info(
            "[LessonHygiene] %s lesson %d %r — %s",
            verdict.verdict, stats.lesson_id, stats.name, verdict.reason,
        )
    return outcome
