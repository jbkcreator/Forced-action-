"""
Autonomous venture ladder — CLONE-v2.2 / CL4.

The decision layer over `ventures`. CL3 made a second venture configurable;
this module makes advancing one a scored, evidence-backed, auditable step:

    evaluate()              compute every gate for a venture's current rung
    advance()               move up one rung, refusing on any red gate
    presell_gate_blocked()  demand evidence check, run before spend
    maybe_auto_double()     scale the venture's daily ceiling on a high reply rate
    maybe_auto_double_cell()  same rule at cell granularity
    cell_reply_rates()      per-cell sends/replies over a window
    record_evidence()       write one evidence row, idempotently

Returns data; it does not notify. src/tasks/venture_ladder_evaluator.py is the
cron driver that decides to act and posts to Slack, mirroring how
county_launch_evaluator.py sits above the expansion gates. Config lives in
config/venture_ladder.py — thresholds are never inlined here.

`blocked_reasons: list[str]`, empty meaning permitted, is deliberately the
same contract src/services/icp_launch_gate.py:icp_launch_blocked() already
uses, so reviewers do not have to learn a second idiom.

TWO THINGS THIS MODULE IS CAREFUL ABOUT

1. Gate colour for a missing metric comes from the gate's own
   `no_metric_behavior`, never a blanket red. docs/adr/0006 records what
   happens otherwise: the existing expansion-gate machine treats None as red,
   requires all-green, and has therefore never once permitted a launch.

2. Relay readiness reads the `ventures` ROW, not the resolved VentureConfig.
   The CL3 resolver falls back to config/settings.py for any NULL column, so a
   brand-new venture with no Instantly campaign of its own resolves to venture
   #1's campaign and would look ready while being able to send only into
   another business's sequence. Gates must see the unresolved truth.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.venture_ladder import (
    AUTO_DOUBLE_CELL_MAX_MULTIPLIER,
    AUTO_DOUBLE_COOLDOWN_DAYS,
    AUTO_DOUBLE_MAX_CEILING,
    AUTO_DOUBLE_MAX_SEND_FAILURE_PCT,
    AUTO_DOUBLE_MIN_SAMPLE,
    AUTO_DOUBLE_MULTIPLIER,
    AUTO_DOUBLE_REPLY_RATE_PCT,
    AUTO_DOUBLE_WINDOW_DAYS,
    CELL_MIN_SAMPLE,
    CELL_REPLY_RATE_FLOOR_PCT,
    EVIDENCE_HARNESS_RESULT,
    EVIDENCE_MARKET_SCORE,
    EVIDENCE_PRESELL_COMMITMENT,
    EVIDENCE_SCRAPE_SAMPLE,
    METRIC_WINDOW_DAYS,
    MIN_COST_ATTRIBUTION_RATIO,
    PRESELL_ACCEPTED_KINDS,
    PRESELL_EXCLUDE_EXISTING_SUBSCRIBERS,
    PRESELL_KNOWN_KINDS,
    PRESELL_MAX_AGE_DAYS,
    PRESELL_MIN_AMOUNT_CENTS,
    PRESELL_MIN_COMMITMENTS,
    TERMINAL_STAGE,
    gate_defs,
    next_stage,
    presell_required,
)
from config.venture_template import REQUIRED_SIGNAL_TYPES

logger = logging.getLogger(__name__)


# ── Result types ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class GateResult:
    name: str
    value: Optional[float]
    threshold: float
    color: str
    description: str
    # True when `value` was None and `color` came from the gate's declared
    # no_metric_behavior rather than from a real measurement. Surfaced so a
    # Slack digest can distinguish "passing" from "not measured, treated as
    # passing" — the distinction ADR 0006 lost.
    imputed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "threshold": self.threshold,
            "color": self.color,
            "description": self.description,
            "imputed": self.imputed,
        }


@dataclass(frozen=True)
class LadderEvaluation:
    venture_key: str
    current_stage: str
    next_stage: Optional[str]
    gates: tuple[GateResult, ...]
    blocked_reasons: tuple[str, ...]

    @property
    def may_advance(self) -> bool:
        return bool(self.next_stage) and not self.blocked_reasons

    def gate_results_dict(self) -> dict[str, Any]:
        return {gate.name: gate.as_dict() for gate in self.gates}


@dataclass(frozen=True)
class CellStats:
    cell_id: str
    sends: int
    replies: int

    @property
    def reply_rate_pct(self) -> Optional[float]:
        if self.sends <= 0:
            return None
        return round(100.0 * self.replies / self.sends, 2)


@dataclass(frozen=True)
class AutoDoubleResult:
    venture_key: str
    fired: bool
    reason: str
    scope: str = "venture"
    cell_id: Optional[str] = None
    reply_rate_pct: Optional[float] = None
    sends: int = 0
    previous_ceiling: Optional[int] = None
    new_ceiling: Optional[int] = None
    previous_multiplier: Optional[int] = None
    new_multiplier: Optional[int] = None


@dataclass(frozen=True)
class PresellStatus:
    venture_key: str
    verified_count: int
    verified_amount_cents: int
    rejected: tuple[str, ...] = field(default=())

    @property
    def satisfied(self) -> bool:
        return (
            self.verified_count >= PRESELL_MIN_COMMITMENTS
            and self.verified_amount_cents >= PRESELL_MIN_AMOUNT_CENTS
        )


# ── Gate colouring ───────────────────────────────────────────────────────────

def _gate_color(cfg: dict[str, Any], value: Optional[float]) -> tuple[str, bool]:
    """Return (colour, imputed) for one gate value.

    Unlike county_launch_evaluator._gate_color, a missing value is NOT
    automatically red — it resolves to the gate's declared
    `no_metric_behavior`. See the module docstring and docs/adr/0006.
    """
    if value is None:
        return cfg.get("no_metric_behavior", "red"), True

    threshold = float(cfg["threshold"])
    direction = cfg["direction"]
    floor = cfg.get("yellow_floor")

    if direction == "binary":
        return ("green" if value >= 1.0 else "red"), False

    if direction == "lower_is_better":
        if value <= threshold:
            return "green", False
        if floor is not None and value <= float(floor):
            return "yellow", False
        return "red", False

    if value >= threshold:
        return "green", False
    if floor is not None and value >= float(floor):
        return "yellow", False
    return "red", False


def _ratio_pct(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return round(100.0 * numerator / denominator, 2)


# ── Metric queries, one per rung ─────────────────────────────────────────────

_RADAR_METRICS = """
WITH score AS (
    SELECT MAX(
        CASE WHEN payload->>'score' ~ '^[0-9]+(\\.[0-9]+)?$'
             THEN (payload->>'score')::numeric END
    ) AS market_score
    FROM venture_ladder_evidence
    WHERE venture_key = :key AND evidence_type = :score_type AND verified
),
own_counties AS (
    SELECT county_id, zip_prefixes FROM counties WHERE venture_key = :key
),
mine AS (
    SELECT DISTINCT jsonb_array_elements_text(zip_prefixes) AS zp FROM own_counties
),
theirs AS (
    SELECT DISTINCT jsonb_array_elements_text(c.zip_prefixes) AS zp
    FROM counties c
    JOIN ventures v ON v.venture_key = c.venture_key
    WHERE c.venture_key <> :key AND v.is_active = true
)
SELECT
    (SELECT market_score FROM score)                          AS market_score,
    (SELECT COUNT(*) FROM own_counties)                        AS county_count,
    (SELECT COUNT(*) FROM mine JOIN theirs USING (zp))         AS overlap_count
"""

_SCRAPE_SAMPLE_COUNT = """
SELECT COUNT(*) AS sample_count
FROM venture_ladder_evidence
WHERE venture_key = :key AND evidence_type = :sample_type AND verified
"""

# Reads the ventures ROW deliberately — see the module docstring on why the
# resolved VentureConfig would report a fresh venture as ready.
_PILOT_METRICS = """
WITH row_state AS (
    SELECT
        COALESCE(NULLIF(TRIM(relay_instantly_campaign_id), ''), NULL)   AS campaign_id,
        COALESCE(NULLIF(TRIM(relay_instantly_sender_email), ''), NULL)  AS sender_email,
        COALESCE(NULLIF(TRIM(relay_slack_channel), ''), NULL)           AS slack_channel,
        COALESCE(NULLIF(TRIM(kill_switch_feature), ''), NULL)           AS kill_switch
    FROM ventures
    WHERE venture_key = :key
),
queue AS (
    SELECT status, dispatched_at, created_at
    FROM relay_approval_queue
    WHERE venture_key = :key
),
windowed AS (
    SELECT status FROM queue
    WHERE created_at >= now() - make_interval(days => :window_days)
)
SELECT
    (SELECT CASE
        WHEN campaign_id IS NOT NULL AND sender_email IS NOT NULL
         AND slack_channel IS NOT NULL AND kill_switch IS NOT NULL THEN 1 ELSE 0 END
     FROM row_state)                                                        AS relay_ready,
    (SELECT COUNT(*) FROM queue WHERE dispatched_at IS NOT NULL)            AS dispatched_count,
    (SELECT COUNT(*) FROM windowed
      WHERE status IN ('rejected', 'failed'))                               AS blocked_count,
    (SELECT COUNT(*) FROM windowed
      WHERE status IN ('rejected', 'failed', 'sent', 'skipped'))            AS decided_count
"""

# Cost attribution: api_usage_logs carries subscriber_id, never venture_key, so
# venture cost is reached via subscribers.county_id -> counties.venture_key.
# Rows with subscriber_id IS NULL (scraping, batch jobs, anything system-wide)
# cannot be attributed to any venture at all. `attributable_ratio` reports what
# share of fleet spend in the window is attributable, and the caller refuses to
# trust the derived gates below MIN_COST_ATTRIBUTION_RATIO — see docs/adr/0006.
_UNIT_ECONOMICS_METRICS = """
WITH venture_subs AS (
    SELECT s.id
    FROM subscribers s
    JOIN counties c ON c.county_id = s.county_id
    WHERE c.venture_key = :key
),
cost_window AS (
    SELECT subscriber_id, COALESCE(cost_usd, 0) AS cost_usd
    FROM api_usage_logs
    WHERE created_at >= now() - make_interval(days => :window_days)
),
revenue AS (
    SELECT COALESCE(SUM(ca.mrr_cents), 0) AS mrr_cents
    FROM customer_accounts ca
    JOIN venture_subs vs ON vs.id = ca.subscriber_id
    WHERE ca.status = 'active'
),
acquisitions AS (
    SELECT COUNT(*) AS n
    FROM customer_accounts ca
    JOIN venture_subs vs ON vs.id = ca.subscriber_id
    WHERE ca.converted_at IS NOT NULL
      AND ca.converted_at >= now() - make_interval(days => :window_days)
),
replies AS (
    SELECT COUNT(*) AS n
    FROM outbound_drafts
    WHERE venture_key = :key
      AND replied_at IS NOT NULL
      AND replied_at >= now() - make_interval(days => :window_days)
)
SELECT
    (SELECT mrr_cents FROM revenue)                                     AS mrr_cents,
    (SELECT COALESCE(SUM(cw.cost_usd), 0) FROM cost_window cw
      JOIN venture_subs vs ON vs.id = cw.subscriber_id)                 AS attributed_cost_usd,
    (SELECT COALESCE(SUM(cost_usd), 0) FROM cost_window)                AS total_cost_usd,
    (SELECT COALESCE(SUM(cost_usd), 0) FROM cost_window
      WHERE subscriber_id IS NOT NULL)                                  AS attributable_cost_usd,
    (SELECT n FROM acquisitions)                                        AS acquisitions,
    (SELECT n FROM replies)                                             AS replies
"""

_CELL_STAGE_METRICS = """
WITH dispatched AS (
    SELECT status FROM relay_approval_queue
    WHERE venture_key = :key
      AND dispatched_at IS NOT NULL
      AND dispatched_at >= now() - make_interval(days => :window_days)
),
failures AS (
    SELECT status FROM relay_approval_queue
    WHERE venture_key = :key
      AND status = 'failed'
      AND updated_at >= now() - make_interval(days => :window_days)
)
SELECT
    (SELECT COUNT(*) FROM dispatched)                       AS dispatched_count,
    (SELECT COUNT(*) FROM failures)                         AS failed_count,
    (SELECT COUNT(*) FROM venture_ladder_events
      WHERE venture_key = :key AND decision = 'auto_double') AS auto_double_count
"""

_SPIN_UP_METRICS = """
WITH clean_batches AS (
    SELECT batch_id
    FROM relay_approval_queue
    WHERE venture_key = :key
      AND batch_id IS NOT NULL
      AND dispatched_at IS NOT NULL
    GROUP BY batch_id
    HAVING COUNT(*) FILTER (WHERE status = 'failed') = 0
)
SELECT
    (SELECT COUNT(*) FROM venture_ladder_evidence
      WHERE venture_key = :key AND evidence_type = :harness_type AND verified
        AND UPPER(COALESCE(payload->>'result', '')) = 'PASS')  AS harness_pass,
    (SELECT COUNT(*) FROM clean_batches)                        AS clean_sweep_count
"""

_CELL_REPLY_RATES = """
SELECT
    d.cell_id,
    COUNT(DISTINCT d.draft_id) AS sends,
    COUNT(DISTINCT d.draft_id) FILTER (WHERE d.replied_at IS NOT NULL) AS replies
FROM outbound_drafts d
JOIN relay_approval_queue q
       ON q.thread_id = d.opportunity_thread_id
      AND q.venture_key = d.venture_key
      AND q.dispatched_at IS NOT NULL
WHERE d.venture_key = :key
  AND d.created_at >= now() - make_interval(days => :window_days)
GROUP BY d.cell_id
ORDER BY d.cell_id
"""


def _radar_metrics(db: Session, venture_key: str) -> dict[str, Optional[float]]:
    row = db.execute(
        text(_RADAR_METRICS),
        {"key": venture_key, "score_type": EVIDENCE_MARKET_SCORE},
    ).one()
    county_count = int(row.county_count or 0)
    return {
        "market_score": float(row.market_score) if row.market_score is not None else None,
        # Geography is binary: the venture must resolve a state and a court and
        # own at least one county. state/court are NOT NULL on `ventures`, so
        # the only real question the DB can answer is whether counties exist.
        "geography_resolvable": 1.0 if county_count >= 1 else 0.0,
        "county_overlap": float(row.overlap_count or 0),
    }


def _probe_metrics(
    db: Session, venture_key: str, *, template_county_id: Optional[str]
) -> dict[str, Optional[float]]:
    """Probe rung — Clone-Pack INPUT: source coverage. OUTPUT: a probed venture.

    Coverage comes from clone_pack.source_coverage() rather than a second copy
    of the query here. The gate that permits an advance and the Clone-Pack that
    reports whether the venture can run must not be able to disagree about what
    "covered" means.
    """
    from src.services.clone_pack import source_coverage

    coverage = source_coverage(db, venture_key, template_county_id=template_county_id)
    total = len(coverage) * len(REQUIRED_SIGNAL_TYPES)
    missing = sum(len(gaps) for gaps in coverage.values())

    sample_count = db.execute(
        text(_SCRAPE_SAMPLE_COUNT),
        {"key": venture_key, "sample_type": EVIDENCE_SCRAPE_SAMPLE},
    ).scalar_one()

    return {
        # No counties means no coverage to measure, which is a red-by-config
        # None rather than a misleading 100%.
        "source_coverage_pct": _ratio_pct(total - missing, total),
        "scrape_sample_count": float(sample_count or 0),
    }


def _pilot_metrics(db: Session, venture_key: str) -> dict[str, Optional[float]]:
    row = db.execute(
        text(_PILOT_METRICS),
        {"key": venture_key, "window_days": METRIC_WINDOW_DAYS},
    ).one()
    return {
        "relay_ready": float(row.relay_ready or 0),
        "dispatched_count": float(row.dispatched_count or 0),
        "compliance_block_pct": _ratio_pct(
            int(row.blocked_count or 0), int(row.decided_count or 0)
        ),
    }


def _unit_economics_metrics(db: Session, venture_key: str) -> dict[str, Optional[float]]:
    row = db.execute(
        text(_UNIT_ECONOMICS_METRICS),
        {"key": venture_key, "window_days": METRIC_WINDOW_DAYS},
    ).one()

    total_cost = float(row.total_cost_usd or 0)
    attributable_cost = float(row.attributable_cost_usd or 0)
    attributed_cost = float(row.attributed_cost_usd or 0)
    attribution_ratio = (attributable_cost / total_cost) if total_cost > 0 else 0.0

    if total_cost > 0 and attribution_ratio < MIN_COST_ATTRIBUTION_RATIO:
        logger.warning(
            "[venture_ladder] %s: only %.0f%% of $%.2f fleet cost in the last %dd is "
            "attributable (subscriber_id IS NULL on the rest) — below the %.0f%% floor, "
            "so the unit-economics gates report no metric rather than a number that "
            "ignores most of the spend",
            venture_key, attribution_ratio * 100, total_cost, METRIC_WINDOW_DAYS,
            MIN_COST_ATTRIBUTION_RATIO * 100,
        )
        return {
            "platform_cost_per_acquisition_usd": None,
            "contribution_margin_usd": None,
            "cost_per_reply_usd": None,
        }

    acquisitions = int(row.acquisitions or 0)
    replies = int(row.replies or 0)
    revenue_usd = float(row.mrr_cents or 0) / 100.0

    return {
        "platform_cost_per_acquisition_usd": (
            round(attributed_cost / acquisitions, 2) if acquisitions > 0 else None
        ),
        # Attributed revenue minus attributed cost. Both sides use the same
        # attribution path, so a venture with no attributable rows reports a
        # margin of 0.0 rather than a falsely positive one.
        "contribution_margin_usd": round(revenue_usd - attributed_cost, 2),
        "cost_per_reply_usd": (
            round(attributed_cost / replies, 2) if replies > 0 else None
        ),
    }


def _cell_stage_metrics(db: Session, venture_key: str) -> dict[str, Optional[float]]:
    row = db.execute(
        text(_CELL_STAGE_METRICS),
        {"key": venture_key, "window_days": METRIC_WINDOW_DAYS},
    ).one()
    stats = cell_reply_rates(db, venture_key, window_days=METRIC_WINDOW_DAYS)
    above_floor = sum(
        1 for cell in stats.values()
        if cell.sends >= CELL_MIN_SAMPLE
        and (cell.reply_rate_pct or 0.0) >= CELL_REPLY_RATE_FLOOR_PCT
    )
    return {
        "cells_above_floor": float(above_floor),
        "clean_auto_double_count": float(row.auto_double_count or 0),
        "send_failure_pct": _ratio_pct(
            int(row.failed_count or 0), int(row.dispatched_count or 0)
        ),
    }


def _spin_up_metrics(db: Session, venture_key: str) -> dict[str, Optional[float]]:
    """Spin-up rung — Clone-Pack INPUT: the complete pack. OUTPUT: a venture
    that runs unattended.

    `clone_pack_complete` is COMPUTED here, not read from a recorded claim. This
    is the rung whose whole job is "is the Clone-Pack actually finished", and
    `assemble()` is the one thing that can answer it: counties attached, every
    required signal type on a county-specific URL rather than an inherited one,
    the venture's own Relay identity present on the row, and its own sweep cron
    line in the crontab.
    """
    from src.services.clone_pack import assemble

    row = db.execute(
        text(_SPIN_UP_METRICS),
        {"key": venture_key, "harness_type": EVIDENCE_HARNESS_RESULT},
    ).one()

    try:
        pack_complete = 1.0 if assemble(db, venture_key).complete else 0.0
    except LookupError:
        pack_complete = 0.0

    return {
        "clone_pack_complete": pack_complete,
        "harness_pass": 1.0 if int(row.harness_pass or 0) > 0 else 0.0,
        "clean_sweep_count": float(row.clean_sweep_count or 0),
    }


_METRIC_BUILDERS = {
    "radar": _radar_metrics,
    "probe": _probe_metrics,
    "pilot": _pilot_metrics,
    "unit_economics": _unit_economics_metrics,
    "cell": _cell_stage_metrics,
    "spin_up": _spin_up_metrics,
}


# ── Venture row access ───────────────────────────────────────────────────────

_SELECT_LADDER_ROW = """
SELECT venture_key, ladder_stage, ladder_entered_at, template_county_id,
       relay_daily_ceiling, relay_send_window_timezone, is_active
FROM ventures
WHERE venture_key = :key
"""


def _ladder_row(db: Session, venture_key: str):
    row = db.execute(text(_SELECT_LADDER_ROW), {"key": venture_key}).first()
    if row is None:
        raise LookupError(
            f"no ventures row for {venture_key!r} — a radar candidate still needs "
            "a row (is_active=false); see docs/venture-ladder.md"
        )
    return row


# ── Public API ───────────────────────────────────────────────────────────────

def cell_reply_rates(
    db: Session, venture_key: str, *, window_days: int = AUTO_DOUBLE_WINDOW_DAYS
) -> dict[str, CellStats]:
    """Sends and replies per cell over a trailing window.

    A "send" is a draft whose thread has a dispatched relay_approval_queue row —
    approved-but-unsent drafts are excluded, because this number decides how
    much mail goes out and must not be inflated by intent.
    """
    rows = db.execute(
        text(_CELL_REPLY_RATES), {"key": venture_key, "window_days": window_days}
    ).fetchall()
    return {
        row.cell_id: CellStats(
            cell_id=row.cell_id, sends=int(row.sends or 0), replies=int(row.replies or 0)
        )
        for row in rows
    }


# `already_subscribed` is TRUE when this deposit's Stripe customer already pays
# some OTHER venture on the fleet — the existing book buying again rather than
# new demand. Matched on subscribers.stripe_customer_id, and scoped to a
# different venture via subscribers.county_id -> counties.venture_key, so a
# customer who only ever subscribed to THIS venture is not excluded.
_PRESELL_EVIDENCE = """
SELECT
    e.payload->>'kind'               AS kind,
    e.payload->>'amount_cents'       AS amount_cents,
    e.payload->>'stripe_customer_id' AS stripe_customer_id,
    e.payload->>'contact_ref'        AS contact_ref,
    e.verified,
    e.recorded_at >= now() - make_interval(days => :max_age) AS in_date,
    EXISTS (
        SELECT 1
        FROM subscribers s
        JOIN counties c ON c.county_id = s.county_id
        WHERE s.stripe_customer_id = e.payload->>'stripe_customer_id'
          AND c.venture_key <> :key
          AND s.status IN ('active', 'grace')
    ) AS already_subscribed
FROM venture_ladder_evidence e
WHERE e.venture_key = :key AND e.evidence_type = :presell_type
ORDER BY e.recorded_at
"""


def presell_gate_status(db: Session, venture_key: str) -> PresellStatus:
    """Count DISTINCT customers and sum amounts across this venture's verified,
    in-date, accepted-kind presell commitments.

    Only `verified` rows count — a row is verified by the machine check that
    confirmed the money moved (a Stripe webhook), never by hand. That is the
    property that makes this gate autonomous rather than a checklist.

    Deduped per customer. The UNIQUE on (venture_key, evidence_type, source_ref)
    stops a webhook retry re-inserting the same PaymentIntent, but says nothing
    about one buyer depositing five times — and five deposits from one
    enthusiastic customer is not evidence of a market. The first commitment per
    customer counts toward both the headcount and the total; later ones from the
    same customer are reported as duplicates.
    """
    rows = db.execute(
        text(_PRESELL_EVIDENCE),
        {
            "key": venture_key,
            "presell_type": EVIDENCE_PRESELL_COMMITMENT,
            "max_age": PRESELL_MAX_AGE_DAYS,
        },
    ).fetchall()

    total_cents = 0
    rejected: list[str] = []
    # Identity for dedup: the Stripe customer if present, else the contact ref.
    # Falling back matters — a commitment recorded without a customer id must not
    # collapse every such row into one bucket keyed on None.
    seen: set[str] = set()

    for index, row in enumerate(rows):
        if not row.verified:
            rejected.append("unverified commitment (no confirmed payment) ignored")
            continue
        if not row.in_date:
            rejected.append(f"commitment older than {PRESELL_MAX_AGE_DAYS}d ignored")
            continue
        kind = (row.kind or "").strip()
        if kind not in PRESELL_ACCEPTED_KINDS:
            known = "known" if kind in PRESELL_KNOWN_KINDS else "unknown"
            rejected.append(f"{known} kind {kind or '(missing)'!r} is not accepted evidence")
            continue
        try:
            amount = int(row.amount_cents)
        except (TypeError, ValueError):
            rejected.append(f"commitment with non-numeric amount_cents ignored ({kind})")
            continue
        if amount <= 0:
            rejected.append(f"commitment with non-positive amount ignored ({kind})")
            continue
        if PRESELL_EXCLUDE_EXISTING_SUBSCRIBERS and row.already_subscribed:
            rejected.append(
                "commitment from a customer who already pays another venture "
                "ignored — that is the existing book, not new demand"
            )
            continue

        customer = (row.stripe_customer_id or "").strip()
        identity = customer or f"contact:{(row.contact_ref or '').strip()}" or f"row:{index}"
        if identity in seen:
            rejected.append(
                "second commitment from a customer already counted ignored — "
                "the gate needs distinct buyers, not repeat deposits"
            )
            continue

        seen.add(identity)
        total_cents += amount

    return PresellStatus(
        venture_key=venture_key,
        verified_count=len(seen),
        verified_amount_cents=total_cents,
        rejected=tuple(rejected),
    )


def presell_gate_blocked(db: Session, venture_key: str) -> list[str]:
    """Reasons the presell gate blocks this venture. Empty means permitted."""
    status = presell_gate_status(db, venture_key)
    reasons: list[str] = []

    if status.verified_count < PRESELL_MIN_COMMITMENTS:
        reasons.append(
            f"presell gate: {status.verified_count} distinct verified customer(s), "
            f"need {PRESELL_MIN_COMMITMENTS}"
        )
    if status.verified_amount_cents < PRESELL_MIN_AMOUNT_CENTS:
        reasons.append(
            f"presell gate: ${status.verified_amount_cents / 100:,.0f} committed, "
            f"need ${PRESELL_MIN_AMOUNT_CENTS / 100:,.0f}"
        )
    if reasons and status.rejected:
        # Only surfaced alongside a real block: a passing gate does not need to
        # explain the rows it ignored.
        reasons.append(f"ignored evidence: {'; '.join(sorted(set(status.rejected)))}")
    return reasons


def evaluate(db: Session, venture_key: str) -> LadderEvaluation:
    """Compute every gate for this venture's current rung. Read-only."""
    row = _ladder_row(db, venture_key)
    stage = row.ladder_stage
    target = next_stage(stage)

    if target is None:
        return LadderEvaluation(
            venture_key=venture_key,
            current_stage=stage,
            next_stage=None,
            gates=(),
            blocked_reasons=(
                () if stage == TERMINAL_STAGE
                else (f"unknown ladder stage {stage!r}",)
            ),
        )

    builder = _METRIC_BUILDERS[stage]
    if stage == "probe":
        values = builder(db, venture_key, template_county_id=row.template_county_id)
    else:
        values = builder(db, venture_key)

    gates: list[GateResult] = []
    blocked: list[str] = []
    for name, cfg in gate_defs(stage).items():
        value = values.get(name)
        color, imputed = _gate_color(cfg, value)
        gates.append(GateResult(
            name=name,
            value=value,
            threshold=float(cfg["threshold"]),
            color=color,
            description=cfg["description"],
            imputed=imputed,
        ))
        if color != "green":
            shown = "no metric" if value is None else f"{value:g}"
            blocked.append(f"{name} is {color} ({shown}; needs {cfg['description']})")

    if presell_required(stage, target):
        blocked.extend(presell_gate_blocked(db, venture_key))

    return LadderEvaluation(
        venture_key=venture_key,
        current_stage=stage,
        next_stage=target,
        gates=tuple(gates),
        blocked_reasons=tuple(blocked),
    )


def advance(
    db: Session, venture_key: str, *, actor: str, force: bool = False
) -> LadderEvaluation:
    """Move a venture up one rung if every gate is green.

    Always writes a venture_ladder_events row — 'advanced' on success,
    'blocked' on refusal — so a refusal is as auditable as a promotion. The
    caller commits.

    `force` promotes despite red gates and is recorded in the audit row's
    actor. It exists because a gate whose metric cannot yet be computed would
    otherwise strand a venture forever, which is the ADR-0006 failure repeated
    at a higher level. Every forced advance is therefore visible, attributed,
    and reviewable — never silent.
    """
    evaluation = evaluate(db, venture_key)

    if evaluation.next_stage is None:
        logger.info(
            "[venture_ladder] %s is at %s — nothing above it",
            venture_key, evaluation.current_stage,
        )
        return evaluation

    permitted = not evaluation.blocked_reasons or force
    recorded_actor = f"{actor} (forced)" if force and evaluation.blocked_reasons else actor

    db.execute(
        text("""
            INSERT INTO venture_ladder_events (
                venture_key, from_stage, to_stage, decision,
                gate_results, blocked_reasons, actor
            ) VALUES (
                :key, :from_stage, :to_stage, :decision,
                CAST(:gate_results AS jsonb), CAST(:blocked AS jsonb), :actor
            )
        """),
        {
            "key": venture_key,
            "from_stage": evaluation.current_stage,
            "to_stage": evaluation.next_stage if permitted else evaluation.current_stage,
            "decision": "advanced" if permitted else "blocked",
            "gate_results": _json(evaluation.gate_results_dict()),
            "blocked": _json(list(evaluation.blocked_reasons)),
            "actor": recorded_actor,
        },
    )

    if not permitted:
        logger.info(
            "[venture_ladder] %s blocked at %s: %s",
            venture_key, evaluation.current_stage, "; ".join(evaluation.blocked_reasons),
        )
        return evaluation

    db.execute(
        text("""
            UPDATE ventures
            SET ladder_stage = :to_stage, ladder_entered_at = now()
            WHERE venture_key = :key
        """),
        {"key": venture_key, "to_stage": evaluation.next_stage},
    )
    _invalidate(venture_key)

    logger.info(
        "[venture_ladder] %s advanced %s -> %s by %s",
        venture_key, evaluation.current_stage, evaluation.next_stage, recorded_actor,
    )
    return LadderEvaluation(
        venture_key=venture_key,
        current_stage=evaluation.next_stage,
        next_stage=next_stage(evaluation.next_stage),
        gates=evaluation.gates,
        blocked_reasons=(),
    )


def record_evidence(
    db: Session,
    venture_key: str,
    *,
    evidence_type: str,
    payload: dict[str, Any],
    stage: str,
    recorded_by: str,
    source_ref: Optional[str] = None,
    verified: bool = False,
) -> bool:
    """Write one evidence row. True if inserted, False if `source_ref` already
    existed for this (venture, evidence_type).

    The UNIQUE on (venture_key, evidence_type, source_ref) is what makes a
    Stripe webhook retry unable to inflate a presell count. Rows with a NULL
    source_ref are always inserted — Postgres treats NULLs as distinct — which
    is correct for repeatable evidence like a fresh scrape sample.
    """
    result = db.execute(
        text("""
            INSERT INTO venture_ladder_evidence (
                venture_key, stage, evidence_type, payload, source_ref,
                verified, recorded_by
            ) VALUES (
                :key, :stage, :evidence_type, CAST(:payload AS jsonb), :source_ref,
                :verified, :recorded_by
            )
            ON CONFLICT (venture_key, evidence_type, source_ref) DO NOTHING
        """),
        {
            "key": venture_key,
            "stage": stage,
            "evidence_type": evidence_type,
            "payload": _json(payload),
            "source_ref": source_ref,
            "verified": verified,
            "recorded_by": recorded_by,
        },
    )
    inserted = bool(result.rowcount)
    if not inserted:
        logger.info(
            "[venture_ladder] evidence %s/%s for %s already recorded (source_ref=%s)",
            evidence_type, stage, venture_key, source_ref,
        )
    return inserted


# ── Auto-double ──────────────────────────────────────────────────────────────

_LAST_AUTO_DOUBLE = """
SELECT created_at, gate_results
FROM venture_ladder_events
WHERE venture_key = :key
  AND decision = 'auto_double'
  AND COALESCE(gate_results->>'scope', 'venture') = :scope
  AND (:cell_id IS NULL OR gate_results->>'cell_id' = :cell_id)
ORDER BY created_at DESC
LIMIT 1
"""


def _cooldown_remaining_days(
    db: Session, venture_key: str, *, scope: str, cell_id: Optional[str], now: datetime
) -> Optional[float]:
    """Days still to wait before another doubling may fire at this scope, or
    None if the cooldown has elapsed / nothing has ever fired.

    Read from the audit table, NOT a Redis flag: the flag would expire
    independently of the ceiling it guards, so a Redis eviction would permit a
    second doubling on the same day.
    """
    row = db.execute(
        text(_LAST_AUTO_DOUBLE),
        {"key": venture_key, "scope": scope, "cell_id": cell_id},
    ).first()
    if row is None:
        return None

    last = row.created_at
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    elapsed_days = (now - last).total_seconds() / 86400.0
    if elapsed_days >= AUTO_DOUBLE_COOLDOWN_DAYS:
        return None
    return round(AUTO_DOUBLE_COOLDOWN_DAYS - elapsed_days, 2)


def _send_failure_pct(db: Session, venture_key: str) -> Optional[float]:
    row = db.execute(
        text(_CELL_STAGE_METRICS),
        {"key": venture_key, "window_days": AUTO_DOUBLE_WINDOW_DAYS},
    ).one()
    return _ratio_pct(int(row.failed_count or 0), int(row.dispatched_count or 0))


def _record_auto_double(
    db: Session,
    venture_key: str,
    *,
    stage: str,
    payload: dict[str, Any],
    actor: str,
) -> None:
    db.execute(
        text("""
            INSERT INTO venture_ladder_events (
                venture_key, from_stage, to_stage, decision, gate_results, actor
            ) VALUES (
                :key, :stage, :stage, 'auto_double', CAST(:payload AS jsonb), :actor
            )
        """),
        {"key": venture_key, "stage": stage, "payload": _json(payload), "actor": actor},
    )


def maybe_auto_double(
    db: Session,
    venture_key: str,
    *,
    now: Optional[datetime] = None,
    actor: str = "venture_ladder.auto_double",
) -> AutoDoubleResult:
    """Double this venture's Relay daily ceiling when its reply rate clears
    AUTO_DOUBLE_REPLY_RATE_PCT. The caller commits.

    Every guard here is load-bearing:

    - min sample     8% of a dozen sends is one reply, which is noise.
    - cooldown       doubling cold-email volume on a warming domain is how a
                     sender gets blacklisted, and lowering the number back does
                     not undo it.
    - send failures  a high reply rate next to a high failure rate means the
                     list is dirty, not that the copy is good.
    - max ceiling    bounds a runaway loop of doublings.
    - cache flush    ventures is read through a 5-minute cache; without the
                     flush the new ceiling silently does not apply for up to
                     5 minutes and the venture under-sends.
    """
    now = now or datetime.now(timezone.utc)
    row = _ladder_row(db, venture_key)

    stats = cell_reply_rates(db, venture_key, window_days=AUTO_DOUBLE_WINDOW_DAYS)
    sends = sum(cell.sends for cell in stats.values())
    replies = sum(cell.replies for cell in stats.values())
    rate = _ratio_pct(replies, sends)

    def _no(reason: str) -> AutoDoubleResult:
        logger.info("[venture_ladder] auto-double declined for %s: %s", venture_key, reason)
        return AutoDoubleResult(
            venture_key=venture_key, fired=False, reason=reason,
            reply_rate_pct=rate, sends=sends, previous_ceiling=row.relay_daily_ceiling,
        )

    if sends < AUTO_DOUBLE_MIN_SAMPLE:
        return _no(
            f"sample too small: {sends} sends in {AUTO_DOUBLE_WINDOW_DAYS}d, "
            f"need {AUTO_DOUBLE_MIN_SAMPLE}"
        )
    if rate is None or rate <= AUTO_DOUBLE_REPLY_RATE_PCT:
        return _no(
            f"reply rate {rate}% does not exceed {AUTO_DOUBLE_REPLY_RATE_PCT}%"
        )

    cooldown = _cooldown_remaining_days(
        db, venture_key, scope="venture", cell_id=None, now=now
    )
    if cooldown is not None:
        return _no(f"cooldown: {cooldown}d remaining of {AUTO_DOUBLE_COOLDOWN_DAYS}d")

    failure_pct = _send_failure_pct(db, venture_key)
    if failure_pct is not None and failure_pct > AUTO_DOUBLE_MAX_SEND_FAILURE_PCT:
        return _no(
            f"send failure rate {failure_pct}% exceeds "
            f"{AUTO_DOUBLE_MAX_SEND_FAILURE_PCT}% — scaling a dirty list is a "
            "reputation risk, not growth"
        )

    previous = int(row.relay_daily_ceiling)
    if previous >= AUTO_DOUBLE_MAX_CEILING:
        return _no(f"ceiling {previous} already at the {AUTO_DOUBLE_MAX_CEILING} cap")

    new_ceiling = min(previous * AUTO_DOUBLE_MULTIPLIER, AUTO_DOUBLE_MAX_CEILING)

    payload = {
        "scope": "venture",
        "reply_rate_pct": rate,
        "sends": sends,
        "replies": replies,
        "window_days": AUTO_DOUBLE_WINDOW_DAYS,
        "send_failure_pct": failure_pct,
        "previous_ceiling": previous,
        "new_ceiling": new_ceiling,
    }

    db.execute(
        text("UPDATE ventures SET relay_daily_ceiling = :ceiling WHERE venture_key = :key"),
        {"ceiling": new_ceiling, "key": venture_key},
    )
    _record_auto_double(db, venture_key, stage=row.ladder_stage, payload=payload, actor=actor)
    _invalidate(venture_key)

    logger.info(
        "[venture_ladder] %s auto-doubled ceiling %d -> %d on a %.2f%% reply rate "
        "over %d sends",
        venture_key, previous, new_ceiling, rate, sends,
    )
    return AutoDoubleResult(
        venture_key=venture_key,
        fired=True,
        reason=f"reply rate {rate}% over {sends} sends",
        reply_rate_pct=rate,
        sends=sends,
        previous_ceiling=previous,
        new_ceiling=new_ceiling,
    )


def cell_production_multipliers(db: Session, venture_key: str) -> dict[str, int]:
    """Per-cell target-production multipliers, derived from the audit log.

    Cora's producers take a plain `limit` (see
    src/agents/cora/ingestion/target_producer.py:produce_targets), so the
    cell-level rule scales how many targets a winning cell produces —
    `limit * multiplier`. It does NOT scale a second ceiling: there is exactly
    one send cap in this system, ventures.relay_daily_ceiling, and the mix
    shift here happens underneath it.

    Derived rather than stored: multiplier = 2 ** (number of auto_double events
    for that cell), capped. One source of truth, and the cooldown and
    idempotency guards read the same rows.
    """
    rows = db.execute(
        text("""
            SELECT gate_results->>'cell_id' AS cell_id, COUNT(*) AS doublings
            FROM venture_ladder_events
            WHERE venture_key = :key
              AND decision = 'auto_double'
              AND gate_results->>'scope' = 'cell'
              AND gate_results->>'cell_id' IS NOT NULL
            GROUP BY gate_results->>'cell_id'
        """),
        {"key": venture_key},
    ).fetchall()
    return {
        row.cell_id: min(
            AUTO_DOUBLE_MULTIPLIER ** int(row.doublings), AUTO_DOUBLE_CELL_MAX_MULTIPLIER
        )
        for row in rows
    }


def maybe_auto_double_cell(
    db: Session,
    venture_key: str,
    cell_id: str,
    *,
    now: Optional[datetime] = None,
    actor: str = "venture_ladder.auto_double_cell",
) -> AutoDoubleResult:
    """Double one cell's target-production multiplier on a high reply rate.

    Same guard set as the venture-level rule, with the same cooldown, applied
    per cell. See cell_production_multipliers() for what the multiplier means —
    it is a production knob, not a second send cap.
    """
    now = now or datetime.now(timezone.utc)
    row = _ladder_row(db, venture_key)

    stats = cell_reply_rates(db, venture_key, window_days=AUTO_DOUBLE_WINDOW_DAYS)
    cell = stats.get(cell_id)
    sends = cell.sends if cell else 0
    rate = cell.reply_rate_pct if cell else None
    current = cell_production_multipliers(db, venture_key).get(cell_id, 1)

    def _no(reason: str) -> AutoDoubleResult:
        logger.info(
            "[venture_ladder] cell auto-double declined for %s/%s: %s",
            venture_key, cell_id, reason,
        )
        return AutoDoubleResult(
            venture_key=venture_key, fired=False, reason=reason, scope="cell",
            cell_id=cell_id, reply_rate_pct=rate, sends=sends,
            previous_multiplier=current,
        )

    if sends < AUTO_DOUBLE_MIN_SAMPLE:
        return _no(
            f"sample too small: {sends} sends in {AUTO_DOUBLE_WINDOW_DAYS}d, "
            f"need {AUTO_DOUBLE_MIN_SAMPLE}"
        )
    if rate is None or rate <= AUTO_DOUBLE_REPLY_RATE_PCT:
        return _no(f"reply rate {rate}% does not exceed {AUTO_DOUBLE_REPLY_RATE_PCT}%")

    cooldown = _cooldown_remaining_days(
        db, venture_key, scope="cell", cell_id=cell_id, now=now
    )
    if cooldown is not None:
        return _no(f"cooldown: {cooldown}d remaining of {AUTO_DOUBLE_COOLDOWN_DAYS}d")

    if current >= AUTO_DOUBLE_CELL_MAX_MULTIPLIER:
        return _no(
            f"multiplier {current}x already at the {AUTO_DOUBLE_CELL_MAX_MULTIPLIER}x cap"
        )

    new_multiplier = min(
        current * AUTO_DOUBLE_MULTIPLIER, AUTO_DOUBLE_CELL_MAX_MULTIPLIER
    )
    _record_auto_double(
        db, venture_key,
        stage=row.ladder_stage,
        payload={
            "scope": "cell",
            "cell_id": cell_id,
            "reply_rate_pct": rate,
            "sends": sends,
            "replies": cell.replies if cell else 0,
            "window_days": AUTO_DOUBLE_WINDOW_DAYS,
            "previous_multiplier": current,
            "new_multiplier": new_multiplier,
        },
        actor=actor,
    )

    logger.info(
        "[venture_ladder] %s cell %s multiplier %dx -> %dx on a %.2f%% reply rate",
        venture_key, cell_id, current, new_multiplier, rate,
    )
    return AutoDoubleResult(
        venture_key=venture_key,
        fired=True,
        reason=f"reply rate {rate}% over {sends} sends",
        scope="cell",
        cell_id=cell_id,
        reply_rate_pct=rate,
        sends=sends,
        previous_multiplier=current,
        new_multiplier=new_multiplier,
    )


# ── Internals ────────────────────────────────────────────────────────────────

def _json(value: Any) -> str:
    import json

    return json.dumps(value, default=str)


def _invalidate(venture_key: str) -> None:
    """Flush the venture config cache after a write to `ventures`.

    Isolated and defensive on purpose: a cache-flush failure must never roll
    back a ceiling change or a stage advance that already succeeded.
    """
    try:
        from src.utils.venture_config import invalidate_cache

        invalidate_cache(venture_key)
    except Exception:
        logger.warning(
            "[venture_ladder] could not flush venture config cache for %s — the "
            "change applies within the 5-minute TTL",
            venture_key, exc_info=True,
        )
