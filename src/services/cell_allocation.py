"""
LEARN-v2.2 Layer 3 — cell kill / throttle / revive service.

Pure decision layer + DB sweep. Config and rationale live in
config/cell_allocation.py. This module is the missing downward half of the
CLONE-v2.2 CL4 auto-double engine; it reuses CL4's helpers rather than
re-implementing them.

Public surface:
    decide(sends, replies, is_throttled, days_since_last_verdict) -> Verdict
    sweep(db, venture_key, *, now, dry_run) -> AllocationReport
    cell_is_throttled(db, venture_key, cell_id) -> bool

The sweep is called by src/tasks/cell_allocation_sweep.py.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.cell_allocation import (
    AMBIGUOUS_MIN_SENDS,
    DECISION_REVIVE,
    DECISION_THROTTLE,
    ELIGIBLE_STAGES,
    KILL_REPLY_RATE_PCT,
    MAX_THROTTLES_PER_RUN,
    REVIVE_MIN_SENDS,
    REVIVE_REPLY_RATE_PCT,
    THROTTLE_FLOOR_PCT,
    VERDICT_COOLDOWN_DAYS,
    VERDICT_HOLD,
    VERDICT_REVIVE,
    VERDICT_SKIP_COOLDOWN,
    VERDICT_SKIP_INSUFFICIENT_SAMPLE,
    VERDICT_THROTTLE,
    WINDOW_DAYS,
    config_snapshot,
    validate_allocation_config,
)
from src.services.venture_ladder import (
    CellStats,
    _cooldown_remaining_days,
    _ineligible_for_auto_double,
    _json,
    _ladder_row,
    cell_reply_rates,
)

logger = logging.getLogger(__name__)

ZERO_REPLY_MIN_SENDS = 30  # imported separately from config for clarity in decide()


# ── Verdict ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CellVerdict:
    cell_id: str
    verdict: str          # one of config.cell_allocation.ALL_VERDICTS
    sends: int = 0
    replies: int = 0
    reply_rate_pct: Optional[float] = None
    reason: str = ""


@dataclass
class AllocationReport:
    venture_key: str
    dry_run: bool
    cells_evaluated: int = 0
    throttled: list[CellVerdict] = field(default_factory=list)
    revived: list[CellVerdict] = field(default_factory=list)
    held: list[CellVerdict] = field(default_factory=list)
    skipped: list[CellVerdict] = field(default_factory=list)
    blast_radius_hit: bool = False

    @property
    def mutations(self) -> int:
        return len(self.throttled) + len(self.revived)


# ── Pure decision function ────────────────────────────────────────────────────

def decide(
    sends: int,
    replies: int,
    *,
    is_throttled: bool,
    days_since_last_verdict: Optional[float],
) -> str:
    """Pure function — no DB, no clock.

    Returns one of the VERDICT_* constants. Caller supplies context.

    Two-path logic for throttle candidates (documented in config):
      - Fast path: 0 replies after ZERO_REPLY_MIN_SENDS → throttle.
      - Slow path: reply rate < KILL_REPLY_RATE_PCT after AMBIGUOUS_MIN_SENDS → throttle.

    Revival is symmetric: is_throttled AND rate >= REVIVE_REPLY_RATE_PCT AND
    sends >= REVIVE_MIN_SENDS → revive.
    """
    # Cooldown blocks any new verdict on this cell.
    if days_since_last_verdict is not None and days_since_last_verdict < VERDICT_COOLDOWN_DAYS:
        return VERDICT_SKIP_COOLDOWN

    rate: Optional[float] = None
    if sends > 0:
        rate = round(100.0 * replies / sends, 2)

    # Revival check comes first: a throttled cell's only exit is revival.
    if is_throttled:
        if sends >= REVIVE_MIN_SENDS and rate is not None and rate >= REVIVE_REPLY_RATE_PCT:
            return VERDICT_REVIVE
        # Throttled cells don't get re-throttled; hold silently.
        return VERDICT_HOLD

    # Fast kill: unambiguous zero evidence at minimum volume.
    if replies == 0 and sends >= ZERO_REPLY_MIN_SENDS:
        return VERDICT_THROTTLE

    # Slow kill: real signal but below the floor — demand a full sample.
    if sends < AMBIGUOUS_MIN_SENDS:
        return VERDICT_SKIP_INSUFFICIENT_SAMPLE

    if rate is not None and rate < KILL_REPLY_RATE_PCT:
        return VERDICT_THROTTLE

    return VERDICT_HOLD


# ── DB helpers ────────────────────────────────────────────────────────────────

_LAST_ALLOCATION_EVENT = """
SELECT created_at
FROM venture_ladder_events
WHERE venture_key = :key
  AND gate_results->>'cell_id' = :cell_id
  AND decision IN ('auto_throttle', 'auto_revive')
ORDER BY created_at DESC
LIMIT 1
"""

_ACTIVE_THROTTLE = """
SELECT COUNT(*) AS cnt
FROM venture_ladder_events thr
WHERE thr.venture_key = :key
  AND thr.decision = 'auto_throttle'
  AND thr.gate_results->>'cell_id' = :cell_id
  AND NOT EXISTS (
      SELECT 1
      FROM venture_ladder_events rev
      WHERE rev.venture_key = :key
        AND rev.decision = 'auto_revive'
        AND rev.gate_results->>'cell_id' = :cell_id
        AND rev.created_at > thr.created_at
  )
"""


def cell_is_throttled(db: Session, venture_key: str, cell_id: str) -> bool:
    """True when the most recent allocation verdict for this cell is a throttle
    that has not been followed by a revive.

    Called by target_producer._cell_multiplier() to apply the floor.
    """
    row = db.execute(
        text(_ACTIVE_THROTTLE), {"key": venture_key, "cell_id": cell_id}
    ).first()
    return bool(row and int(row.cnt) > 0)


def _days_since_last_allocation_event(
    db: Session, venture_key: str, cell_id: str, *, now: datetime
) -> Optional[float]:
    row = db.execute(
        text(_LAST_ALLOCATION_EVENT), {"key": venture_key, "cell_id": cell_id}
    ).first()
    if row is None:
        return None
    last = row.created_at
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (now - last).total_seconds() / 86400.0


def _record_allocation_event(
    db: Session,
    venture_key: str,
    *,
    stage: str,
    decision: str,
    payload: dict,
    actor: str,
) -> None:
    db.execute(
        text("""
            INSERT INTO venture_ladder_events (
                venture_key, from_stage, to_stage, decision, gate_results, actor
            ) VALUES (
                :key, :stage, :stage, :decision, CAST(:payload AS jsonb), :actor
            )
        """),
        {
            "key": venture_key,
            "stage": stage,
            "decision": decision,
            "payload": _json(payload),
            "actor": actor,
        },
    )


# ── Sweep ─────────────────────────────────────────────────────────────────────

def sweep(
    db: Session,
    venture_key: str,
    *,
    now: Optional[datetime] = None,
    actor: str = "cell_allocation.sweep",
    dry_run: bool = False,
) -> AllocationReport:
    """Evaluate every cell for this venture and fire throttle/revive events.

    Blast-radius rail: at most MAX_THROTTLES_PER_RUN throttles per run. A feed
    break that zeroes all reply counts looks exactly like every cell failing;
    this cap prevents acting on all of them in one pass.

    The caller commits after a non-dry-run sweep.
    """
    now = now or datetime.now(timezone.utc)
    report = AllocationReport(venture_key=venture_key, dry_run=dry_run)

    config_errors = validate_allocation_config()
    if config_errors:
        for err in config_errors:
            logger.error("[cell_allocation] config invalid: %s", err)
        raise RuntimeError(f"cell_allocation config invalid: {config_errors[0]}")

    row = _ladder_row(db, venture_key)
    ineligible = _ineligible_for_auto_double(row)
    if ineligible:
        logger.info(
            "[cell_allocation] skipping %s — venture ineligible: %s", venture_key, ineligible
        )
        return report

    stats: dict[str, CellStats] = cell_reply_rates(db, venture_key, window_days=WINDOW_DAYS)
    report.cells_evaluated = len(stats)

    throttles_this_run = 0
    snapshot = config_snapshot()

    for cell_id, cell in stats.items():
        is_throttled = cell_is_throttled(db, venture_key, cell_id)
        days_since = _days_since_last_allocation_event(
            db, venture_key, cell_id, now=now
        )

        verdict = decide(
            cell.sends,
            cell.replies,
            is_throttled=is_throttled,
            days_since_last_verdict=days_since,
        )

        cv = CellVerdict(
            cell_id=cell_id,
            verdict=verdict,
            sends=cell.sends,
            replies=cell.replies,
            reply_rate_pct=cell.reply_rate_pct,
            reason=_verdict_reason(verdict, cell),
        )

        if verdict == VERDICT_THROTTLE:
            if throttles_this_run >= MAX_THROTTLES_PER_RUN:
                report.blast_radius_hit = True
                report.skipped.append(
                    CellVerdict(
                        cell_id=cell_id,
                        verdict="skip_blast_radius",
                        sends=cell.sends,
                        replies=cell.replies,
                        reply_rate_pct=cell.reply_rate_pct,
                        reason="blast-radius cap reached",
                    )
                )
                continue

            throttles_this_run += 1
            report.throttled.append(cv)
            if not dry_run:
                _record_allocation_event(
                    db,
                    venture_key,
                    stage=row.ladder_stage,
                    decision=DECISION_THROTTLE,
                    payload={
                        "cell_id": cell_id,
                        "sends": cell.sends,
                        "replies": cell.replies,
                        "reply_rate_pct": cell.reply_rate_pct,
                        "window_days": WINDOW_DAYS,
                        "throttle_floor_pct": THROTTLE_FLOOR_PCT,
                        **snapshot,
                    },
                    actor=actor,
                )
                logger.info(
                    "[cell_allocation] throttled cell %s/%s: %d sends, %s%% reply",
                    venture_key, cell_id, cell.sends, cell.reply_rate_pct,
                )

        elif verdict == VERDICT_REVIVE:
            report.revived.append(cv)
            if not dry_run:
                _record_allocation_event(
                    db,
                    venture_key,
                    stage=row.ladder_stage,
                    decision=DECISION_REVIVE,
                    payload={
                        "cell_id": cell_id,
                        "sends": cell.sends,
                        "replies": cell.replies,
                        "reply_rate_pct": cell.reply_rate_pct,
                        "window_days": WINDOW_DAYS,
                        **snapshot,
                    },
                    actor=actor,
                )
                logger.info(
                    "[cell_allocation] revived cell %s/%s: %d sends, %s%% reply",
                    venture_key, cell_id, cell.sends, cell.reply_rate_pct,
                )

        elif verdict == VERDICT_HOLD:
            report.held.append(cv)
        else:
            report.skipped.append(cv)

    return report


def _verdict_reason(verdict: str, cell: CellStats) -> str:
    if verdict == VERDICT_THROTTLE:
        if cell.replies == 0:
            return f"0 replies in {cell.sends} sends (fast kill)"
        return f"{cell.reply_rate_pct}% reply rate below {KILL_REPLY_RATE_PCT}% floor"
    if verdict == VERDICT_REVIVE:
        return f"{cell.reply_rate_pct}% reply rate cleared {REVIVE_REPLY_RATE_PCT}% revival threshold"
    if verdict == VERDICT_HOLD:
        return "within bounds"
    if verdict == VERDICT_SKIP_COOLDOWN:
        return f"cooldown ({VERDICT_COOLDOWN_DAYS}d) not elapsed"
    if verdict == VERDICT_SKIP_INSUFFICIENT_SAMPLE:
        return f"only {cell.sends} sends, need {AMBIGUOUS_MIN_SENDS}"
    return verdict
