"""
LEARN-v2.2 Layer 3 — challenger protected-floor monitor service (PROPOSE-ONLY).

This module MEASURES the challenger cohort's send share against the 30% floor
(spec §9.4 line 238/458) and reports it. It does NOT reallocate capacity and it
never sets a price — see config/challenger_floor.py for why auto-enforcement,
the >=20% GP formal-challenge trigger, and the 20/10 winner split are deferred
(no divisible production pool + no retained-GP/declared winner pre-launch). This
respects spec line 259 ("never set").

A2: a challenger is DERIVED — a cell is a challenger iff it has no L3 verdict
event yet, and an incumbent iff it has one; send count alone never judges it. A4: this
is a COHORT reserve, not a per-cell guarantee — L3 kill/throttle (see
src/services/cell_allocation.py) still governs each individual challenger cell;
the floor protects the cohort as a whole, not any one cell.

NOTE ON "CHALLENGER" (spec fidelity): the spec (line 238) names a distinct
Opportunity Challenger Board, engine-fit scored. Pre-launch, with no engine-fit
score or GP data, this build measures the floor over a DERIVED proxy population —
active campaign cells that L3 has not yet judged — not the literal Challenger
Board. When engine-fit scoring / a real board exist, that becomes the population;
the floor math here is unchanged by that swap.

Public surface:
    compute_share(challenger_sends, total_sends, floor_pct, min_total) -> tuple
    classify_cells(db, venture_key, stats) -> tuple[set[str], set[str]]
    evaluate_floor(db, venture_key, *, now=None) -> ChallengerFloorReport
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.challenger_floor import (
    CHALLENGER_FLOOR_PCT,
    MIN_TOTAL_SENDS,
    WINDOW_DAYS,
)
from src.services.venture_ladder import (
    _ineligible_for_auto_double,
    _ladder_row,
    cell_reply_rates,
)

logger = logging.getLogger(__name__)

# The venture_ladder_events.decision values that mean L3 has issued a verdict on
# a cell. A cell with any such row is no longer a challenger.
VERDICT_DECISIONS = ("auto_double", "auto_throttle", "auto_revive")

_VERDICT_CELL_IDS = """
SELECT DISTINCT gate_results->>'cell_id' AS cell_id
FROM venture_ladder_events
WHERE venture_key = :key
  AND decision = ANY(:decisions)
  AND gate_results->>'cell_id' IS NOT NULL
"""


@dataclass(frozen=True)
class ChallengerFloorReport:
    venture_key: str
    challenger_cells: list[str]
    incumbent_cells: list[str]
    challenger_sends: int
    total_sends: int
    challenger_share_pct: Optional[float]
    floor_pct: int
    under_floor: bool
    shortfall_pct: Optional[float]
    note: str


def compute_share(
    challenger_sends: int,
    total_sends: int,
    floor_pct: int,
    min_total: int,
) -> tuple[Optional[float], bool, Optional[float], str]:
    """Pure floor math — the testable core of evaluate_floor().

    Returns (share_pct, under_floor, shortfall_pct, note). Never divides by zero:
    a total below min_total (which includes zero) returns a note and under_floor
    False, because a cohort share measured on a handful of sends is noise, not a
    signal that the challenger cohort is being starved.
    """
    if total_sends < min_total:
        return (
            None,
            False,
            None,
            f"insufficient volume ({total_sends} < {min_total} sends) — share not evaluated",
        )

    share_pct = round(100.0 * challenger_sends / total_sends, 2)
    under_floor = share_pct < floor_pct
    shortfall_pct = round(floor_pct - share_pct, 2) if under_floor else None
    note = (
        f"challenger cohort at {share_pct}% vs {floor_pct}% floor"
        if not under_floor
        else f"challenger cohort at {share_pct}% — under {floor_pct}% floor by {shortfall_pct}%"
    )
    return share_pct, under_floor, shortfall_pct, note


def classify_cells(
    db: Session, venture_key: str, stats
) -> tuple[set[str], set[str]]:
    """Split the venture's active cells into (challengers, incumbents).

    A cell is a CHALLENGER iff it has NO L3 verdict row yet (A2: no verdict
    yet); it is an INCUMBENT iff it HAS a verdict row. Send count alone never
    reclassifies a cell — a well-sampled cell sitting in the dead zone between
    the kill and double bars has accrued no verdict and stays a challenger. The
    verdict lookup is ONE bulk query, not one per cell (CLAUDE.md: never query
    inside a loop).
    """
    verdict_rows = db.execute(
        text(_VERDICT_CELL_IDS),
        {"key": venture_key, "decisions": list(VERDICT_DECISIONS)},
    ).fetchall()
    verdict_cells = {r.cell_id for r in verdict_rows if r.cell_id}

    challengers: set[str] = set()
    incumbents: set[str] = set()
    for cell_id in stats:
        if cell_id in verdict_cells:
            incumbents.add(cell_id)
        else:
            challengers.add(cell_id)
    return challengers, incumbents


def _safe_report(venture_key: str, note: str) -> ChallengerFloorReport:
    return ChallengerFloorReport(
        venture_key=venture_key,
        challenger_cells=[],
        incumbent_cells=[],
        challenger_sends=0,
        total_sends=0,
        challenger_share_pct=None,
        floor_pct=CHALLENGER_FLOOR_PCT,
        under_floor=False,
        shortfall_pct=None,
        note=note,
    )


def evaluate_floor(
    db: Session, venture_key: str, *, now: Optional[datetime] = None
) -> ChallengerFloorReport:
    """Measure the challenger cohort's send share against the 30% floor.

    Venture-eligibility is L3's (a venture that may not auto-double is skipped
    here too). Never raises — any failure returns a safe, under_floor=False
    report so one bad venture cannot break the sweep.
    """
    try:
        try:
            row = _ladder_row(db, venture_key)
        except LookupError as exc:
            return _safe_report(venture_key, f"skipped: {exc}")

        ineligible = _ineligible_for_auto_double(row)
        if ineligible:
            return _safe_report(venture_key, f"skipped: {ineligible}")

        stats = cell_reply_rates(db, venture_key, window_days=WINDOW_DAYS)
        challengers, incumbents = classify_cells(db, venture_key, stats)

        challenger_sends = sum(stats[c].sends for c in challengers)
        total_sends = sum(s.sends for s in stats.values())

        if not challengers:
            # Every active cell has an L3 verdict — the cohort is empty, so there
            # is nothing to reserve for. A 0% share here is not an under-floor
            # signal; proposing a reserve for "(none)" is an unactionable alert.
            return ChallengerFloorReport(
                venture_key=venture_key,
                challenger_cells=[],
                incumbent_cells=sorted(incumbents),
                challenger_sends=0,
                total_sends=total_sends,
                challenger_share_pct=0.0,
                floor_pct=CHALLENGER_FLOOR_PCT,
                under_floor=False,
                shortfall_pct=None,
                note="no active challenger cells — nothing to reserve",
            )

        share_pct, under_floor, shortfall_pct, note = compute_share(
            challenger_sends, total_sends, CHALLENGER_FLOOR_PCT, MIN_TOTAL_SENDS
        )

        return ChallengerFloorReport(
            venture_key=venture_key,
            challenger_cells=sorted(challengers),
            incumbent_cells=sorted(incumbents),
            challenger_sends=challenger_sends,
            total_sends=total_sends,
            challenger_share_pct=share_pct,
            floor_pct=CHALLENGER_FLOOR_PCT,
            under_floor=under_floor,
            shortfall_pct=shortfall_pct,
            note=note,
        )
    except Exception:
        logger.error(
            "[challenger_floor] error evaluating venture %s", venture_key, exc_info=True
        )
        return _safe_report(venture_key, "error during evaluation — see logs")
