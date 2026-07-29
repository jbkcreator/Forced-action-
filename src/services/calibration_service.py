"""
Vera calibration data pipeline — REVINT-I1.

Computes monthly predicted-vs-actual accuracy metrics per segment and writes
a JSON summary that Vera reads independently. This service does NOT write
Vera's conclusions — it only produces the raw calibration facts.

Output: shared/facts/calibration_summary_YYYY_MM.json

Run monthly via cron or on-demand:
    PYTHONPATH=. python -m src.services.calibration_service
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TypedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

FACTS_DIR = Path("shared/facts")


class SegmentCalibration(TypedDict):
    segment: str
    predicted_p_close: float
    actual_close_rate: float
    predicted_time_to_cash: float
    actual_time_to_cash: float
    sample_size: int
    computed_at: str


class CalibrationSummary(TypedDict):
    month: str            # YYYY-MM
    segments: list[SegmentCalibration]
    computed_at: str
    note: str


_SEGMENTS = ("whale", "auction_winner", "lapsed_subscriber", "default")


def _compute_segment(
    db: Session,
    segment: str,
    year: int,
    month: int,
) -> Optional[SegmentCalibration]:
    """
    Queries opportunity_scores for the given month+segment and computes
    predicted vs actual rates.

    "actual_close_rate" is approximated from opportunity_score_history rows
    that indicate a "closed" reason within the same calendar month, divided
    by total scored records for that segment+month.

    If no records exist, returns None (segment excluded from summary).
    """
    # Predicted averages from scored records created this month
    predicted = db.execute(
        text(
            """
            SELECT
                AVG(p_close)           AS avg_p_close,
                AVG(time_to_cash_days) AS avg_time_to_cash,
                COUNT(*)               AS sample_size
            FROM opportunity_scores
            WHERE segment = :seg
              AND date_trunc('month', created_at) =
                  make_date(:yr, :mo, 1)::timestamptz
            """
        ),
        {"seg": segment, "yr": year, "mo": month},
    ).fetchone()

    if not predicted or (predicted[2] or 0) == 0:
        return None

    sample_size = int(predicted[2])
    avg_p_close = float(predicted[0] or 0)
    avg_time_to_cash = float(predicted[1] or 0)

    # Actual close rate: history rows with reason='closed' this month / sample_size
    closed_count = db.execute(
        text(
            """
            SELECT COUNT(DISTINCT h.opportunity_score_id)
            FROM opportunity_score_history h
            JOIN opportunity_scores s ON s.id = h.opportunity_score_id
            WHERE s.segment = :seg
              AND lower(h.reason) = 'closed'
              AND date_trunc('month', h.snapshot_at) =
                  make_date(:yr, :mo, 1)::timestamptz
            """
        ),
        {"seg": segment, "yr": year, "mo": month},
    ).scalar() or 0

    actual_close_rate = float(closed_count) / sample_size if sample_size else 0.0

    # Actual time-to-cash: median snapshot_at - created_at for closed records
    actual_ttc_row = db.execute(
        text(
            """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM h.snapshot_at - s.created_at) / 86400
            )
            FROM opportunity_score_history h
            JOIN opportunity_scores s ON s.id = h.opportunity_score_id
            WHERE s.segment = :seg
              AND lower(h.reason) = 'closed'
              AND date_trunc('month', h.snapshot_at) =
                  make_date(:yr, :mo, 1)::timestamptz
            """
        ),
        {"seg": segment, "yr": year, "mo": month},
    ).scalar()

    actual_time_to_cash = float(actual_ttc_row) if actual_ttc_row is not None else 0.0

    return SegmentCalibration(
        segment=segment,
        predicted_p_close=round(avg_p_close, 4),
        actual_close_rate=round(actual_close_rate, 4),
        predicted_time_to_cash=round(avg_time_to_cash, 2),
        actual_time_to_cash=round(actual_time_to_cash, 2),
        sample_size=sample_size,
        computed_at=datetime.now(timezone.utc).isoformat(),
    )


def compute_monthly_calibration(
    db: Session,
    year: int,
    month: int,
) -> CalibrationSummary:
    """Compute CalibrationSummary for a given YYYY-MM."""
    segments = []
    for seg in _SEGMENTS:
        result = _compute_segment(db, seg, year, month)
        if result is not None:
            segments.append(result)

    return CalibrationSummary(
        month=f"{year}-{month:02d}",
        segments=segments,
        computed_at=datetime.now(timezone.utc).isoformat(),
        note=(
            "Vera reads this file independently. "
            "This service produces facts only — it does not write Vera's conclusions."
        ),
    )


def write_calibration_summary(summary: CalibrationSummary) -> Path:
    """Persist summary JSON to shared/facts/calibration_summary_YYYY_MM.json."""
    FACTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FACTS_DIR / f"calibration_summary_{summary['month'].replace('-', '_')}.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("calibration_service: wrote %s (%d segments)", out_path, len(summary["segments"]))
    return out_path


def run_monthly(year: Optional[int] = None, month: Optional[int] = None) -> Path:
    """
    Entry point: compute + persist calibration for the given month.
    Defaults to the previous calendar month if not specified.
    """
    now = datetime.now(timezone.utc)
    if year is None or month is None:
        # Default to previous month
        if now.month == 1:
            year, month = now.year - 1, 12
        else:
            year, month = now.year, now.month - 1

    with get_db_context() as db:
        summary = compute_monthly_calibration(db, year, month)

    return write_calibration_summary(summary)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    path = run_monthly()
    print(f"Calibration written to {path}")
