"""
Churn Validation Report — Phase 7 (fa051).

Reads backfilled churn_predictions rows to compute:
  - Precision / recall of high-risk flags vs realized Inactivity Onset
  - Median lead time (days flagged before onset)
  - Intervention lift: Inactivity Onset rate in holdout arm vs saved arm

Runs weekly (Monday 13:00 UTC) or on demand.

Usage:
    python -m src.tasks.churn_validation_report [--json]
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import select

from src.core.database import get_db_context
from src.core.models import ChurnPrediction

logger = logging.getLogger(__name__)

_MIN_SAMPLE = 30  # minimum labeled rows for a meaningful report


def compute_report(db) -> dict:
    """Build the validation report from backfilled churn_predictions rows.

    Returns a dict with keys: precision, recall, median_lead_time_days,
    lift, sample_size, holdout_n, saved_n, insufficient_sample.
    """
    rows = db.execute(
        select(ChurnPrediction).where(
            ChurnPrediction.was_correct.isnot(None),
            ChurnPrediction.realized_inactive_at.isnot(None),
        )
    ).scalars().all()

    if len(rows) < _MIN_SAMPLE:
        return {
            "insufficient_sample": True,
            "sample_size": len(rows),
            "min_required": _MIN_SAMPLE,
        }

    # ── Precision / Recall ────────────────────────────────────────────────
    # True positive: predicted onset (was_correct=True, not holdout)
    # False positive: predicted but subscriber stayed active (was_correct=False)
    # Recall: share of actual onsets that were predicted
    high_risk_rows = [r for r in rows if r.churn_risk_band in ("high", "very_high")]
    tp = sum(1 for r in high_risk_rows if r.was_correct)
    fp = sum(1 for r in high_risk_rows if not r.was_correct)
    # Actual positives = all rows where subscriber went inactive
    actual_positives = sum(1 for r in rows if r.was_correct)

    precision = tp / max(tp + fp, 1)
    recall = tp / max(actual_positives, 1)

    # ── Lead Time ─────────────────────────────────────────────────────────
    lead_times = []
    for row in high_risk_rows:
        if row.was_correct and row.predicted_inactivity_at and row.realized_inactive_at:
            predicted = row.predicted_inactivity_at
            realized = row.realized_inactive_at
            if predicted.tzinfo is None:
                predicted = predicted.replace(tzinfo=timezone.utc)
            if realized.tzinfo is None:
                realized = realized.replace(tzinfo=timezone.utc)
            lead_days = (realized - row.predicted_at).total_seconds() / 86400
            lead_times.append(lead_days)

    median_lead_time = _median(lead_times) if lead_times else None

    # ── Intervention Lift (holdout arm only — clean labels) ───────────────
    holdout_rows = [r for r in high_risk_rows if r.in_holdout]
    saved_rows = [r for r in high_risk_rows if not r.in_holdout and r.save_offer_sent_at]

    holdout_n = len(holdout_rows)
    saved_n = len(saved_rows)

    holdout_onset_rate = sum(1 for r in holdout_rows if r.was_correct) / max(holdout_n, 1)
    saved_onset_rate = sum(1 for r in saved_rows if r.was_correct) / max(saved_n, 1)
    lift = holdout_onset_rate - saved_onset_rate if holdout_n > 0 and saved_n > 0 else None

    return {
        "insufficient_sample": False,
        "sample_size": len(rows),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "median_lead_time_days": round(median_lead_time, 2) if median_lead_time is not None else None,
        "holdout_n": holdout_n,
        "saved_n": saved_n,
        "holdout_onset_rate": round(holdout_onset_rate, 4) if holdout_n > 0 else None,
        "saved_onset_rate": round(saved_onset_rate, 4) if saved_n > 0 else None,
        "lift": round(lift, 4) if lift is not None else None,
    }


def _median(values: list) -> float:
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def run(as_json: bool = False) -> dict:
    with get_db_context() as db:
        report = compute_report(db)

    if as_json:
        print(json.dumps(report, indent=2, default=str))
    else:
        if report.get("insufficient_sample"):
            logger.warning(
                "[ChurnValidation] Insufficient sample: %d/%d labeled rows",
                report["sample_size"],
                report["min_required"],
            )
        else:
            logger.info(
                "[ChurnValidation] precision=%.3f recall=%.3f lead_time=%s lift=%s "
                "holdout_n=%d saved_n=%d",
                report["precision"],
                report["recall"],
                report.get("median_lead_time_days"),
                report.get("lift"),
                report["holdout_n"],
                report["saved_n"],
            )
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    as_json = "--json" in sys.argv
    run(as_json=as_json)
    sys.exit(0)
