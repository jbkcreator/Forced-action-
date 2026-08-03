"""
Read-only helpers for the weekly Kill-Switch Scorecard.

Provides:
  consecutive_red_days  — true pure-red trailing streak from daily snapshots
  latest_color          — current color (snapshot or Redis fallback)
  open_incident_for     — open lifecycle_incident row for a (metric, county)
"""
from __future__ import annotations

from typing import Optional, Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.lifecycle_guardrails import KILL_SWITCH
from src.services.kill_switch_grade import grade
from src.tasks.kill_switch_metric_ingest import _BASELINE_COLUMNS, get_cached_metric

# Metrics that have a daily column in platform_daily_stats and therefore
# support a true consecutive-red-day streak.
SNAPSHOTTED_METRICS = set(_BASELINE_COLUMNS.keys())


def consecutive_red_days(
    db: Session,
    metric_name: str,
    county_id: str,
    max_window: int = 7,
) -> Optional[int]:
    """Return the trailing run of consecutive pure-red days for metric_name.

    Returns None if the metric has no daily column (caller renders 'streak n/a').
    Returns 0 if the metric is snapshotted but the most recent day is not red.
    """
    col = _BASELINE_COLUMNS.get(metric_name)
    if col is None:
        return None

    rows = db.execute(sa_text(f"""
        SELECT {col} AS val
        FROM platform_daily_stats
        WHERE county_id = :county_id
          AND {col} IS NOT NULL
        ORDER BY run_date DESC
        LIMIT :window
    """), {"county_id": county_id, "window": max_window}).fetchall()

    streak = 0
    for row in rows[:max_window]:
        if grade(metric_name, row.val) == "red":
            streak += 1
        else:
            break
    return streak


def latest_color(
    db: Session,
    metric_name: str,
    county_id: str,
) -> tuple[str, Optional[float]]:
    """Return (color, observed_value) for metric_name in county_id.

    Prefers the most-recent daily snapshot row; falls back to the live
    Redis-cached value when no snapshot row exists for today.
    """
    col = _BASELINE_COLUMNS.get(metric_name)
    observed: Optional[float] = None

    if col is not None:
        row = db.execute(sa_text(f"""
            SELECT {col} AS val
            FROM platform_daily_stats
            WHERE county_id = :county_id
              AND {col} IS NOT NULL
            ORDER BY run_date DESC
            LIMIT 1
        """), {"county_id": county_id}).first()
        if row is not None:
            observed = float(row.val) if row.val is not None else None

    # Fallback: live Redis value (also covers metrics not in _BASELINE_COLUMNS).
    if observed is None:
        observed = get_cached_metric(metric_name, county_id=county_id)

    return grade(metric_name, observed), observed


def open_incident_for(
    db: Session,
    metric_name: str,
    county_id: str,
) -> Optional[Any]:
    """Return the open lifecycle_incident row for (metric_name, county_id), or None.

    Raw SQL only. Used by the scorecard to annotate kill-rec state.
    """
    return db.execute(sa_text("""
        SELECT id, metric_name, county_id, severity,
               observed_value, threshold_value, breach_started,
               action_taken
        FROM lifecycle_incident
        WHERE metric_name = :metric
          AND county_id = :county
          AND breach_resolved IS NULL
        ORDER BY breach_started DESC
        LIMIT 1
    """), {"metric": metric_name, "county": county_id}).first()
