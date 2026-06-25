"""Stream self-diagnosis engine (Sprint 4.2).

classify_breach  — pure deterministic rules → structured dict
polish_summary   — LLM prose; templated fallback on any failure
run_metric_lifecycle — open/update/resolve one episode per (county, metric)
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.stream_diagnostics import (
    CONSECUTIVE_DAYS_TO_DIAGNOSE,
    RECOMMENDATION_PLAYBOOK,
    RED_AFTER_DAYS,
    RED_IF_BELOW_FRACTION_OF_TARGET,
    STREAM_METRICS,
)

from src.services.claude_router import call_claude_with_usage

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# classify_breach
# ─────────────────────────────────────────────────────────────────────────────

def classify_breach(
    metric_name: str,
    history: list[float],
    target: float,
    days_below: int,
) -> dict[str, Any]:
    """Deterministic rules → {severity, category, magnitude_pct, recommendations}."""
    latest = history[-1] if history else 0.0
    magnitude_pct = round((target - latest) / target * 100, 1) if target else 0.0

    severity = _compute_severity(latest, target, days_below)
    category = _classify_category(metric_name, latest, target, history)
    recs = RECOMMENDATION_PLAYBOOK.get(metric_name, {}).get(
        category,
        RECOMMENDATION_PLAYBOOK.get(metric_name, {}).get("default", []),
    )

    return {
        "severity": severity,
        "category": category,
        "magnitude_pct": magnitude_pct,
        "observed_value": latest,
        "target_value": target,
        "recommendations": recs,
    }


def _compute_severity(observed: float, target: float, days_below: int) -> str:
    if days_below >= RED_AFTER_DAYS:
        return "red"
    if target > 0 and observed < target * RED_IF_BELOW_FRACTION_OF_TARGET:
        return "red"
    return "yellow"


def _classify_category(
    metric_name: str,
    latest: float,
    target: float,
    history: list[float],
) -> str:
    playbook = RECOMMENDATION_PLAYBOOK.get(metric_name, {})
    known_categories = [k for k in playbook if k != "default"]

    # Simple heuristic: large sudden drop → skip_trace_degraded for pipeline metrics
    if len(history) >= 2:
        drop = history[-2] - history[-1] if len(history) >= 2 else 0
        if drop > 0.15 and "skip_trace_degraded" in known_categories:
            return "skip_trace_degraded"
        if drop > 0.10 and "carrier_filtering" in known_categories:
            return "carrier_filtering"

    if latest < target * 0.60 and "call_quality" in known_categories:
        return "call_quality"

    return "default"


# ─────────────────────────────────────────────────────────────────────────────
# polish_summary
# ─────────────────────────────────────────────────────────────────────────────

def polish_summary(structured: dict[str, Any]) -> str:
    """Try LLM-polished prose; fall back to template on any error."""
    try:
        prompt = (
            f"Write one concise paragraph (2-3 sentences) summarizing this stream health issue "
            f"for an operations dashboard. Be direct and specific.\n\n"
            f"Metric: {structured['metric_name']}\n"
            f"Observed: {structured['observed_value']:.2%} vs target {structured['target_value']:.2%}\n"
            f"Days below target: {structured['days_below']}\n"
            f"Category: {structured['category']}\n"
            f"Severity: {structured['severity']}"
        )
        result = call_claude_with_usage(
            task_type="haiku",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        return result["text"].strip()
    except Exception:
        logger.warning("LLM polish failed for %s — using template", structured.get("metric_name"))
        return _template_summary(structured)


def _template_summary(structured: dict[str, Any]) -> str:
    observed = structured.get("observed_value", 0)
    target = structured.get("target_value", 0)
    days = structured.get("days_below", 0)
    metric = structured.get("metric_name", "unknown")
    severity = structured.get("severity", "yellow")
    cat = structured.get("category", "default")

    pct_obs = f"{observed:.0%}" if isinstance(observed, float) else str(observed)
    pct_tgt = f"{target:.0%}" if isinstance(target, float) else str(target)

    return (
        f"{metric.replace('_', ' ').title()} is {pct_obs} against a target of {pct_tgt}, "
        f"now {days} day{'s' if days != 1 else ''} below threshold ({severity.upper()}). "
        f"Likely cause: {cat.replace('_', ' ')}."
    )


# ─────────────────────────────────────────────────────────────────────────────
# run_metric_lifecycle  (open / update / resolve one episode)
# ─────────────────────────────────────────────────────────────────────────────

def run_metric_lifecycle(
    db: Session,
    county_id: str,
    metric_name: str,
    today: date,
) -> dict[str, Any]:
    """Open, update, or resolve a stream_diagnostics episode for one metric.

    Returns a dict describing what action was taken.
    """
    cfg = STREAM_METRICS[metric_name]
    target = cfg["target"]
    stream = cfg["stream"]
    col = cfg["column"]

    # Read last CONSECUTIVE_DAYS_TO_DIAGNOSE rows for the metric column
    rows = db.execute(text(f"""
        SELECT {col}, run_date
        FROM platform_daily_stats
        WHERE county_id = :cid
          AND run_date <= :today
          AND {col} IS NOT NULL
        ORDER BY run_date DESC
        LIMIT :n
    """), {"cid": county_id, "today": today, "n": CONSECUTIVE_DAYS_TO_DIAGNOSE}).fetchall()

    values = [float(r[0]) for r in rows]
    streak_below = (
        len(values) >= CONSECUTIVE_DAYS_TO_DIAGNOSE
        and all(v < target for v in values)
    )
    latest_value = values[0] if values else None

    # Find open episode
    open_ep = db.execute(text("""
        SELECT id, days_below, detected_on
        FROM stream_diagnostics
        WHERE county_id = :cid AND metric_name = :mn AND resolved_on IS NULL
        ORDER BY detected_on DESC
        LIMIT 1
    """), {"cid": county_id, "mn": metric_name}).fetchone()

    if not streak_below and open_ep is None:
        return {"action": "noop", "metric_name": metric_name}

    if not streak_below and open_ep is not None:
        # Recover
        db.execute(text("""
            UPDATE stream_diagnostics
            SET resolved_on = :today, updated_at = NOW()
            WHERE id = :eid
        """), {"today": today, "eid": open_ep[0]})
        db.flush()
        return {"action": "resolved", "metric_name": metric_name}

    # streak_below = True
    days_below = len(values)  # approximation: how many recent rows are below
    structured = classify_breach(metric_name, values[::-1], target, days_below)
    summary = polish_summary({**structured, "metric_name": metric_name, "days_below": days_below})

    if open_ep is None:
        # Open new episode — guard against duplicate on same detected_on
        existing = db.execute(text("""
            SELECT id FROM stream_diagnostics
            WHERE county_id=:cid AND metric_name=:mn AND detected_on=:det
        """), {"cid": county_id, "mn": metric_name, "det": today}).fetchone()
        if existing:
            return {"action": "noop_dup", "metric_name": metric_name}

        db.execute(text("""
            INSERT INTO stream_diagnostics
                (county_id, stream, metric_name, severity, observed_value,
                 target_value, days_below, category, trend_summary,
                 recommendations, detected_on)
            VALUES (:cid, :stream, :mn, :sev, :obs, :tgt, :days,
                    :cat, :summary, cast(:recs as jsonb), :det)
        """), {
            "cid": county_id,
            "stream": stream,
            "mn": metric_name,
            "sev": structured["severity"],
            "obs": latest_value,
            "tgt": target,
            "days": days_below,
            "cat": structured["category"],
            "summary": summary,
            "recs": _json(structured["recommendations"]),
        "det": today,
        })
        db.flush()
        return {"action": "opened", "metric_name": metric_name}

    # Update existing open episode
    db.execute(text("""
        UPDATE stream_diagnostics
        SET days_below=:days, severity=:sev, observed_value=:obs,
            trend_summary=:summary, recommendations=cast(:recs as jsonb),
            updated_at=NOW()
        WHERE id=:eid
    """), {
        "days": days_below,
        "sev": structured["severity"],
        "obs": latest_value,
        "summary": summary,
        "recs": _json(structured["recommendations"]),
        "eid": open_ep[0],
    })
    db.flush()
    return {"action": "updated", "metric_name": metric_name}


def _json(value) -> str:
    import json
    return json.dumps(value)
