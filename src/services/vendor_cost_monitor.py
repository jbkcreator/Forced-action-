"""
Daily vendor cost monitor — aggregates vendor spend, computes baselines,
evaluates anomalies, and creates/extends pauses.

Pure calculation helpers are separated from DB orchestration for testability.
"""

import logging
import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.vendor_cost_caps import HARD_CAPS_USD, LOOKBACK_DAYS, MIN_HISTORY_DAYS
from src.core.models import ApiUsageLog, AgentDecision
from src.services.vendor_cost_attribution import resolve_pause_target

logger = logging.getLogger(__name__)

# Vendors we actively monitor for auto-pause
AUTO_PAUSE_VENDORS = {"claude", "anthropic"}  # Stripe and Telnyx are alert-only in v1

# Alert-only vendors (monitor + report, no auto-pause)
ALERT_ONLY_VENDORS = {"stripe", "telnyx", "twilio"}


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


# ══════════════════════════════════════════════════════════════════════════════
# Pure calculation helpers (testable without DB)
# ══════════════════════════════════════════════════════════════════════════════


def compute_baseline(daily_costs: list[float]) -> dict:
    """
    Given a list of daily cost totals (one per day, 0 = zero-spend day),
    compute baseline stats.

    Returns:
        {
            "avg": float | None,       # avg of non-zero days
            "stddev": float | None,    # stddev of non-zero days
            "non_zero_days": int,       # count of days with > 0 spend
            "total_days": int,          # total days in window
        }
    """
    non_zero = [c for c in daily_costs if c > 0]
    n = len(non_zero)
    total = len(daily_costs)

    if n == 0:
        return {"avg": None, "stddev": None, "non_zero_days": 0, "total_days": total}

    avg = sum(non_zero) / n
    if n >= 2:
        variance = sum((x - avg) ** 2 for x in non_zero) / (n - 1)
        stddev = math.sqrt(variance)
    else:
        stddev = 0.0

    return {
        "avg": avg,
        "stddev": stddev,
        "non_zero_days": n,
        "total_days": total,
    }


def detect_anomaly(
    today_cost: float,
    baseline: dict,
    pause_target: str,
    vendor: str,
) -> dict:
    """
    Determine if today's cost is anomalous.

    Returns:
        {
            "is_anomaly": bool,
            "threshold_usd": float | None,
            "reason": str,
            "anomaly_score": float | None,   # (today - avg) / stddev  or None
            "used_hard_cap": bool,
        }
    """
    non_zero_days = baseline["non_zero_days"]
    avg = baseline["avg"]
    stddev = baseline["stddev"]

    # Hard cap fallback when history is sparse
    if non_zero_days < MIN_HISTORY_DAYS:
        caps = HARD_CAPS_USD.get(pause_target, HARD_CAPS_USD.get("default", {}))
        hard_cap = caps.get("hard_cap_usd")
        if hard_cap is not None and today_cost > hard_cap:
            return {
                "is_anomaly": True,
                "threshold_usd": hard_cap,
                "reason": f"hard_cap_exceeded: ${today_cost:.2f} > ${hard_cap:.2f} "
                          f"(insufficient history: {non_zero_days}/{MIN_HISTORY_DAYS} days)",
                "anomaly_score": None,
                "used_hard_cap": True,
            }
        return {
            "is_anomaly": False,
            "threshold_usd": hard_cap,
            "reason": f"insufficient_history: {non_zero_days}/{MIN_HISTORY_DAYS} days, "
                      f"today=${today_cost:.2f} under hard cap",
            "anomaly_score": None,
            "used_hard_cap": True,
        }

    # Statistical anomaly detection: avg + 2 * stddev
    if avg is not None and stddev is not None:
        threshold = avg + 2 * stddev
        if today_cost > threshold:
            score = (today_cost - avg) / stddev if stddev > 0 else 0
            return {
                "is_anomaly": True,
                "threshold_usd": threshold,
                "reason": f"statistical: ${today_cost:.2f} > ${threshold:.2f} "
                          f"(avg=${avg:.4f}, stddev=${stddev:.4f}, z={score:.2f})",
                "anomaly_score": score,
                "used_hard_cap": False,
            }

        return {
            "is_anomaly": False,
            "threshold_usd": threshold,
            "reason": f"normal: ${today_cost:.2f} <= ${threshold:.2f}",
            "anomaly_score": None,
            "used_hard_cap": False,
        }

    return {
        "is_anomaly": False,
        "threshold_usd": None,
        "reason": "no_baseline_data",
        "anomaly_score": None,
        "used_hard_cap": False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# DB-backed aggregation and orchestration
# ══════════════════════════════════════════════════════════════════════════════


def aggregate_daily_spend(
    db: Session,
    target_date: Optional[date] = None,
    vendor: str = "claude",
) -> list[dict]:
    """
    Aggregate daily Claude spend per pause_target for a given date.

    Uses AgentDecision.cost_usd as the primary source (graph_name -> pause_target),
    falling back to ApiUsageLog.cost_usd (task_type -> pause_target).

    Returns list of { "pause_target": str, "cost_usd": float }
    """
    ref_date = target_date or _utc_today()
    day_start = datetime.combine(ref_date, datetime.min.time(), tzinfo=timezone.utc)
    day_end = datetime.combine(ref_date, datetime.max.time(), tzinfo=timezone.utc)

    results: dict[str, float] = {}

    # 1. Aggregate from AgentDecision (primary: graph_name attribution)
    graph_rows = db.execute(
        select(
            AgentDecision.graph_name,
            func.sum(AgentDecision.cost_usd),
        ).where(
            AgentDecision.started_at >= day_start,
            AgentDecision.started_at <= day_end,
            AgentDecision.cost_usd > 0,
        ).group_by(AgentDecision.graph_name)
    ).all()

    for graph_name, cost in graph_rows:
        cost_float = float(cost) if cost else 0.0
        pause_target = resolve_pause_target(graph_name=graph_name)
        target = pause_target or f"graph:{graph_name}"
        results[target] = results.get(target, 0.0) + cost_float

    # 2. Aggregate from ApiUsageLog (fallback: task_type + graph_name)
    log_rows = db.execute(
        select(
            ApiUsageLog.graph_name,
            ApiUsageLog.task_type,
            func.sum(ApiUsageLog.cost_usd),
        ).where(
            ApiUsageLog.service == vendor,
            ApiUsageLog.created_at >= day_start,
            ApiUsageLog.created_at <= day_end,
            ApiUsageLog.cost_usd > 0,
        ).group_by(ApiUsageLog.graph_name, ApiUsageLog.task_type)
    ).all()

    for graph_name, task_type, cost in log_rows:
        cost_float = float(cost) if cost else 0.0
        pause_target = resolve_pause_target(graph_name=graph_name, task_type=task_type)
        target = pause_target or (
            f"task:{task_type}" if task_type else
            f"graph:{graph_name}" if graph_name else
            "unattributed"
        )
        results[target] = results.get(target, 0.0) + cost_float

    return [{"pause_target": pt, "cost_usd": c} for pt, c in sorted(results.items())]


def get_historical_baseline(
    db: Session,
    pause_target: str,
    vendor: str = "claude",
    lookback_days: int = LOOKBACK_DAYS,
    exclude_date: Optional[date] = None,
) -> dict:
    """
    Compute baseline stats from the trailing N days (excluding today).

    Queries both AgentDecision and ApiUsageLog for the given pause_target.
    Returns the same shape as compute_baseline().
    """
    ref_date = exclude_date or _utc_today()
    window_start = ref_date - timedelta(days=lookback_days)

    daily_costs: dict[date, float] = {}

    # AgentDecision costs (by graph_name)
    for graph_name in _graph_names_for_target(pause_target):
        rows = db.execute(
            select(
                func.date(AgentDecision.started_at),
                func.sum(AgentDecision.cost_usd),
            ).where(
                AgentDecision.graph_name == graph_name,
                AgentDecision.started_at >= datetime.combine(window_start, datetime.min.time(), tzinfo=timezone.utc),
                AgentDecision.started_at < datetime.combine(ref_date, datetime.min.time(), tzinfo=timezone.utc),
                AgentDecision.cost_usd > 0,
            ).group_by(func.date(AgentDecision.started_at))
        ).all()
        for d, cost in rows:
            daily_costs[d] = daily_costs.get(d, 0.0) + float(cost or 0.0)

    # ApiUsageLog costs (by pause_target directly)
    rows = db.execute(
        select(
            func.date(ApiUsageLog.created_at),
            func.sum(ApiUsageLog.cost_usd),
        ).where(
            ApiUsageLog.service == vendor,
            ApiUsageLog.pause_target == pause_target,
            ApiUsageLog.created_at >= datetime.combine(window_start, datetime.min.time(), tzinfo=timezone.utc),
            ApiUsageLog.created_at < datetime.combine(ref_date, datetime.min.time(), tzinfo=timezone.utc),
            ApiUsageLog.cost_usd > 0,
            ApiUsageLog.blocked_by_pause == False,
        ).group_by(func.date(ApiUsageLog.created_at))
    ).all()
    for d, cost in rows:
        daily_costs[d] = daily_costs.get(d, 0.0) + float(cost or 0.0)

    # Fill in zero-cost days
    all_costs = []
    for i in range(lookback_days):
        day = ref_date - timedelta(days=i + 1)
        all_costs.append(daily_costs.get(day, 0.0))

    return compute_baseline(all_costs)


def _graph_names_for_target(pause_target: str) -> list[str]:
    """Resolve which graph_names map to a given pause_target."""
    from src.services.vendor_cost_attribution import GRAPH_TO_PAUSE_TARGET
    return [g for g, t in GRAPH_TO_PAUSE_TARGET.items() if t == pause_target]


# ══════════════════════════════════════════════════════════════════════════════
# Daily monitor orchestration
# ══════════════════════════════════════════════════════════════════════════════


def run_daily_monitor(db: Session, dry_run: bool = False) -> dict:
    """
    Run the full daily vendor cost monitor cycle:

    1. Auto-resume expired pauses
    2. Aggregate today's vendor spend
    3. Compute baselines and detect anomalies
    4. Create/extend pauses for anomalous targets (Claude only in v1)
    5. Return a report dict

    Returns:
        {
            "auto_resumed": int,
            "aggregated": [ { pause_target, cost_usd }, ... ],
            "anomalies": [ { pause_target, cost_usd, baseline, detection }, ... ],
            "pauses_created": int,
            "pauses_extended": int,
            "errors": [ str, ... ],
        }
    """
    from src.services.vendor_cost_pause_service import (
        auto_resume_expired,
        create_pause,
        get_active_pause,
    )

    result: dict = {
        "auto_resumed": 0,
        "aggregated": [],
        "anomalies": [],
        "pauses_created": 0,
        "pauses_extended": 0,
        "errors": [],
    }

    try:
        # Step 1: Auto-resume expired pauses
        result["auto_resumed"] = auto_resume_expired(db)
    except Exception as exc:
        logger.error("vendor_cost_monitor: auto-resume failed: %s", exc)
        result["errors"].append(f"auto_resume: {exc}")

    try:
        # Step 2: Aggregate today's spend
        aggregated = aggregate_daily_spend(db)
        result["aggregated"] = aggregated
    except Exception as exc:
        logger.error("vendor_cost_monitor: aggregation failed: %s", exc)
        result["errors"].append(f"aggregation: {exc}")
        aggregated = []

    # Step 3-4: For each vendor/pause_target, detect anomalies and pause
    for vendor in AUTO_PAUSE_VENDORS:
        for entry in aggregated:
            pause_target = entry["pause_target"]
            today_cost = entry["cost_usd"]

            try:
                baseline = get_historical_baseline(db, pause_target, vendor=vendor)
                detection = detect_anomaly(today_cost, baseline, pause_target, vendor)

                anomaly_entry = {
                    "pause_target": pause_target,
                    "cost_usd": today_cost,
                    "baseline": baseline,
                    "detection": detection,
                }
                result["anomalies"].append(anomaly_entry)

                if detection["is_anomaly"]:
                    logger.info(
                        "vendor_cost_monitor: ANOMALY vendor=%s target=%s cost=%.4f %s",
                        vendor, pause_target, today_cost, detection["reason"],
                    )

                    if not dry_run:
                        existing = get_active_pause(db, vendor, pause_target, use_cache=False)
                        if existing:
                            from src.services.vendor_cost_pause_service import extend_pause
                            extend_pause(
                                db, existing,
                                anomaly_score=detection.get("anomaly_score"),
                                today_cost_usd=today_cost,
                                baseline_avg_usd=baseline.get("avg"),
                                baseline_stddev_usd=baseline.get("stddev"),
                                threshold_usd=detection.get("threshold_usd"),
                                sample_n=baseline.get("non_zero_days"),
                            )
                            result["pauses_extended"] += 1
                        else:
                            create_pause(
                                db,
                                vendor=vendor,
                                pause_target=pause_target,
                                reason=detection["reason"],
                                anomaly_score=detection.get("anomaly_score"),
                                today_cost_usd=today_cost,
                                baseline_avg_usd=baseline.get("avg"),
                                baseline_stddev_usd=baseline.get("stddev"),
                                threshold_usd=detection.get("threshold_usd"),
                                sample_n=baseline.get("non_zero_days"),
                                window_days=LOOKBACK_DAYS,
                                source_table="agent_decisions",
                                metadata_json={"vendor": vendor, "detection": detection},
                            )
                            result["pauses_created"] += 1

            except Exception as exc:
                logger.error(
                    "vendor_cost_monitor: anomaly check failed vendor=%s target=%s: %s",
                    vendor, pause_target, exc,
                )
                result["errors"].append(f"anomaly:{vendor}/{pause_target}: {exc}")

    # Step 5: Run alert-only vendor aggregation (Stripe, Telnyx) — no pausing
    for vendor in ALERT_ONLY_VENDORS:
        try:
            alert_aggregated = aggregate_daily_spend(db, vendor=vendor)
            for entry in alert_aggregated:
                # Just record for reporting — no pause created
                result.setdefault("alert_only", []).append({
                    "vendor": vendor,
                    "pause_target": entry["pause_target"],
                    "cost_usd": entry["cost_usd"],
                })
        except Exception as exc:
            logger.warning(
                "vendor_cost_monitor: alert-only aggregation failed for %s: %s",
                vendor, exc,
            )

    result["dry_run"] = dry_run
    return result