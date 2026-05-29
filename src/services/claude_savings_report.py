"""
Counterfactual savings report — D3.

Computes cost savings vs naive "all-Sonnet" policy from the cost ledger
(api_usage_logs). Used by Revenue Pulse and standalone reporting.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import ApiUsageLog


def compute_savings_report(
    db: Session,
    since_hours: int = 24,
) -> dict:
    """
    Compute counterfactual savings from api_usage_logs.

    Returns:
        {
            'actual_cost': float,
            'all_sonnet_cost': float,
            'savings_pct': float,
            'haiku_share': float,
            'calls': int,
            'by_task': list[dict],
        }
    """
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    rows = db.execute(
        select(ApiUsageLog).where(
            ApiUsageLog.service == "claude",
            ApiUsageLog.blocked_by_pause == False,
            ApiUsageLog.created_at >= since,
        )
    ).scalars().all()

    if not rows:
        return {
            "actual_cost": 0.0,
            "all_sonnet_cost": 0.0,
            "savings_pct": 0.0,
            "haiku_share": 0.0,
            "calls": 0,
            "by_task": [],
        }

    actual_cost = 0.0
    all_sonnet_cost = 0.0
    haiku_count = 0
    by_task: dict[str, dict] = {}

    for row in rows:
        cost = float(row.cost_usd or 0)
        inp = row.input_tokens or 0
        out = row.output_tokens or 0

        actual_cost += cost
        all_sonnet_cost += (inp * 3.00 + out * 15.00) / 1_000_000

        if row.model == "haiku":
            haiku_count += 1

        task = row.task_type or "unknown"
        if task not in by_task:
            by_task[task] = {"calls": 0, "actual_cost": 0.0, "all_sonnet_cost": 0.0}
        by_task[task]["calls"] += 1
        by_task[task]["actual_cost"] += cost
        by_task[task]["all_sonnet_cost"] += (inp * 3.00 + out * 15.00) / 1_000_000

    total_calls = len(rows)
    haiku_share = haiku_count / total_calls if total_calls else 0.0

    if all_sonnet_cost > 0:
        savings_pct = 1.0 - (actual_cost / all_sonnet_cost)
    else:
        savings_pct = 0.0

    by_task_list = [
        {
            "task_type": task,
            "calls": data["calls"],
            "actual_cost": round(data["actual_cost"], 4),
            "all_sonnet_cost": round(data["all_sonnet_cost"], 4),
            "savings_pct": round(1.0 - data["actual_cost"] / data["all_sonnet_cost"], 4) if data["all_sonnet_cost"] > 0 else 0.0,
        }
        for task, data in sorted(by_task.items(), key=lambda x: x[1]["actual_cost"], reverse=True)
    ]

    return {
        "actual_cost": round(actual_cost, 4),
        "all_sonnet_cost": round(all_sonnet_cost, 4),
        "savings_pct": round(savings_pct, 4),
        "haiku_share": round(haiku_share, 4),
        "calls": total_calls,
        "by_task": by_task_list,
    }


def format_savings_sms(report: dict) -> str:
    """Format savings report as founder-facing SMS line."""
    if report["calls"] == 0:
        return "Claude cost: no data yet"

    savings_pct = report["savings_pct"] * 100
    haiku_share_pct = report["haiku_share"] * 100
    return f"Claude: ${report['actual_cost']:.2f} actual, ${report['all_sonnet_cost']:.2f} if all-Sonnet ({savings_pct:.0f}% saved, {haiku_share_pct:.0f}% Haiku)"