"""
Gate metrics aggregator.

Queries agent_decisions to produce daily pass/fail/abort rate summaries
per graph_name. Results are returned as dicts and logged. Intended for
ops dashboards and alerting.

Cron: 0 7 * * * (7:00 UTC daily, after metric ingest at 6:00)

Usage:
    python -m src.tasks.gate_metrics_aggregator [--dry-run] [--days=7]
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import AgentDecision

logger = logging.getLogger(__name__)


def compute_gate_metrics(db: Session, days: int = 1) -> List[dict]:
    """
    Aggregate agent_decisions for the last `days` days.
    Returns one row per graph_name with pass/fail/abort/unknown counts
    and pass_rate_pct.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    rows = db.execute(
        select(
            AgentDecision.graph_name,
            AgentDecision.terminal_status,
            func.count(AgentDecision.decision_id).label("cnt"),
        )
        .where(AgentDecision.started_at >= since)
        .group_by(AgentDecision.graph_name, AgentDecision.terminal_status)
        .order_by(AgentDecision.graph_name, AgentDecision.terminal_status)
    ).all()

    # Pivot into per-graph buckets
    graphs: dict = {}
    for graph_name, status, cnt in rows:
        if graph_name not in graphs:
            graphs[graph_name] = {"graph_name": graph_name, "total": 0, "completed": 0,
                                  "aborted": 0, "failed": 0, "escalated": 0, "null_status": 0}
        key = status if status in ("completed", "aborted", "failed", "escalated") else "null_status"
        graphs[graph_name][key] += cnt
        graphs[graph_name]["total"] += cnt

    results = []
    for g in graphs.values():
        total = g["total"]
        completed = g["completed"]
        g["pass_rate_pct"] = round(completed / total * 100, 1) if total > 0 else 0.0
        g["abort_rate_pct"] = round(g["aborted"] / total * 100, 1) if total > 0 else 0.0
        g["fail_rate_pct"] = round(g["failed"] / total * 100, 1) if total > 0 else 0.0
        results.append(g)

    return sorted(results, key=lambda x: x["graph_name"])


def compute_block_reasons(db: Session, days: int = 7) -> List[dict]:
    """
    Query agent_decisions.summary->>'action_blocked_reason' for the last N days.
    Returns top block reasons with counts.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Use Postgres JSONB operator to extract block reason
    rows = db.execute(
        text("""
            SELECT
                summary->>'action_blocked_reason' AS reason,
                COUNT(*) AS cnt
            FROM agent_decisions
            WHERE started_at >= :since
              AND terminal_status = 'aborted'
              AND summary->>'action_blocked_reason' IS NOT NULL
            GROUP BY reason
            ORDER BY cnt DESC
            LIMIT 20
        """),
        {"since": since},
    ).all()

    return [{"reason": r.reason, "count": r.cnt} for r in rows]


def run_gate_metrics_aggregator(dry_run: bool = False, days: int = 1) -> dict:
    """Compute gate metrics for the last N days. Returns summary dict."""
    with get_db_context() as db:
        metrics = compute_gate_metrics(db, days=days)
        block_reasons = compute_block_reasons(db, days=max(days, 7))

    result = {"days": days, "graphs": metrics, "top_block_reasons": block_reasons}

    if dry_run:
        logger.info("[GateMetricsAggregator] dry_run:\n%s", json.dumps(result, indent=2))
    else:
        for g in metrics:
            if g["pass_rate_pct"] < 90:
                logger.warning(
                    "[GateMetricsAggregator] LOW PASS RATE graph=%s pass_rate=%.1f%% total=%d",
                    g["graph_name"], g["pass_rate_pct"], g["total"],
                )
            else:
                logger.info(
                    "[GateMetricsAggregator] graph=%s pass_rate=%.1f%% total=%d",
                    g["graph_name"], g["pass_rate_pct"], g["total"],
                )

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    days_arg = next((int(a.split("=")[1]) for a in sys.argv if a.startswith("--days=")), 1)
    result = run_gate_metrics_aggregator(dry_run=dry, days=days_arg)
    print(json.dumps(result, indent=2, default=str))
