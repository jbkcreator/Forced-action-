"""
QUALITY-v2.2 Q2 — Agent P&L monthly rollup service.

Aggregates four cost terms per seat from existing DB tables, computes
net contribution, identifies the binding constraint, and formats the
monthly email. Called by src/tasks/agent_pnl_monthly.py.

Seat definitions (decision C1 from QUALITY-v2.2 analysis):
  vera       — no LLM, no vendor spend; attributed GP only (currently $0)
  cora       — Claude drafting/classification across three graph names
  hunter     — buyer_entity_match Haiku calls (NULL graph_name, task_type match)
  relay      — Telnyx SMS cost logged via sms_compliance.send_sms
  dev_shop   — manual cost entries only (contractor invoices)
  lifecycle  — 14 existing autonomous-send graphs (the ex-Lifecycle runtime)

Banks excluded (decision C1-Banks) — no FA code exists; runs on a personal budget.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from dateutil.relativedelta import relativedelta
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

HUNTER_TASK_TYPE = "buyer_entity_match"

SEAT_GRAPH_MAP: dict[str, list[str]] = {
    "vera": [],
    "cora": ["cora_outreach", "cora_pre_call", "cora_reply"],
    "hunter": [],   # aggregated by HUNTER_TASK_TYPE (graph_name IS NULL)
    "relay": [],    # Telnyx — aggregated by service='telnyx' AND task_type='telnyx_sms'
    "dev_shop": [], # manual cost entries only
    "lifecycle": [
        "fomo",
        "abandonment_wave1",
        "abandonment_wave2",
        "retention",
        "wallet_to_lock_close",
        "accelerated_wallet_push",
        "ap_lite_close",
        "human_close_route",
        "synthflow_voice_drop",
        "new_lead_voice_call",
        "nws_urgency",
        "reactivation",
        "dfy_lite_pink",
        "quora_channel",
    ],
}

# §9.5: "report the binding constraint BY NAME every month."
_CONSTRAINT_LABELS: dict[str, str] = {
    "compute_cost_cents":         "compute",
    "data_cost_cents":            "data_supply",
    "founder_minutes_cost_cents": "founder_time",
}


def rollup_month(
    session: Session,
    period_month: date,
    settings=None,
) -> list[dict[str, Any]]:
    """Compute one month's P&L for all six seats.

    Args:
        session:      DB session for all reads.
        period_month: First-of-month date (e.g. date(2026, 7, 1)).
        settings:     AgentsSettings instance; defaults to get_settings() if None.

    Returns a list of dicts, one per seat, keyed by agent_pnl column names.
    """
    if settings is None:
        from config.agents import AgentsSettings
        from config.settings import get_settings
        _base = get_settings()
        settings = AgentsSettings(**_base.model_dump(exclude_unset=False))

    period_start = datetime(period_month.year, period_month.month, 1, tzinfo=timezone.utc)
    period_end = period_start + relativedelta(months=1)

    founder_cost_per_approval = (
        settings.founder_minutes_per_approval * settings.founder_minute_rate_cents
    )

    rows = []
    for seat in SEAT_GRAPH_MAP:
        compute = _compute_cost(session, seat, period_start, period_end)
        data = _data_cost(session, seat, period_start, period_end)
        manual = _manual_cost(session, seat, period_month)
        gp = _attributed_gp(session, seat, period_start, period_end)
        approvals = _approval_count(session, seat, period_start, period_end)
        dwell = _queue_dwell_median(session, seat, period_start, period_end)

        founder_minutes_cost = approvals * founder_cost_per_approval
        total_data = data + manual
        net = gp - compute - total_data - founder_minutes_cost
        binding = _binding_constraint(compute, total_data, founder_minutes_cost)

        rows.append({
            "seat": seat,
            "period_month": period_month,
            "attributed_gp_cents": gp,
            "compute_cost_cents": compute,
            "data_cost_cents": total_data,
            "founder_minutes_cost_cents": founder_minutes_cost,
            "net_contribution_cents": net,
            "binding_constraint": binding,
            "approval_count": approvals,
            "queue_dwell_median_minutes": dwell,
        })
        logger.info(
            "agent_pnl rollup: seat=%s period=%s gp=%d compute=%d data=%d "
            "founder_min=%d net=%d constraint=%s",
            seat, period_month, gp, compute, total_data, founder_minutes_cost, net, binding,
        )
    return rows


def _compute_cost(session: Session, seat: str, start: datetime, end: datetime) -> int:
    graphs = SEAT_GRAPH_MAP[seat]

    if seat == "hunter" and not graphs:
        row = session.execute(sa_text("""
            SELECT COALESCE(SUM(cost_usd), 0) AS total
            FROM api_usage_logs
            WHERE task_type = :task_type
              AND graph_name IS NULL
              AND created_at >= :start AND created_at < :end
              AND cost_usd IS NOT NULL
        """), {"task_type": HUNTER_TASK_TYPE, "start": start, "end": end}).fetchone()
        return int(round(float(row.total) * 100))

    if seat == "relay" and not graphs:
        row = session.execute(sa_text("""
            SELECT COALESCE(SUM(cost_usd), 0) AS total
            FROM api_usage_logs
            WHERE service = 'telnyx' AND task_type = 'telnyx_sms'
              AND created_at >= :start AND created_at < :end
              AND cost_usd IS NOT NULL
        """), {"start": start, "end": end}).fetchone()
        return int(round(float(row.total) * 100))

    if not graphs:
        return 0

    row = session.execute(sa_text("""
        SELECT COALESCE(SUM(cost_usd), 0) AS total
        FROM api_usage_logs
        WHERE graph_name = ANY(:graphs)
          AND created_at >= :start AND created_at < :end
          AND cost_usd IS NOT NULL
    """), {"graphs": graphs, "start": start, "end": end}).fetchone()
    return int(round(float(row.total) * 100))


def _data_cost(session: Session, seat: str, start: datetime, end: datetime) -> int:
    row = session.execute(sa_text("""
        SELECT COALESCE(SUM(cost_cents), 0) AS total
        FROM enrichment_usage_logs
        WHERE caller = :seat
          AND created_at >= :start AND created_at < :end
    """), {"seat": seat, "start": start, "end": end}).fetchone()
    return int(row.total)


def _manual_cost(session: Session, seat: str, period_month: date) -> int:
    row = session.execute(sa_text("""
        SELECT COALESCE(SUM(amount_cents), 0) AS total
        FROM agent_manual_cost_entries
        WHERE seat = :seat AND period_month = :month
    """), {"seat": seat, "month": period_month}).fetchone()
    return int(row.total)


def _attributed_gp(session: Session, seat: str, start: datetime, end: datetime) -> int:
    """Returns 0 until platform_revenue_ledger.opportunity_thread_id is populated.

    The column exists (added by migrations/apply_agent_pnl.py); population through
    _on_checkout_completed is deferred as a follow-on task.
    """
    return 0


def _approval_count(session: Session, seat: str, start: datetime, end: datetime) -> int:
    """Only Cora's drafts go through relay_approval_queue today."""
    if seat != "cora":
        return 0
    row = session.execute(sa_text("""
        SELECT COUNT(*) AS cnt
        FROM relay_approval_queue
        WHERE status IN ('approved', 'rejected', 'sent', 'failed', 'skipped')
          AND decided_at >= :start AND decided_at < :end
    """), {"start": start, "end": end}).fetchone()
    return int(row.cnt)


def _queue_dwell_median(
    session: Session, seat: str, start: datetime, end: datetime
) -> float | None:
    """Median queue dwell in minutes — latency metric, not cost (§5c)."""
    if seat != "cora":
        return None
    row = session.execute(sa_text("""
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (decided_at - created_at)) / 60
        ) AS median_minutes
        FROM relay_approval_queue
        WHERE decided_at >= :start AND decided_at < :end
          AND decided_at IS NOT NULL
    """), {"start": start, "end": end}).fetchone()
    return float(row.median_minutes) if row.median_minutes is not None else None


def _binding_constraint(compute: int, data: int, founder_minutes: int) -> str | None:
    """Name the largest cost term. Returns None when all terms are zero."""
    terms = {
        "compute":       compute,
        "data_supply":   data,
        "founder_time":  founder_minutes,
    }
    if all(v == 0 for v in terms.values()):
        return None
    return max(terms, key=lambda k: terms[k])


def build_monthly_email(rollup: list[dict[str, Any]], period_month: date) -> str:
    """Format the monthly P&L rollup as a plain-text email body."""
    lines = [
        f"Agent Fleet P&L — {period_month.strftime('%B %Y')}",
        "=" * 50,
        "",
    ]
    for row in sorted(rollup, key=lambda r: r["seat"]):
        net = row["net_contribution_cents"]
        sign = "+" if net >= 0 else "-"
        lines += [
            f"  {row['seat'].upper()}",
            f"    Attributed GP :  ${row['attributed_gp_cents'] / 100:,.2f}",
            f"    Compute       : -${row['compute_cost_cents'] / 100:,.2f}",
            f"    Data          : -${row['data_cost_cents'] / 100:,.2f}",
            f"    Founder time  : -${row['founder_minutes_cost_cents'] / 100:,.2f}"
            f"  ({row['approval_count']} approvals)",
            f"    Net           :  {sign}${abs(net) / 100:,.2f}",
            f"    Constraint    :  {row['binding_constraint'] or 'n/a'}",
        ]
        if row.get("queue_dwell_median_minutes") is not None:
            lines.append(
                f"    Queue dwell   :  {row['queue_dwell_median_minutes']:.0f} min median"
            )
        lines.append("")
    lines += [
        "Note: Attributed GP shows $0 until opportunity_thread_id is wired",
        "through checkout (follow-on task).",
        "",
    ]
    return "\n".join(lines)
