"""Operator Dashboard aggregation layer (T-B8-01).

Reads across existing metrics services and tables — computes no new ledger.
Each KPI is its own small function so a single KPI can be swapped later
without touching the others.

Four KPIs (`deals_submitted`, `lender_matches`, `loans_funded`,
`commissions_owed`) have no backing table yet — they depend on Block 5
(`investor_deals`, lender-matrix engine) and Block 7 (referral commission
ledger), which ship later and are gated on RESPA clearance / a signed
lender. Each returns `_unavailable(reason)` for now. Swapping one in later
is a one-function change: replace that KPI's function body with a real
query and update the call site below — nothing else in this module or in
the API contract changes shape.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.revenue_metrics import compute_revenue_metrics


def _unavailable(reason: str) -> dict:
    return {"available": False, "reason": reason}


def _kpi_mrr(rm: dict) -> dict:
    return {"available": True, "value_cents": rm["mrr_cents"]}


def _kpi_new_accounts(db: Session, frm: datetime, to: datetime) -> dict:
    n = db.execute(text(
        "SELECT COUNT(*) FROM mrr_movements "
        "WHERE movement_type = 'new' AND effective_at >= :frm AND effective_at < :to"
    ), {"frm": frm, "to": to}).scalar()
    return {"available": True, "value": int(n or 0)}


def _kpi_churn_risk(db: Session) -> dict:
    n = db.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (subscriber_id) subscriber_id, churn_risk_band
            FROM churn_predictions
            ORDER BY subscriber_id, predicted_at DESC
        ) latest
        WHERE latest.churn_risk_band IN ('high', 'very_high')
    """)).scalar()
    return {"available": True, "value": int(n or 0)}


def _kpi_leads_delivered(rm: dict) -> dict:
    return {"available": True, "value": sum(rm["leads_delivered_by_grade"].values())}


def _kpi_activation(rm: dict) -> dict:
    return {
        "available": True,
        "free_to_paid_rate": rm["free_to_paid_rate"],
        "avg_days_to_convert": rm["avg_time_to_convert_days"],
        "note": "proxy metric — no dedicated activation event exists yet (see T-B12-05)",
    }


def _kpi_source_failures(db: Session, frm: datetime, to: datetime) -> dict:
    # BOOTSTRAP COUNT — not canonical. T-B8-03 owns the action-queue's 4-source
    # union (src/services/action_queue.py, once it exists) and is the source of
    # truth for "source failures" as queue rows. When that lands, replace this
    # body with a call into its count helper instead of this inline query, so
    # the KPI tile and the queue never drift apart.
    n = db.execute(text(
        "SELECT COUNT(*) FROM scraper_alert_log WHERE alerted_at >= :frm AND alerted_at < :to"
    ), {"frm": frm, "to": to}).scalar()
    return {"available": True, "value": int(n or 0)}


def _kpi_cora_approvals_waiting(db: Session) -> dict:
    # BOOTSTRAP COUNT — not canonical, same caveat as _kpi_source_failures
    # above. T-B8-03's action-queue service is the intended source of truth
    # for "approvals waiting"; swap this for its count helper once it ships.
    incidents = db.execute(text(
        "SELECT COUNT(*) FROM cora_incident WHERE breach_resolved IS NULL"
    )).scalar()
    escalations = db.execute(text(
        "SELECT COUNT(*) FROM human_close_escalations WHERE outcome IS NULL"
    )).scalar()
    return {
        "available": True,
        "value": int(incidents or 0) + int(escalations or 0),
        "note": (
            "cora_incident (unresolved) + human_close_escalations (no outcome) only; "
            "win-story Slack approvals have no DB-tracked pending state yet (see T-B8-03)"
        ),
    }


def compute_operator_dashboard(db: Session, frm: datetime, to: datetime) -> dict:
    """Single aggregation point for the Block 8 at-a-glance KPI grid (T-B8-02).

    Every available KPI reads an existing service/table — no new ledger.
    """
    rm = compute_revenue_metrics(db, frm, to)

    return {
        "from": frm.isoformat(),
        "to": to.isoformat(),
        "kpis": {
            "mrr": _kpi_mrr(rm),
            "new_accounts": _kpi_new_accounts(db, frm, to),
            "churn_risk": _kpi_churn_risk(db),
            "leads_delivered": _kpi_leads_delivered(rm),
            "activation": _kpi_activation(rm),
            "deals_submitted": _unavailable("Block 5 (investor_deals) not yet built"),
            "lender_matches": _unavailable("Block 5 (lender-matrix rule engine) not yet built"),
            "loans_funded": _unavailable("Block 7 (lender integration tiers) not yet built"),
            "commissions_owed": _unavailable("Block 7 (referral commission ledger) not yet built"),
            "source_failures": _kpi_source_failures(db, frm, to),
            "cora_approvals_waiting": _kpi_cora_approvals_waiting(db),
        },
    }
