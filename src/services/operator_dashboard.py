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

from src.services.action_queue import (
    cora_approvals_waiting as _aq_cora_approvals_waiting,
    source_failures as _aq_source_failures,
)
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
    # Canonical count owned by T-B8-03's action queue. This KPI tile deep-links
    # into /admin/action-queue?lane=failures&category=source, so it must match
    # the queue exactly — i.e. scraper alerts open within the rolling cooldown
    # window, NOT the dashboard's from/to window. frm/to are intentionally
    # ignored here for that reason.
    return {"available": True, "value": _aq_source_failures(db)}


def _kpi_cora_approvals_waiting(db: Session) -> dict:
    # Canonical count owned by T-B8-03's action queue. Legal-lane cora incidents
    # only (human_escalated / feature_killed) — excludes auto-handled incidents
    # and human-close escalations, matching the approvals lane the KPI links to.
    return {
        "available": True,
        "value": _aq_cora_approvals_waiting(db),
        "note": (
            "legal-lane cora only (human_escalated/feature_killed); "
            "canonical source: action_queue.cora_approvals_waiting"
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
