"""M11 / B3 — Revenue Reporting Surface (§4A.5).

Computes the S1 revenue + unit-economics metrics on demand from the DB. Read-only:
no new tables, no writes. Reads what B1 (customer_accounts, mrr_movements) and B2
(deliveries, free_to_paid_attribution) already record, plus the fa005 enrichment
cost log.

Metric kinds (§ grilled with client):
  * snapshot  — current state, ignore the window: mrr_cents, active_accounts,
                past_due_count, at_risk_mrr_cents
  * period    — counted within [frm, to): everything else

MRR recognition (§12.7) is already enforced upstream: B1 normalizes annual→/12 and
excludes one-time/trial from mrr_cents, and mrr_movements carries is_involuntary so
churn splits voluntary vs involuntary here.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def compute_revenue_metrics(db: Session, frm: datetime, to: datetime) -> dict:
    """Return the §4A.5 metric bundle for the window [frm, to).

    Snapshot metrics reflect the current DB state; period metrics are bounded by
    the window. See the §8B API contract for the field set.
    """
    p = {"frm": frm, "to": to}

    # ── snapshot (current state) ─────────────────────────────────────────────
    mrr_cents = db.execute(text(
        "SELECT COALESCE(SUM(mrr_cents),0) FROM customer_accounts WHERE status = 'active'"
    )).scalar()
    active_accounts = db.execute(text(
        "SELECT count(*) FROM customer_accounts WHERE status = 'active'"
    )).scalar()
    past_due_count = db.execute(text(
        "SELECT count(*) FROM customer_accounts WHERE status = 'past_due'"
    )).scalar()
    at_risk_mrr_cents = db.execute(text(
        "SELECT COALESCE(SUM(mrr_cents),0) FROM customer_accounts WHERE status = 'past_due'"
    )).scalar()

    # ── period: MRR movements ────────────────────────────────────────────────
    new_mrr_cents = db.execute(text(
        "SELECT COALESCE(SUM(delta_cents),0) FROM mrr_movements "
        "WHERE movement_type = 'new' AND effective_at >= :frm AND effective_at < :to"
    ), p).scalar()
    churn = db.execute(text("""
        SELECT
          COALESCE(SUM(CASE WHEN is_involuntary     THEN -delta_cents ELSE 0 END), 0) AS involuntary,
          COALESCE(SUM(CASE WHEN NOT is_involuntary THEN -delta_cents ELSE 0 END), 0) AS voluntary,
          count(*) AS churned_count
        FROM mrr_movements
        WHERE movement_type = 'churn' AND effective_at >= :frm AND effective_at < :to
    """), p).fetchone()

    # ── period: deliveries (by grade, by account) ────────────────────────────
    by_grade = {r.grade: r.n for r in db.execute(text(
        "SELECT grade, count(*) AS n FROM deliveries "
        "WHERE status = 'delivered' AND delivered_at >= :frm AND delivered_at < :to "
        "GROUP BY grade"
    ), p).fetchall()}
    by_account = {str(r.account_id): r.n for r in db.execute(text(
        "SELECT account_id, count(*) AS n FROM deliveries "
        "WHERE status = 'delivered' AND delivered_at >= :frm AND delivered_at < :to "
        "GROUP BY account_id"
    ), p).fetchall()}
    leads_delivered = sum(by_grade.values())

    # ── entitlement utilization = delivered (period) / capacity owed (active) ─
    capacity = db.execute(text("""
        SELECT COALESCE(SUM((value)::int), 0)
        FROM customer_accounts ca, jsonb_each_text(ca.lead_entitlement)
        WHERE ca.status = 'active'
    """)).scalar() or 0
    entitlement_utilization = round(leads_delivered / capacity, 4) if capacity else 0.0

    # ── free→paid (§12.8) ────────────────────────────────────────────────────
    conversions = db.execute(text(
        "SELECT count(*) FROM free_to_paid_attribution "
        "WHERE free_leads_count > 0 AND converted_at >= :frm AND converted_at < :to"
    ), p).scalar()
    free_lead_accounts = db.execute(text(
        "SELECT count(DISTINCT account_id) FROM deliveries WHERE billing_period_end IS NULL"
    )).scalar() or 0
    free_to_paid_rate = round(conversions / free_lead_accounts, 4) if free_lead_accounts else 0.0
    avg_days = db.execute(text("""
        SELECT AVG(EXTRACT(EPOCH FROM (a.converted_at - d.delivered_at)) / 86400.0)
        FROM free_to_paid_attribution a
        JOIN deliveries d ON d.id = a.first_free_delivery_id
        WHERE a.free_leads_count > 0 AND a.converted_at >= :frm AND a.converted_at < :to
    """), p).scalar()
    avg_time_to_convert_days = round(float(avg_days), 2) if avg_days is not None else None

    # ── unit economics: cost-per-record vs revenue-per-lead ──────────────────
    cost_row = db.execute(text("""
        SELECT COALESCE(SUM(cost_cents),0) AS spend, COUNT(DISTINCT property_id) AS records
        FROM enrichment_usage_logs
        WHERE success = TRUE AND property_id IS NOT NULL
          AND created_at >= :frm AND created_at < :to
    """), p).fetchone()
    cost_per_record_cents = round(cost_row.spend / cost_row.records) if cost_row.records else 0
    revenue_per_lead_cents = round(mrr_cents / leads_delivered) if leads_delivered else 0

    return {
        "from": frm.isoformat(),
        "to": to.isoformat(),
        "mrr_cents": int(mrr_cents),
        "new_mrr_cents": int(new_mrr_cents),
        "active_accounts": int(active_accounts),
        "leads_delivered_by_grade": by_grade,
        "leads_delivered_by_account": by_account,
        "entitlement_utilization": entitlement_utilization,
        "free_to_paid_rate": free_to_paid_rate,
        "avg_time_to_convert_days": avg_time_to_convert_days,
        "past_due_count": int(past_due_count),
        "at_risk_mrr_cents": int(at_risk_mrr_cents),
        "voluntary_churn_cents": int(churn.voluntary),
        "involuntary_churn_cents": int(churn.involuntary),
        "churned_count": int(churn.churned_count),
        "cost_per_record_cents": int(cost_per_record_cents),
        "revenue_per_lead_cents": int(revenue_per_lead_cents),
    }
