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


# Channel dimension for CAC/payback (Block 4 #23).
#
# Meta is normalized to one combined 'meta' channel regardless of placement —
# mirroring how meta_capi_service.py already treats Facebook and Instagram as
# a single integration (identified there by the shared fbclid click-id, not by
# platform name). fbclid itself is never persisted onto the subscriber row
# (only passed transiently to the CAPI call), so utm_source is the only
# durable signal here; any of facebook/instagram/fb/ig/meta collapse to 'meta'
# so ad spend entered once against "meta" always matches every placement.
#
# Quora is special-cased separately: its producer
# (src/agents/graphs/quora_channel.py, see docs/adr/0021) stamps only
# utm_campaign ("quora_<slug>"), never utm_source — so without this case
# Quora traffic would silently fall through to signup_source='landing_page'
# and merge with organic/other paid traffic instead of its own row.
_META_UTM_SOURCES = ("facebook", "instagram", "fb", "ig", "meta")
_META_SOURCE_NORMALIZE_SQL = " OR ".join(
    f"lower(s.utm_source) = '{v}'" for v in _META_UTM_SOURCES
)
_CHANNEL_KEY_SQL = f"""
    COALESCE(
        CASE WHEN {_META_SOURCE_NORMALIZE_SQL} THEN 'meta' ELSE s.utm_source END,
        CASE WHEN substring(s.utm_campaign from 1 for 6) = 'quora_' THEN 'quora' END,
        s.signup_source,
        'unattributed'
    )
"""


def compute_channel_metrics(db: Session, frm: datetime, to: datetime) -> list[dict]:
    """Per-channel CAC / payback rollup for the window [frm, to) (Block 4 #23).

    Channel key: see `_CHANNEL_KEY_SQL`. Must match the vocabulary enforced by
    the /api/admin/marketing-spend allow-list (MANUAL_SPEND_CHANNELS in
    admin_router.py) or manual spend silently fails to join.

    Spend is hybrid, per §8 of the Block 4 plan:
      - meta (facebook/instagram, normalized) / google / dbpr_email: manually
        entered, `marketing_spend`, period-scoped.
      - quora: auto-read from `quora_topics.cumulative_spend` — a running
        total with no period column (Quora doesn't track spend per period),
        so it is attributed as a snapshot to every window queried rather than
        split by [frm, to). Documented approximation, not a bug.
      - affiliate: auto-read from `affiliate_payout_ledger` accrual lines
        (minus clawbacks), period-scoped via `period_month`.
      - all other channels: no spend source exists yet → CAC is null.

    payback_months = CAC / avg monthly MRR per new customer on that channel
    (simple margin proxy — true payback would additionally subtract per-lead
    COGS from enrichment_usage_logs; documented later refinement, not built
    here).
    """
    p = {"frm": frm, "to": to}
    frm_date, to_date = frm.date(), to.date()

    new_customers = {
        r.channel: r.n
        for r in db.execute(text(f"""
            SELECT {_CHANNEL_KEY_SQL} AS channel, COUNT(DISTINCT mm.account_id) AS n
            FROM mrr_movements mm
            JOIN customer_accounts ca ON ca.account_id = mm.account_id
            LEFT JOIN subscribers s ON s.id = ca.subscriber_id
            WHERE mm.movement_type = 'new' AND mm.effective_at >= :frm AND mm.effective_at < :to
            GROUP BY channel
        """), p).fetchall()
    }
    revenue_cents = {
        r.channel: r.cents
        for r in db.execute(text(f"""
            SELECT {_CHANNEL_KEY_SQL} AS channel, COALESCE(SUM(mm.delta_cents), 0) AS cents
            FROM mrr_movements mm
            JOIN customer_accounts ca ON ca.account_id = mm.account_id
            LEFT JOIN subscribers s ON s.id = ca.subscriber_id
            WHERE mm.movement_type IN ('new', 'expansion')
              AND mm.effective_at >= :frm AND mm.effective_at < :to
            GROUP BY channel
        """), p).fetchall()
    }
    churn = {
        r.channel: {"churn_cents": r.churn_cents, "churned_count": r.churned_count}
        for r in db.execute(text(f"""
            SELECT {_CHANNEL_KEY_SQL} AS channel,
                   COALESCE(SUM(-mm.delta_cents), 0) AS churn_cents,
                   COUNT(*) AS churned_count
            FROM mrr_movements mm
            JOIN customer_accounts ca ON ca.account_id = mm.account_id
            LEFT JOIN subscribers s ON s.id = ca.subscriber_id
            WHERE mm.movement_type = 'churn' AND mm.effective_at >= :frm AND mm.effective_at < :to
            GROUP BY channel
        """), p).fetchall()
    }
    active_mrr = {
        r.channel: {"mrr_cents": r.mrr_cents, "active_count": r.active_count}
        for r in db.execute(text(f"""
            SELECT {_CHANNEL_KEY_SQL} AS channel,
                   COALESCE(SUM(ca.mrr_cents), 0) AS mrr_cents,
                   COUNT(*) AS active_count
            FROM customer_accounts ca
            LEFT JOIN subscribers s ON s.id = ca.subscriber_id
            WHERE ca.status = 'active'
            GROUP BY channel
        """), p).fetchall()
    }
    manual_spend_cents = {
        r.channel: r.cents
        for r in db.execute(text("""
            SELECT channel, COALESCE(SUM(amount_cents), 0) AS cents
            FROM marketing_spend
            WHERE period_end >= :frm_date AND period_start < :to_date
            GROUP BY channel
        """), {"frm_date": frm_date, "to_date": to_date}).fetchall()
    }
    quora_spend_cents = int(round(
        (db.execute(text("SELECT COALESCE(SUM(cumulative_spend), 0) FROM quora_topics")).scalar() or 0) * 100
    ))
    affiliate_spend_cents = int(
        db.execute(text("""
            SELECT COALESCE(SUM(
                CASE WHEN line_type = 'accrual' THEN amount_cents ELSE -amount_cents END
            ), 0)
            FROM affiliate_payout_ledger
            WHERE period_month >= :frm_date AND period_month < :to_date
        """), {"frm_date": frm_date, "to_date": to_date}).scalar() or 0
    )

    channels = set(new_customers) | set(revenue_cents) | set(churn) | set(active_mrr) | set(manual_spend_cents)
    channels.update({"quora", "affiliate"})
    channels.discard(None)

    results = []
    for channel in sorted(channels):
        new_count = int(new_customers.get(channel, 0))
        revenue = int(revenue_cents.get(channel, 0))
        churn_row = churn.get(channel, {"churn_cents": 0, "churned_count": 0})
        mrr_row = active_mrr.get(channel, {"mrr_cents": 0, "active_count": 0})

        if channel == "quora":
            spend = quora_spend_cents
        elif channel == "affiliate":
            spend = affiliate_spend_cents
        else:
            spend = int(manual_spend_cents.get(channel, 0))

        cac_cents = round(spend / new_count) if (spend and new_count) else None
        avg_mrr_cents = round(mrr_row["mrr_cents"] / mrr_row["active_count"]) if mrr_row["active_count"] else None
        payback_months = (
            round(cac_cents / avg_mrr_cents, 2) if (cac_cents and avg_mrr_cents) else None
        )

        results.append({
            "channel": channel,
            "new_customers": new_count,
            "revenue_cents": revenue,
            "churn_cents": int(churn_row["churn_cents"]),
            "churned_count": int(churn_row["churned_count"]),
            "spend_cents": spend,
            "cac_cents": cac_cents,
            "payback_months": payback_months,
        })

    results.sort(key=lambda r: r["revenue_cents"], reverse=True)
    return results
