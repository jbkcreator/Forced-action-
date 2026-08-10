"""Task 6.1 — Revenue OS Analytics Spine.

Refactored to read from the centralized revenue/cost ledger
(src/services/revenue_ledger.py, platform_revenue_ledger +
platform_cost_attribution) instead of hand-joining SentLead/LeadPackPurchase/
PremiumPurchase/subscription_invoices per product. Every purchase-confirmation
path now writes into that ledger; these functions never look at a
product-specific table directly, so a new product integrates by writing to
the ledger, not by touching this file.

Two functions, kept deliberately separate rather than blended into one
"margin" number, because they rest on different kinds of evidence:

  * compute_confirmed_delivery_margin — windowed, backed by a real event row
    with a real timestamp and a real captured amount (lead_unlock, lead_pack,
    premium report/brief).
  * compute_zip_territory_margin — a current-state snapshot, not windowed:
    ZIP-territory ownership has no historical ledger, only current lock
    status, so this can only answer "who owns what right now." Cost comes
    from platform_cost_attribution rows written by the daily
    src/tasks/zip_territory_cost_attribution_refresh.py job — this function
    depends on that job having run for the current date; if it hasn't run
    yet today, territory cost/leads will show as zero (not stale data from
    a prior day, since attribution rows are versioned by computed_for_date
    and never overwritten in place).

Read-only throughout: no new tables, no writes, raw sqlalchemy.text() only.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

_CONFIRMED_DELIVERY_PRODUCT_TYPES = ("lead_unlock", "hot_lead_unlock", "lead_pack", "premium_report", "premium_brief")


def compute_platform_enrichment_spend_ratio(
    db: Session, window_days: int = 30, as_of: Optional[datetime] = None,
) -> dict:
    """Task 6.2 dependency — platform-wide rolling ratio: paid enrichment
    spend / captured subscription revenue, both over the trailing
    window_days. Reads platform_revenue_ledger (this module's own table),
    not subscription_invoices directly, so Task 6.2's budget gate is a real
    dependency on this module rather than a second query against a table it
    doesn't own.

    revenue_cents is scoped to product_type='subscription' specifically —
    lead_unlock/lead_pack/premium rows are correctly excluded, matching the
    task's literal "captured_subscription_revenue" (not total platform
    revenue).

    as_of defaults to real current time; tests pass a fixed far-future value
    so the window never overlaps real production enrichment_usage_logs
    rows — this is a platform-wide aggregate with no grouping key, so
    unlike Task 6.1's per-subscriber queries there's no way to scope a test
    assertion away from real background data other than moving the window.

    Returns {spend_cents, revenue_cents, ratio, window_days}. ratio is None
    when revenue_cents == 0 (undefined — the caller must treat None as the
    zero/unavailable-revenue edge case, not as a 0.0 ratio).
    """
    window_start = (as_of or datetime.now(timezone.utc)) - timedelta(days=window_days)

    row = db.execute(text("""
        WITH spend AS (
            SELECT COALESCE(SUM(cost_cents), 0) AS spend_cents
            FROM enrichment_usage_logs
            WHERE success = TRUE AND created_at >= :window_start
        ),
        revenue AS (
            SELECT COALESCE(SUM(amount_cents), 0) AS revenue_cents
            FROM platform_revenue_ledger
            WHERE product_type = 'subscription'
              AND refunded_at IS NULL
              AND occurred_at >= :window_start
        )
        SELECT spend.spend_cents, revenue.revenue_cents
        FROM spend, revenue
    """), {"window_start": window_start}).fetchone()

    spend_cents = int(row.spend_cents)
    revenue_cents = int(row.revenue_cents)
    ratio = (spend_cents / revenue_cents) if revenue_cents > 0 else None

    return {
        "spend_cents": spend_cents,
        "revenue_cents": revenue_cents,
        "ratio": ratio,
        "window_days": window_days,
    }


def compute_confirmed_delivery_margin(db: Session, frm: datetime, to: datetime) -> list[dict]:
    """One row per subscriber_id with a confirmed-delivery ledger event in
    [frm, to). Sourced entirely from platform_revenue_ledger /
    platform_cost_attribution — no per-product joins.

    Fields: subscriber_id, delivered_leads_count, revenue_cents (COALESCE 0),
    attributed_cost_cents (COALESCE 0), net_margin_cents, margin_pct (None
    when revenue_cents == 0 — undefined, not neutral).
    """
    rows = db.execute(text("""
        WITH revenue AS (
            SELECT id, subscriber_id, property_id, amount_cents
            FROM platform_revenue_ledger
            WHERE product_type = ANY(:product_types)
              AND refunded_at IS NULL
              AND occurred_at >= :frm AND occurred_at < :to
              AND (subscriber_id IS NULL OR subscriber_id NOT IN (SELECT id FROM subscribers WHERE is_test = TRUE))
        ),
        cost_by_subscriber_property AS (
            SELECT subscriber_id, property_id, SUM(attributed_cost_cents) AS cost_cents
            FROM platform_cost_attribution
            WHERE attribution_method = 'direct_purchase'
            GROUP BY subscriber_id, property_id
        ),
        per_subscriber AS (
            SELECT
                r.subscriber_id,
                COUNT(*) AS delivered_leads_count,
                COALESCE(SUM(r.amount_cents), 0) AS revenue_cents,
                COALESCE(SUM(cbp.cost_cents), 0) AS attributed_cost_cents
            FROM revenue r
            LEFT JOIN cost_by_subscriber_property cbp
                ON cbp.subscriber_id = r.subscriber_id AND cbp.property_id = r.property_id
            GROUP BY r.subscriber_id
        )
        SELECT
            subscriber_id, delivered_leads_count, revenue_cents, attributed_cost_cents,
            (revenue_cents - attributed_cost_cents) AS net_margin_cents,
            CASE WHEN revenue_cents = 0 THEN NULL
                 ELSE ROUND((revenue_cents - attributed_cost_cents)::numeric / revenue_cents, 4)
            END AS margin_pct
        FROM per_subscriber
        ORDER BY net_margin_cents DESC, subscriber_id ASC
    """), {
        "product_types": list(_CONFIRMED_DELIVERY_PRODUCT_TYPES),
        "frm": frm, "to": to,
    }).fetchall()

    return [
        {
            "subscriber_id": r.subscriber_id,
            "delivered_leads_count": int(r.delivered_leads_count),
            "revenue_cents": int(r.revenue_cents),
            "attributed_cost_cents": int(r.attributed_cost_cents),
            "net_margin_cents": int(r.net_margin_cents),
            "margin_pct": float(r.margin_pct) if r.margin_pct is not None else None,
        }
        for r in rows
    ]


_ZIP_CANDIDATE_OWNERSHIP_CTE = """
    latest_score AS (
        SELECT DISTINCT ON (property_id) property_id, vertical_scores
        FROM distress_scores
        ORDER BY property_id, score_date DESC
    ),
    already_direct AS (
        SELECT DISTINCT property_id
        FROM platform_cost_attribution
        WHERE attribution_method = 'direct_purchase'
    ),
    candidate_ownership AS (
        SELECT
            p.id AS property_id,
            zt.subscriber_id,
            zt.vertical,
            (ls.vertical_scores ->> zt.vertical)::float AS vertical_score,
            ROW_NUMBER() OVER (
                PARTITION BY p.id
                ORDER BY (ls.vertical_scores ->> zt.vertical)::float DESC NULLS LAST,
                         zt.subscriber_id ASC
            ) AS ownership_rank
        FROM zip_territories zt
        JOIN properties p
            ON p.zip = zt.zip_code AND p.county_id = zt.county_id
        JOIN latest_score ls ON ls.property_id = p.id
        WHERE zt.status IN ('locked', 'grace')
          AND zt.subscriber_id IS NOT NULL
          AND ls.vertical_scores ? zt.vertical
    )
"""

# Winners only (candidate_ownership rank=1, not already claimed by a direct
# purchase) — used by compute_zip_territory_margin, which only needs
# ownership counts, not the losing candidates' detail.
_ZIP_WINNING_OWNERSHIP_CTE = _ZIP_CANDIDATE_OWNERSHIP_CTE + """,
    winning_ownership AS (
        SELECT co.property_id, co.subscriber_id
        FROM candidate_ownership co
        LEFT JOIN already_direct ad ON ad.property_id = co.property_id
        WHERE co.ownership_rank = 1 AND ad.property_id IS NULL
    )
"""


def compute_zip_territory_margin(db: Session) -> list[dict]:
    """Current-state snapshot: one row per subscriber currently holding a
    locked ZIP territory. NOT windowed.

    territory_leads_count is computed live from current ZIP-territory
    ownership (the same collision-resolution query the daily job runs), NOT
    from counting platform_cost_attribution rows — that table only has a
    row when a property has at least one successful enrichment log
    (attributed_cost_cents can't be represented for a property with none,
    since the FK to enrichment_usage_logs is required), so a raw row count
    would silently drop owned-but-never-traced properties and could
    double-count a property with more than one successful log. Cost is read
    from platform_cost_attribution (written by the daily refresh job) via a
    LEFT JOIN so it correctly shows $0 for anything not yet attributed
    rather than dropping the property.

    Fields: subscriber_id, territory_leads_count, revenue_cents (all-time,
    non-reversed platform_revenue_ledger subscription rows),
    territory_attributed_cost_cents, net_margin_cents, margin_pct (None
    when revenue_cents == 0), basis="current_territory_snapshot".
    """
    rows = db.execute(text(f"""
        WITH {_ZIP_WINNING_OWNERSHIP_CTE},
        territory_holders AS (
            SELECT DISTINCT subscriber_id
            FROM zip_territories
            WHERE status IN ('locked', 'grace') AND subscriber_id IS NOT NULL
              AND subscriber_id NOT IN (SELECT id FROM subscribers WHERE is_test = TRUE)
        ),
        ownership_counts AS (
            SELECT subscriber_id, COUNT(DISTINCT property_id) AS territory_leads_count
            FROM winning_ownership
            GROUP BY subscriber_id
        ),
        today_cost AS (
            SELECT wo.subscriber_id,
                   COALESCE(SUM(pca.attributed_cost_cents), 0) AS territory_attributed_cost_cents
            FROM winning_ownership wo
            LEFT JOIN platform_cost_attribution pca
                ON pca.property_id = wo.property_id AND pca.subscriber_id = wo.subscriber_id
               AND pca.attribution_method = 'zip_territory_highest_vertical'
               AND pca.computed_for_date = CURRENT_DATE
            GROUP BY wo.subscriber_id
        ),
        revenue_by_subscriber AS (
            SELECT subscriber_id, COALESCE(SUM(amount_cents), 0) AS revenue_cents
            FROM platform_revenue_ledger
            WHERE product_type = 'subscription' AND refunded_at IS NULL
            GROUP BY subscriber_id
        )
        SELECT
            th.subscriber_id,
            COALESCE(oc.territory_leads_count, 0) AS territory_leads_count,
            COALESCE(rb.revenue_cents, 0) AS revenue_cents,
            COALESCE(tc.territory_attributed_cost_cents, 0) AS territory_attributed_cost_cents,
            COALESCE(rb.revenue_cents, 0) - COALESCE(tc.territory_attributed_cost_cents, 0) AS net_margin_cents,
            CASE WHEN COALESCE(rb.revenue_cents, 0) = 0 THEN NULL
                 ELSE ROUND((COALESCE(rb.revenue_cents,0) - COALESCE(tc.territory_attributed_cost_cents,0))::numeric
                             / rb.revenue_cents, 4)
            END AS margin_pct
        FROM territory_holders th
        LEFT JOIN ownership_counts oc ON oc.subscriber_id = th.subscriber_id
        LEFT JOIN today_cost tc ON tc.subscriber_id = th.subscriber_id
        LEFT JOIN revenue_by_subscriber rb ON rb.subscriber_id = th.subscriber_id
        ORDER BY net_margin_cents DESC, th.subscriber_id ASC
    """)).fetchall()

    return [
        {
            "subscriber_id": r.subscriber_id,
            "territory_leads_count": int(r.territory_leads_count),
            "revenue_cents": int(r.revenue_cents),
            "territory_attributed_cost_cents": int(r.territory_attributed_cost_cents),
            "net_margin_cents": int(r.net_margin_cents),
            "margin_pct": float(r.margin_pct) if r.margin_pct is not None else None,
            "basis": "current_territory_snapshot",
        }
        for r in rows
    ]


def list_zip_territory_leads(db: Session, subscriber_id: int) -> list[dict]:
    """Per-property drill-down of the current territory snapshot for one
    subscriber — including properties they qualify for but LOST to a
    higher-scoring vertical, so a $0 row is always auditable, never a silent
    zero. Ordered by vertical_score DESC.

    The win/lose determination and vertical scores are computed live (a
    single-subscriber lookup is cheap and this is a drill-down/debugging
    tool, not a high-volume report) — but the winning row's
    attributed_cost_cents is read from today's persisted
    platform_cost_attribution snapshot (matching what
    compute_zip_territory_margin reports), not recomputed independently, so
    the two functions can never disagree. A win shows 0 here if the daily
    refresh job hasn't run yet today — that's real information (attribution
    pending), not a bug.

    Fields: property_id, vertical, vertical_score, is_owner (bool),
    attributed_cost_cents (0 when is_owner is False, or when the daily job
    hasn't attributed this property yet today), cost_attribution_note (None
    when is_owner is True; otherwise names the winning vertical/subscriber
    and their score).
    """
    rows = db.execute(text(f"""
        WITH {_ZIP_CANDIDATE_OWNERSHIP_CTE},
        winner AS (
            SELECT property_id, subscriber_id AS owner_subscriber_id, vertical AS owner_vertical,
                   vertical_score AS owner_vertical_score
            FROM candidate_ownership WHERE ownership_rank = 1
        ),
        todays_attribution AS (
            SELECT property_id, subscriber_id, SUM(attributed_cost_cents) AS cost_cents
            FROM platform_cost_attribution
            WHERE attribution_method = 'zip_territory_highest_vertical'
              AND computed_for_date = CURRENT_DATE
            GROUP BY property_id, subscriber_id
        )
        SELECT
            co.property_id, co.vertical, co.vertical_score,
            (co.ownership_rank = 1 AND ad.property_id IS NULL) AS is_owner,
            COALESCE(ta.cost_cents, 0) AS attributed_cost_cents,
            CASE
                WHEN ad.property_id IS NOT NULL THEN
                    'Property already has a confirmed delivery event (lead_unlock/lead_pack/premium) — see compute_confirmed_delivery_margin'
                WHEN co.ownership_rank != 1 THEN
                    format('Cost attributed to higher-scoring vertical %s (subscriber %s, score %s vs this vertical''s %s)',
                           w.owner_vertical, w.owner_subscriber_id, w.owner_vertical_score, co.vertical_score)
                ELSE NULL
            END AS cost_attribution_note
        FROM candidate_ownership co
        LEFT JOIN already_direct ad ON ad.property_id = co.property_id
        LEFT JOIN winner w ON w.property_id = co.property_id
        LEFT JOIN todays_attribution ta
            ON ta.property_id = co.property_id AND ta.subscriber_id = co.subscriber_id
        WHERE co.subscriber_id = :subscriber_id
        ORDER BY co.vertical_score DESC NULLS LAST
    """), {"subscriber_id": subscriber_id}).fetchall()

    return [
        {
            "property_id": r.property_id,
            "vertical": r.vertical,
            "vertical_score": float(r.vertical_score) if r.vertical_score is not None else None,
            "is_owner": bool(r.is_owner),
            "attributed_cost_cents": int(r.attributed_cost_cents),
            "cost_attribution_note": r.cost_attribution_note,
        }
        for r in rows
    ]
