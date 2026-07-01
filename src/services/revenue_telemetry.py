"""Task 6.1 — Revenue OS Analytics Spine.

Redesigned after discovering the S1 `deliveries`/`customer_accounts` pipeline
this module originally read is dormant in production (no scheduled job ever
writes to `deliveries`; only 2 real `customer_accounts` rows exist). The real,
live subscriber revenue/delivery mechanisms are:

  * Recurring ZIP-territory subscription (`subscribers` + `zip_territories`):
    locking a ZIP+vertical+county reveals full owner contact for every
    qualifying property in that territory *live on the dashboard*
    (`GET /api/feed/{feed_uuid}`), independent of any email job — confirmed
    in src/api/main.py: "Subscribers with locked ZIP territories see all
    in-territory contacts revealed — independent of whether the daily-email
    job has stamped a SentLead row yet." This is a real, continuously-running
    access mechanism, not an inference.
  * One-time purchases with a real per-delivery DB row: `lead_unlock`
    ($2.50-$7) and `lead_pack` ($99/5 leads) write `SentLead`; `premium`
    report/brief write `PremiumPurchase`. `hot_lead_unlock` is currently
    disabled (settings.hot_lead_unlock_enabled=False) because it has no
    delivery-recording path at all. `premium` transfer/byol are excluded —
    their fulfillment cost isn't cleanly attributable to the purchase row.

Two functions, kept deliberately separate rather than blended into one
"margin" number, because they rest on different kinds of evidence:

  * compute_confirmed_delivery_margin — windowed, backed by a real event row
    with a real timestamp and (for lead_unlock/premium) a real captured
    amount. `lead_pack` revenue comes from the parent LeadPackPurchase row
    (one $99 payment covers 5 leads — summing a per-lead amount would
    5x-overcount).
  * compute_zip_territory_margin — a current-state snapshot, not windowed:
    `zip_territories` has no historical ownership ledger, only current
    lock status, so this can only answer "who owns what right now," and
    sums all-time captured revenue/cost for that ownership. Solves the
    multi-vertical collision (a property can qualify for more than one
    vertical, so two subscribers holding different verticals' locks on the
    same ZIP can both dashboard-see it) by attributing the one-time
    enrichment cost to whichever vertical scores that property highest;
    the loser shows $0 plus an explicit reason, never a silent zero. Also
    excludes any property already owned by the confirmed-delivery leg, so
    the two functions never double-count the same enrichment cost.

Read-only throughout: no new tables, no writes, raw sqlalchemy.text() only.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


def compute_confirmed_delivery_margin(db: Session, frm: datetime, to: datetime) -> list[dict]:
    """One row per subscriber_id with a confirmed-delivery event in [frm, to).

    Sources (each a real, timestamped, per-property delivery record):
      * SentLead(source='lead_unlock_payment') — amount_cents on the row itself
      * SentLead(source='lead_pack') — amount split from the parent
        LeadPackPurchase.amount_cents (one payment, up to 5 leads/rows)
      * PremiumPurchase(sku in ('report','brief')) — amount_cents or, for
        credits-paid rows, amount_cents is NULL (no cash changed hands)

    Cost is enrichment_usage_logs.cost_cents summed per property_id
    (unwindowed — a trace is a durable fact, not a period fact), joined to
    whichever of the above delivered that property to that subscriber.

    Fields: subscriber_id, delivered_leads_count, revenue_cents (COALESCE 0),
    attributed_cost_cents (COALESCE 0), net_margin_cents, margin_pct (None
    when revenue_cents == 0 — undefined, not neutral).
    """
    rows = db.execute(text("""
        WITH property_cost AS (
            SELECT property_id, SUM(cost_cents) AS total_cost_cents
            FROM enrichment_usage_logs
            WHERE success = TRUE AND property_id IS NOT NULL
            GROUP BY property_id
        ),
        lead_pack_per_lead AS (
            -- split one purchase's captured amount evenly across its delivered leads
            SELECT lpp.subscriber_id, sl.property_id, sl.sent_at,
                   lpp.amount_cents::numeric / NULLIF(array_length(lpp.lead_ids, 1), 0) AS revenue_cents
            FROM sent_leads sl
            JOIN lead_pack_purchases lpp
                ON lpp.stripe_payment_intent_id = sl.stripe_payment_intent_id
            WHERE sl.source = 'lead_pack' AND lpp.status = 'delivered'
        ),
        events AS (
            SELECT subscriber_id, property_id, sent_at AS event_at,
                   COALESCE(amount_cents, 0) AS revenue_cents
            FROM sent_leads
            WHERE source = 'lead_unlock_payment' AND sent_at >= :frm AND sent_at < :to

            UNION ALL

            SELECT subscriber_id, property_id, sent_at AS event_at,
                   COALESCE(revenue_cents, 0) AS revenue_cents
            FROM lead_pack_per_lead
            WHERE sent_at >= :frm AND sent_at < :to

            UNION ALL

            SELECT subscriber_id, property_id, delivered_at AS event_at,
                   COALESCE(amount_cents, 0) AS revenue_cents
            FROM premium_purchases
            WHERE sku IN ('report', 'brief')
              AND status = 'delivered'
              AND refunded_at IS NULL AND disputed_at IS NULL
              AND delivered_at >= :frm AND delivered_at < :to
        ),
        per_subscriber AS (
            SELECT
                e.subscriber_id,
                COUNT(*) AS delivered_leads_count,
                COALESCE(SUM(e.revenue_cents), 0) AS revenue_cents,
                COALESCE(SUM(pc.total_cost_cents), 0) AS attributed_cost_cents
            FROM events e
            LEFT JOIN property_cost pc ON pc.property_id = e.property_id
            GROUP BY e.subscriber_id
        )
        SELECT
            subscriber_id, delivered_leads_count, revenue_cents, attributed_cost_cents,
            (revenue_cents - attributed_cost_cents) AS net_margin_cents,
            CASE WHEN revenue_cents = 0 THEN NULL
                 ELSE ROUND((revenue_cents - attributed_cost_cents)::numeric / revenue_cents, 4)
            END AS margin_pct
        FROM per_subscriber
        ORDER BY net_margin_cents DESC, subscriber_id ASC
    """), {"frm": frm, "to": to}).fetchall()

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


_ZIP_TERRITORY_OWNERSHIP_CTE = """
    property_cost AS (
        SELECT property_id, SUM(cost_cents) AS total_cost_cents
        FROM enrichment_usage_logs
        WHERE success = TRUE AND property_id IS NOT NULL
        GROUP BY property_id
    ),
    already_confirmed AS (
        -- properties already owned by a real delivery event (SentLead or
        -- PremiumPurchase) belong to compute_confirmed_delivery_margin, not
        -- here — prevents double-counting the same enrichment cost twice.
        SELECT DISTINCT property_id FROM sent_leads
        UNION
        SELECT DISTINCT property_id FROM premium_purchases WHERE property_id IS NOT NULL
    ),
    latest_score AS (
        SELECT DISTINCT ON (property_id) property_id, vertical_scores
        FROM distress_scores
        ORDER BY property_id, score_date DESC
    ),
    candidate_ownership AS (
        -- every (property, subscriber, vertical) where a locked ZIP territory
        -- covers this property AND the property actually scores for that
        -- vertical. A property can appear more than once here if more than
        -- one vertical's territory-holder qualifies for it (the collision).
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
    ),
    winning_ownership AS (
        SELECT co.property_id, co.subscriber_id, co.vertical, co.vertical_score
        FROM candidate_ownership co
        LEFT JOIN already_confirmed ac ON ac.property_id = co.property_id
        WHERE co.ownership_rank = 1 AND ac.property_id IS NULL
    )
"""


def compute_zip_territory_margin(db: Session) -> list[dict]:
    """Current-state snapshot: one row per subscriber currently holding a
    locked ZIP territory. NOT windowed — zip_territories has no historical
    ownership ledger, only current lock status, so this answers "who owns
    what right now," summing all-time captured revenue/cost for that
    ownership.

    Collision rule: if a property qualifies for more than one vertical and
    each vertical's ZIP is locked by a different subscriber, the full
    enrichment cost is attributed to whichever vertical scores that property
    highest — see list_zip_territory_leads() for the per-property detail and
    the explicit reason on every losing row.

    Fields: subscriber_id, territory_leads_count, revenue_cents (all-time,
    non-reversed subscription_invoices), territory_attributed_cost_cents,
    net_margin_cents, margin_pct (None when revenue_cents == 0),
    basis="current_territory_snapshot" (never comparable 1:1 with the
    windowed, event-backed compute_confirmed_delivery_margin numbers).
    """
    rows = db.execute(text(f"""
        WITH {_ZIP_TERRITORY_OWNERSHIP_CTE},
        territory_holders AS (
            -- anchor on every subscriber who currently holds a lock, not just
            -- those with a winning property — a subscriber paying for a zip
            -- with zero currently-qualifying leads must still be reported
            -- (real revenue, $0 territory cost), not silently dropped.
            SELECT DISTINCT subscriber_id
            FROM zip_territories
            WHERE status IN ('locked', 'grace') AND subscriber_id IS NOT NULL
        ),
        cost_by_subscriber AS (
            SELECT wo.subscriber_id,
                   COUNT(*) AS territory_leads_count,
                   COALESCE(SUM(pc.total_cost_cents), 0) AS territory_attributed_cost_cents
            FROM winning_ownership wo
            LEFT JOIN property_cost pc ON pc.property_id = wo.property_id
            GROUP BY wo.subscriber_id
        ),
        revenue_by_subscriber AS (
            SELECT subscriber_id, COALESCE(SUM(amount_collected_cents), 0) AS revenue_cents
            FROM subscription_invoices
            WHERE reversed_at IS NULL
            GROUP BY subscriber_id
        )
        SELECT
            th.subscriber_id,
            COALESCE(cb.territory_leads_count, 0) AS territory_leads_count,
            COALESCE(rb.revenue_cents, 0) AS revenue_cents,
            COALESCE(cb.territory_attributed_cost_cents, 0) AS territory_attributed_cost_cents,
            COALESCE(rb.revenue_cents, 0) - COALESCE(cb.territory_attributed_cost_cents, 0) AS net_margin_cents,
            CASE WHEN COALESCE(rb.revenue_cents, 0) = 0 THEN NULL
                 ELSE ROUND((COALESCE(rb.revenue_cents,0) - COALESCE(cb.territory_attributed_cost_cents,0))::numeric
                             / rb.revenue_cents, 4)
            END AS margin_pct
        FROM territory_holders th
        LEFT JOIN cost_by_subscriber cb ON cb.subscriber_id = th.subscriber_id
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

    Fields: property_id, vertical, vertical_score, is_owner (bool),
    attributed_cost_cents (0 when is_owner is False),
    cost_attribution_note (None when is_owner is True; otherwise names the
    winning vertical/subscriber and their score).
    """
    rows = db.execute(text(f"""
        WITH {_ZIP_TERRITORY_OWNERSHIP_CTE}
        SELECT
            co.property_id, co.vertical, co.vertical_score,
            (co.ownership_rank = 1 AND ac.property_id IS NULL) AS is_owner,
            CASE WHEN co.ownership_rank = 1 AND ac.property_id IS NULL
                     THEN COALESCE(pc.total_cost_cents, 0)
                 ELSE 0
            END AS attributed_cost_cents,
            CASE
                WHEN ac.property_id IS NOT NULL THEN
                    'Property already has a confirmed delivery event (lead_unlock/lead_pack/premium) — see compute_confirmed_delivery_margin'
                WHEN co.ownership_rank != 1 THEN
                    format('Cost attributed to higher-scoring vertical %s (subscriber %s, score %s vs this vertical''s %s)',
                           winner.vertical, winner.subscriber_id, winner.vertical_score, co.vertical_score)
                ELSE NULL
            END AS cost_attribution_note
        FROM candidate_ownership co
        LEFT JOIN already_confirmed ac ON ac.property_id = co.property_id
        LEFT JOIN property_cost pc ON pc.property_id = co.property_id
        LEFT JOIN candidate_ownership winner
            ON winner.property_id = co.property_id AND winner.ownership_rank = 1
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
