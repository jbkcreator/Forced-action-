"""
Grid-cell P&L framework (CLONE-v2.2 CL2).

Generalizes PlatformRevenueLedger/PlatformCostAttribution's additive-rollup
pattern (src/services/revenue_ledger.py) down from product/subscriber level
to the cell level. A "cell" is county_id x distress_type x buyer_vertical x
offer_step (see GridCellPnl in src/core/models.py for the exact dimension
definitions, sourced from config/scoring.py:VERTICAL_WEIGHTS and
config/revenue_ladder.py:REVENUE_LADDER).

Revenue/cost source rows (platform_revenue_ledger, platform_cost_attribution)
don't carry distress_type/buyer_vertical/offer_step directly — those are
per-deal tags that live on golden_close_chains (CL2's other new table).
rollup_cell_from_ledger therefore uses golden_close_chains as the bridge:
it sums ledger/cost rows for the properties tagged with a given cell in that
period. This means a cell's rollup only reflects revenue/cost for deals that
have a golden_close_chains row — expected at this stage since chain-tagging
is itself new; as tagging coverage grows, rollup coverage grows with it.

All DB I/O is raw SQL via sa_text — repo convention.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional, Sequence

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def compute_contribution_margin_cents(revenue_cents: int, cost_cents: int) -> int:
    """Pure additive-rollup formula: margin = revenue - cost.

    Kept as its own function (rather than inlined at every call site) so
    the formula has exactly one definition if it ever grows beyond simple
    subtraction (e.g. a platform-fee deduction).
    """
    return revenue_cents - cost_cents


def estimate_payback_days(daily_margins_cents: Sequence[int], cac_cents: int) -> Optional[int]:
    """Days of cumulative contribution margin needed to recoup a
    per-acquisition cost (CAC). Returns None if the margins never recoup it
    within the given series (i.e. payback hasn't happened yet).

    `daily_margins_cents` must be in chronological order — day 0 first.
    """
    if cac_cents <= 0:
        return 0
    cumulative = 0
    for day_index, margin_cents in enumerate(daily_margins_cents):
        cumulative += margin_cents
        if cumulative >= cac_cents:
            return day_index + 1
    return None


def upsert_cell_pnl(
    session: Session,
    *,
    county_id: str,
    distress_type: str,
    buyer_vertical: str,
    offer_step: str,
    period_start: date,
    period_end: date,
    revenue_cents: int,
    cost_cents: int,
    deal_count: int,
) -> int:
    """INSERT or overwrite one grid_cell_pnl row for (cell, period).

    Unlike PlatformRevenueLedger (append-only, ON CONFLICT DO NOTHING), a
    cell-period P&L row is a point-in-time recomputation — re-running the
    rollup for an already-computed period is expected (e.g. late-arriving
    refunds) and should overwrite, not silently skip. Returns the row id.
    """
    margin_cents = compute_contribution_margin_cents(revenue_cents, cost_cents)
    row = session.execute(sa_text("""
        INSERT INTO grid_cell_pnl (
            county_id, distress_type, buyer_vertical, offer_step,
            period_start, period_end,
            revenue_cents, cost_cents, contribution_margin_cents, deal_count,
            computed_at
        ) VALUES (
            :county_id, :distress_type, :buyer_vertical, :offer_step,
            :period_start, :period_end,
            :revenue_cents, :cost_cents, :margin_cents, :deal_count,
            NOW()
        )
        ON CONFLICT (county_id, distress_type, buyer_vertical, offer_step, period_start, period_end)
        DO UPDATE SET
            revenue_cents = EXCLUDED.revenue_cents,
            cost_cents = EXCLUDED.cost_cents,
            contribution_margin_cents = EXCLUDED.contribution_margin_cents,
            deal_count = EXCLUDED.deal_count,
            computed_at = EXCLUDED.computed_at
        RETURNING id
    """), {
        "county_id": county_id,
        "distress_type": distress_type,
        "buyer_vertical": buyer_vertical,
        "offer_step": offer_step,
        "period_start": period_start,
        "period_end": period_end,
        "revenue_cents": revenue_cents,
        "cost_cents": cost_cents,
        "margin_cents": margin_cents,
        "deal_count": deal_count,
    }).first()

    cell_id = int(row.id)
    logger.info(
        "[grid_cell_pnl] cell=%s/%s/%s/%s period=%s..%s margin_cents=%d",
        county_id, distress_type, buyer_vertical, offer_step,
        period_start, period_end, margin_cents,
    )
    return cell_id


def rollup_cell_from_ledger(
    session: Session,
    *,
    county_id: str,
    distress_type: str,
    buyer_vertical: str,
    offer_step: str,
    period_start: date,
    period_end: date,
) -> int:
    """Sum platform_revenue_ledger/platform_cost_attribution for every
    property tagged with this exact cell in golden_close_chains, for the
    given period, then upsert the result via upsert_cell_pnl(). Returns the
    grid_cell_pnl row id.
    """
    totals = session.execute(sa_text("""
        WITH cell_properties AS (
            SELECT DISTINCT property_id
            FROM golden_close_chains
            WHERE county_id = :county_id
              AND distress_type = :distress_type
              AND buyer_vertical = :buyer_vertical
              AND offer_step = :offer_step
              AND property_id IS NOT NULL
        ),
        revenue AS (
            SELECT
                COALESCE(SUM(r.amount_cents - COALESCE(r.refunded_amount_cents, 0)), 0) AS revenue_cents,
                COUNT(*) AS deal_count
            FROM platform_revenue_ledger r
            JOIN cell_properties cp ON cp.property_id = r.property_id
            WHERE r.occurred_at::date BETWEEN :period_start AND :period_end
        ),
        cost AS (
            SELECT COALESCE(SUM(c.attributed_cost_cents), 0) AS cost_cents
            FROM platform_cost_attribution c
            JOIN cell_properties cp ON cp.property_id = c.property_id
            WHERE c.created_at::date BETWEEN :period_start AND :period_end
        )
        SELECT revenue.revenue_cents, revenue.deal_count, cost.cost_cents
        FROM revenue, cost
    """), {
        "county_id": county_id,
        "distress_type": distress_type,
        "buyer_vertical": buyer_vertical,
        "offer_step": offer_step,
        "period_start": period_start,
        "period_end": period_end,
    }).one()

    return upsert_cell_pnl(
        session,
        county_id=county_id,
        distress_type=distress_type,
        buyer_vertical=buyer_vertical,
        offer_step=offer_step,
        period_start=period_start,
        period_end=period_end,
        revenue_cents=int(totals.revenue_cents),
        cost_cents=int(totals.cost_cents),
        deal_count=int(totals.deal_count),
    )
