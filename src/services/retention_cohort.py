"""Retention Cohort viewport (T-B8-04).

Read-time aggregation only — no new table, no recompute cron. See
docs/adr/0038-retention-cohort-paid-logo-aged-off-mrr-ledger.md and
CONTEXT.md's "Retention Cohort" entry for the full definition.

Paid **logo** retention: subscribers are bucketed by the calendar month of
`subscribers.created_at`, then aged off the `mrr_movements` ledger rather than
`subscribers.status`/`churned_at` — a row can be `status='churned'` with
`churned_at IS NULL` (see tests/test_reactivation_foundation.py) and free/trial
exits never set `churned_at` at all, so neither can reliably age a survival
curve. A subscriber enters the curve at their first `new` movement
(`entered_at`) and is "alive" at month N if their net ledger state (new −
churn, up to `entered_at` + N months) is positive, so append-only
reactivations correctly re-count as alive instead of freezing dead at first
churn. Aging is anchored to `entered_at`, not the `cohort_month` row label —
a subscriber can sign up (created_at) weeks before their first paid movement
(free trial), and anchoring the clock to the calendar signup month instead of
the actual paid-entry timestamp produces impossible curves (0% at M0 rising
to 100% at M1). The cohort *row* is still labeled by signup month for
readability; only the per-month cutoff math uses `entered_at`.

Channel key reuses `_CHANNEL_KEY_SQL` from revenue_metrics.py verbatim so this
viewport agrees with the CAC/channel dashboard. ZIP membership is
many-to-many via zip_territories (locked/grace) — a multi-ZIP subscriber
counts toward each of its ZIPs; per-ZIP denominators are not mutually
exclusive and must not be summed across ZIPs.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.revenue_metrics import _CHANNEL_KEY_SQL

MIN_COHORT_SIZE = 5


def compute_retention_cohorts(
    db: Session,
    *,
    channel: str | None = None,
    tier: str | None = None,
    zip_code: str | None = None,
    months: int = 12,
    cohorts: int = 12,
) -> dict:
    """Paid logo retention grid: rows = signup-month cohorts, columns =
    months-since-signup. Filters (channel/tier/zip_code) are independent AND
    facets, each defaulting to all.
    """
    filters_sql = []
    params: dict = {"months": months, "cohorts": cohorts}

    if channel is not None:
        filters_sql.append(f"{_CHANNEL_KEY_SQL} = :channel")
        params["channel"] = channel
    if tier is not None:
        filters_sql.append("s.tier = :tier")
        params["tier"] = tier
    if zip_code is not None:
        filters_sql.append("""
            EXISTS (
                SELECT 1 FROM zip_territories zt
                WHERE zt.subscriber_id = s.id
                  AND zt.status IN ('locked', 'grace')
                  AND zt.zip_code = :zip_code
            )
        """)
        params["zip_code"] = zip_code

    where_clause = ""
    if filters_sql:
        where_clause = "AND " + " AND ".join(filters_sql)

    rows = db.execute(text(f"""
        WITH members AS (
            -- ever-paid subscribers: first 'new' movement, joined back to
            -- their signup-month cohort and filter facets
            SELECT
                s.id AS subscriber_id,
                date_trunc('month', s.created_at) AS cohort_month,
                MIN(mm.effective_at) FILTER (WHERE mm.movement_type = 'new') AS entered_at
            FROM subscribers s
            JOIN customer_accounts ca ON ca.subscriber_id = s.id
            JOIN mrr_movements mm ON mm.account_id = ca.account_id
            WHERE s.created_at >= date_trunc('month', now()) - ((:cohorts - 1) || ' months')::interval
              {where_clause}
            GROUP BY s.id, s.created_at
            HAVING MIN(mm.effective_at) FILTER (WHERE mm.movement_type = 'new') IS NOT NULL
        ),
        offsets AS (
            SELECT generate_series(0, :months - 1) AS m
        ),
        ledger AS (
            -- net ledger state per member per month-offset: sum of signed
            -- deltas (new/expansion positive, churn negative) up to that age
            SELECT
                mem.subscriber_id,
                mem.cohort_month,
                off.m,
                COALESCE(SUM(
                    CASE WHEN mm.movement_type = 'churn' THEN -1 ELSE 1 END
                ) FILTER (
                    WHERE mm.effective_at <= mem.entered_at + (off.m || ' months')::interval
                      AND mm.movement_type IN ('new', 'churn')
                ), 0) AS net_state,
                (mem.entered_at + (off.m || ' months')::interval) <= now() AS aged_into
            FROM members mem
            JOIN customer_accounts ca ON ca.subscriber_id = mem.subscriber_id
            JOIN mrr_movements mm ON mm.account_id = ca.account_id
            CROSS JOIN offsets off
            GROUP BY mem.subscriber_id, mem.cohort_month, mem.entered_at, off.m
        )
        SELECT
            cohort_month,
            m,
            COUNT(*) FILTER (WHERE aged_into) AS cohort_size,
            COUNT(*) FILTER (WHERE aged_into AND net_state > 0) AS alive,
            bool_and(aged_into) AS fully_aged
        FROM ledger
        GROUP BY cohort_month, m
        ORDER BY cohort_month, m
    """), params).fetchall()

    cohort_sizes: dict = {}
    grid: dict = {}
    for r in rows:
        key = r.cohort_month.strftime("%Y-%m")
        grid.setdefault(key, {})[r.m] = {
            "alive": int(r.alive) if r.fully_aged else None,
            "aged": bool(r.fully_aged),
        }
        if r.m == 0:
            cohort_sizes[key] = int(r.cohort_size)

    result_cohorts = []
    for cohort_month in sorted(grid.keys()):
        size = cohort_sizes.get(cohort_month, 0)
        suppressed = size < MIN_COHORT_SIZE
        cells = []
        for m in range(months):
            cell = grid[cohort_month].get(m)
            if cell is None or not cell["aged"]:
                cells.append({"m": m, "alive": None, "rate": None})
            elif suppressed:
                cells.append({"m": m, "alive": cell["alive"], "rate": None})
            else:
                cells.append({
                    "m": m,
                    "alive": cell["alive"],
                    "rate": round(cell["alive"] / size, 4) if size else None,
                })
        result_cohorts.append({
            "cohort_month": cohort_month,
            "size": size,
            "suppressed": suppressed,
            "cells": cells,
        })

    return {
        "filters": {"channel": channel, "tier": tier, "zip": zip_code},
        "months": months,
        "cohorts": result_cohorts,
    }
