"""Daily refresh of platform_cost_attribution for ZIP-territory subscribers.

ZIP-territory ownership is continuous, not a discrete purchase event, so
there's no webhook to hook a write into (unlike lead_unlock/lead_pack/premium,
which write via src/services/revenue_ledger.py at their confirmation point).
This job re-runs the multi-vertical collision resolution once a day and
writes a fresh, versioned (computed_for_date) batch of attribution rows —
never updating a prior day's rows in place, so a report run against a past
date stays reproducible even after territory ownership later shifts.

Excludes any property already covered by a direct-purchase attribution
(attribution_method='direct_purchase') so the same enrichment cost is never
attributed twice across the two attribution methods.

Usage:
    python -m src.tasks.zip_territory_cost_attribution_refresh
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def refresh_zip_territory_cost_attribution(db: Session, for_date: date | None = None) -> int:
    """Compute today's (or for_date's) winning ZIP-territory cost attribution
    and insert it as a new versioned batch. Idempotent for the same
    for_date via the partial unique index on
    (enrichment_usage_log_id, subscriber_id, computed_for_date) — re-running
    for the same day is a no-op, not a duplicate.

    Returns the number of rows inserted.
    """
    target_date = for_date or datetime.now(timezone.utc).date()

    result = db.execute(text("""
        WITH latest_score AS (
            SELECT DISTINCT ON (property_id) property_id, vertical_scores
            FROM distress_scores
            ORDER BY property_id, score_date DESC
        ),
        already_direct AS (
            SELECT DISTINCT enrichment_usage_log_id
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
        ),
        winning_ownership AS (
            SELECT property_id, subscriber_id
            FROM candidate_ownership
            WHERE ownership_rank = 1
        ),
        eligible_cost AS (
            SELECT eul.id AS enrichment_usage_log_id, eul.property_id, eul.cost_cents, wo.subscriber_id
            FROM enrichment_usage_logs eul
            JOIN winning_ownership wo ON wo.property_id = eul.property_id
            LEFT JOIN already_direct ad ON ad.enrichment_usage_log_id = eul.id
            WHERE eul.success = TRUE AND ad.enrichment_usage_log_id IS NULL
        )
        INSERT INTO platform_cost_attribution (
            enrichment_usage_log_id, subscriber_id, property_id,
            attribution_method, attributed_cost_cents, computed_for_date
        )
        SELECT
            enrichment_usage_log_id, subscriber_id, property_id,
            'zip_territory_highest_vertical', cost_cents, :target_date
        FROM eligible_cost
        ON CONFLICT (enrichment_usage_log_id, subscriber_id, computed_for_date)
            WHERE attribution_method = 'zip_territory_highest_vertical'
            DO NOTHING
    """), {"target_date": target_date})

    inserted = result.rowcount or 0
    # No commit here — the caller owns the transaction boundary. Calling
    # commit() inside this function broke test isolation (a caller running
    # this inside a rollback-based test fixture would have its whole session
    # permanently persisted early) and is wrong for any future caller that
    # wants to run this as one step inside a larger transaction. The
    # __main__ entry point below commits explicitly for real standalone runs.
    logger.info(
        "[ZipTerritoryCostAttribution] refreshed for_date=%s rows_inserted=%d",
        target_date, inserted,
    )
    return inserted


if __name__ == "__main__":
    from src.core.database import get_db_context

    logging.basicConfig(level=logging.INFO)
    with get_db_context() as session:
        refresh_zip_territory_cost_attribution(session)
