"""WP-T2-11 — GYR Opportunity Router schema migration.

Idempotent. Run once against the shared DB:
    PYTHONPATH=. python migrations/apply_fa_max_wp_t2_11_opportunity_router.py

Adds GYR columns to fa_max_opportunities and creates fa_max_gyr_routing_log.
"""
import logging
import sys

from sqlalchemy import text

sys.path.insert(0, ".")

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


DDL = [
    # GYR columns on fa_max_opportunities
    """
    ALTER TABLE fa_max_opportunities
        ADD COLUMN IF NOT EXISTS gyr_color VARCHAR(10)
            CHECK (gyr_color IN ('green','yellow','red')),
        ADD COLUMN IF NOT EXISTS expected_revenue_cents BIGINT,
        ADD COLUMN IF NOT EXISTS gyr_reason JSONB,
        ADD COLUMN IF NOT EXISTS gyr_ranked_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS gyr_stale_alerted_at TIMESTAMPTZ
    """,
    # Partial index for MONEY ranking query (open opps only)
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_opp_gyr_money
        ON fa_max_opportunities (gyr_color, expected_revenue_cents DESC)
        WHERE outcome = 'open'
    """,
    # Audit log table
    """
    CREATE TABLE IF NOT EXISTS fa_max_gyr_routing_log (
        id BIGSERIAL PRIMARY KEY,
        opportunity_id UUID NOT NULL
            REFERENCES fa_max_opportunities(opportunity_id)
            ON DELETE CASCADE,
        color VARCHAR(10) NOT NULL
            CHECK (color IN ('green','yellow','red')),
        expected_revenue_cents BIGINT,
        reason_codes JSONB,
        disqualifying_rule TEXT,
        queue VARCHAR(12)
            CHECK (queue IN ('MONEY','EXCEPTIONS') OR queue IS NULL),
        decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_gyr_log_opp_decided
        ON fa_max_gyr_routing_log (opportunity_id, decided_at DESC)
    """,
]


def run() -> None:
    with get_db_context() as db:
        for stmt in DDL:
            db.execute(text(stmt.strip()))
        db.commit()
    log.info("apply_fa_max_wp_t2_11_opportunity_router: done")


if __name__ == "__main__":
    run()
