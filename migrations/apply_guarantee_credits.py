"""Apply the guarantee_credits table (tiered volume guarantee).

Backs src/tasks/guarantee_shortfall_sweep.py — one row per subscriber per
evaluated ~30-day cycle, unique on (subscriber_id, period_end) so re-running
the sweep never double-evaluates or double-credits a cycle.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_guarantee_credits.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS guarantee_credits (
        id                      BIGSERIAL PRIMARY KEY,
        subscriber_id           INTEGER NOT NULL REFERENCES subscribers(id),
        period_start            TIMESTAMPTZ NOT NULL,
        period_end              TIMESTAMPTZ NOT NULL,
        tier                    VARCHAR(20) NOT NULL,
        quota                   INTEGER NOT NULL,
        delivered               INTEGER NOT NULL,
        shortfall               INTEGER NOT NULL,
        credit_cents            INTEGER NOT NULL DEFAULT 0,
        stripe_balance_txn_id   VARCHAR(100),
        status                  VARCHAR(30) NOT NULL
            CHECK (status IN ('met', 'issued', 'failed', 'skipped_no_charge_basis')),
        created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_guarantee_credit_subscriber_period UNIQUE (subscriber_id, period_end)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_guarantee_credits_subscriber_period "
    "ON guarantee_credits (subscriber_id, period_end);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("guarantee_credits migration complete.")


if __name__ == "__main__":
    main()
