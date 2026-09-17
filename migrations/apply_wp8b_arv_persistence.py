"""Apply WP-8B canonical ARV persistence table.

Backs the D6-WP7 consumption contract: the ONE canonical ARV store, keyed by
property_id, spine-independent (no dependency on WP-1 opportunities).

  fa_max_arv_results -- one row per computed valuation. Determinative-input
                        changes INSERT a new row + mark the prior superseded;
                        an identical recompute is a no-op. low/high/point are
                        stored already rounded to nearest $5,000. selected_comps
                        and locality_tier are internal-only (never on the
                        borrower-facing PublishedARV projection).

Idempotent — IF NOT EXISTS guards throughout. Requires the shared DB's
`generate_uuidv7()` function (already present, used by other fa_max/loan-lane
UUID PKs).

Usage:
    PYTHONPATH=. python migrations/apply_wp8b_arv_persistence.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS fa_max_arv_results (
        arv_result_id UUID PRIMARY KEY DEFAULT generate_uuidv7(),
        property_id BIGINT NOT NULL,
        computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        low NUMERIC(14, 2),
        high NUMERIC(14, 2),
        point NUMERIC(14, 2),
        confidence TEXT,
        comp_count INTEGER NOT NULL DEFAULT 0,
        weak_comp BOOLEAN NOT NULL DEFAULT true,
        locality_tier TEXT,
        selected_comps JSONB,
        source TEXT NOT NULL,
        arv_unknown BOOLEAN NOT NULL DEFAULT false,
        calculation_version TEXT NOT NULL,
        input_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'computed',
        supersedes_result_id UUID,
        CONSTRAINT ck_fa_max_arv_status CHECK (status IN ('computed', 'superseded'))
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_fa_max_arv_property_computed "
    "ON fa_max_arv_results (property_id, computed_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_fa_max_arv_property_status "
    "ON fa_max_arv_results (property_id, status);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_fa_max_arv_one_computed_per_property "
    "ON fa_max_arv_results (property_id) WHERE status = 'computed';",
]


def run(conn) -> None:
    for i, stmt in enumerate(DDL, 1):
        logger.info("DDL step %d/%d", i, len(DDL))
        conn.execute(text(stmt))
    logger.info("wp8b_arv_persistence migration complete.")


if __name__ == "__main__":
    engine = create_engine(str(get_settings().database_url))
    with engine.begin() as conn:
        run(conn)
