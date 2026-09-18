"""Stage A + A′ — WP-T2-8 Construction/Builder Engine schema.

building_permits: add contractor_name, holder_name, job_value, completion_status.
permit_staging:   new table for unmatched permits (no property FK).

Idempotent — ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT EXISTS throughout.

Usage:
    PYTHONPATH=. python migrations/apply_builder_permit_enrichment.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # Stage A — enrich building_permits
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_name  TEXT;",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS holder_name       TEXT;",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS job_value         NUMERIC(14, 2);",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS completion_status VARCHAR(50);",
    "CREATE INDEX IF NOT EXISTS idx_permit_holder_name      ON building_permits (holder_name);",
    "CREATE INDEX IF NOT EXISTS idx_permit_contractor_name  ON building_permits (contractor_name);",

    # Stage A′ — permit_staging for unmatched permits
    """
    CREATE TABLE IF NOT EXISTS permit_staging (
        id                   SERIAL PRIMARY KEY,
        permit_number        VARCHAR(100) NOT NULL,
        permit_type          VARCHAR(100),
        county_id            VARCHAR(50),
        address              TEXT,
        holder_name          TEXT,
        contractor_name      TEXT,
        job_value            NUMERIC(14, 2),
        completion_status    VARCHAR(50),
        status               VARCHAR(50),
        description          TEXT,
        issue_date           DATE,
        expire_date          DATE,
        date_added           DATE DEFAULT CURRENT_DATE,
        matched              BOOLEAN NOT NULL DEFAULT FALSE,
        matched_property_id  INTEGER REFERENCES properties(id),
        CONSTRAINT uq_permit_staging_permit_number UNIQUE (permit_number)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_permit_number   ON permit_staging (permit_number);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_county_id        ON permit_staging (county_id);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_date_added       ON permit_staging (date_added);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_matched          ON permit_staging (matched);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_matched_prop     ON permit_staging (matched_property_id);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_holder           ON permit_staging (holder_name);",
    "CREATE INDEX IF NOT EXISTS idx_permit_staging_county_issue     ON permit_staging (county_id, issue_date);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("apply_builder_permit_enrichment complete.")


if __name__ == "__main__":
    main()
