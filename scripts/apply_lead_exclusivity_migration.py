"""
Apply the lead_pack_exclusivity DDL directly via SQLAlchemy text().

Alembic CLI is unusable in this repo (multi-head tree). This script is the
canonical way to apply the cross-trade exclusivity schema (S0c / 414 scope).

Mirrors migrations/001_lead_exclusivity.sql.

Usage:
    python scripts/apply_lead_exclusivity_migration.py [--dry-run]
"""

import argparse
import logging
import sys

from sqlalchemy import text

sys.path.insert(0, ".")

from config.settings import get_settings  # noqa: E402
from src.core.database import get_db_context  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL_STATEMENTS = [
    # 1. Cross-trade exclusivity table
    """
    CREATE TABLE IF NOT EXISTS lead_exclusivity (
        id              SERIAL PRIMARY KEY,
        property_id     INTEGER NOT NULL,
        zip_code        VARCHAR(10) NOT NULL,
        county_id       VARCHAR(50) NOT NULL,
        sold_to_trade   VARCHAR(50) NOT NULL,
        source          VARCHAR(20) NOT NULL,
        source_id       INTEGER NOT NULL,
        exclusive_until TIMESTAMPTZ NOT NULL,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_property_source UNIQUE (property_id, source),
        CONSTRAINT ck_lead_exclusivity_source CHECK (source IN ('lead_pack', 'bundle'))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exclusivity_zip_county
        ON lead_exclusivity (zip_code, county_id, exclusive_until)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exclusivity_property
        ON lead_exclusivity (property_id)
    """,

    # 2. Refund columns on lead_pack_purchases
    """
    ALTER TABLE lead_pack_purchases
        ADD COLUMN IF NOT EXISTS refunded_at      TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS refund_reason    VARCHAR(100),
        ADD COLUMN IF NOT EXISTS stripe_refund_id VARCHAR(100)
    """,

    # 3. Allow 'refunded' status
    """
    ALTER TABLE lead_pack_purchases
        DROP CONSTRAINT IF EXISTS check_lead_pack_status
    """,
    """
    ALTER TABLE lead_pack_purchases
        ADD CONSTRAINT check_lead_pack_status
        CHECK (status IN ('pending', 'delivered', 'expired', 'refunded'))
    """,
]


def run(dry_run: bool = False) -> None:
    settings = get_settings()
    logger.info("Connecting to DB: %s", str(settings.database_url)[:40] + "…")

    with get_db_context() as db:
        for i, stmt in enumerate(DDL_STATEMENTS, 1):
            preview = stmt.strip().splitlines()[0][:80]
            if dry_run:
                logger.info("[dry-run] would execute (%d): %s", i, preview)
            else:
                logger.info("Executing (%d): %s", i, preview)
                db.execute(text(stmt))
                db.commit()
                logger.info("  OK")

    if dry_run:
        logger.info("Dry run complete — no changes made.")
    else:
        logger.info("Migration lead_pack_exclusivity applied successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
