"""
Apply the lead_pack_hot_enrichment DDL directly via SQLAlchemy text().

Alembic CLI is unusable in this repo (multi-head tree). This script is the
canonical way to apply the Lead Pack Hot-Enrichment schema (S0c / ADR 0018).

Mirrors migrations/002_lead_pack_hot_enrichment.sql.

Usage:
    python scripts/apply_lead_pack_hot_enrichment_migration.py [--dry-run]
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
    # 1. Hot-Enrichment tracking columns
    """
    ALTER TABLE lead_pack_purchases
        ADD COLUMN IF NOT EXISTS enrichment_submitted_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS tracerfy_queue_id       VARCHAR(50)
    """,

    # 2. Allow the 'enriching' lifecycle state
    """
    ALTER TABLE lead_pack_purchases
        DROP CONSTRAINT IF EXISTS check_lead_pack_status
    """,
    """
    ALTER TABLE lead_pack_purchases
        ADD CONSTRAINT check_lead_pack_status
        CHECK (status IN ('pending', 'enriching', 'delivered', 'expired', 'refunded'))
    """,

    # 3. Index for the fulfillment sweep's status claim query
    """
    CREATE INDEX IF NOT EXISTS idx_lead_pack_status
        ON lead_pack_purchases (status)
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
        logger.info("Migration lead_pack_hot_enrichment applied successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
