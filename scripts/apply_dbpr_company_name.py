"""
One-off DDL applier for the dbpr_contacts company-name columns.

Mirrors alembic/versions/fa032_dbpr_company_name.py, but applies the change
directly (idempotent — safe to re-run) for environments where the Alembic
multi-head tree isn't resolved locally. After running this, stamp the
revision once Alembic heads are merged:

    alembic stamp fa032_dbpr_company_name

Run:
    python -m scripts.apply_dbpr_company_name
    python -m scripts.apply_dbpr_company_name --dry-run
"""

import argparse

from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_STATEMENTS = [
    "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name VARCHAR(255)",
    "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name_status "
    "VARCHAR(20) NOT NULL DEFAULT 'pending'",
    "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name_scraped_at "
    "TIMESTAMPTZ",
    # CHECK constraint — add only if missing (no IF NOT EXISTS for constraints)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_dbpr_company_name_status'
        ) THEN
            ALTER TABLE dbpr_contacts ADD CONSTRAINT check_dbpr_company_name_status
                CHECK (company_name_status IN ('pending', 'found', 'none', 'failed'));
        END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS ix_dbpr_company_name_status "
    "ON dbpr_contacts (company_name_status)",
    # Widen scraper_run_stats source_type to allow the company scraper's run row.
    """
    ALTER TABLE scraper_run_stats DROP CONSTRAINT IF EXISTS check_run_stats_source_type;
    """,
    """
    ALTER TABLE scraper_run_stats ADD CONSTRAINT check_run_stats_source_type CHECK (
        source_type IN (
            'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',
            'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',
            'violations', 'foreclosures', 'permits', 'tax_delinquencies',
            'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',
            'sunbiz', 'property_appraiser', 'dbpr_company'
        )
    );
    """,
]


def main(dry_run: bool = False) -> None:
    with get_db_context() as db:
        for stmt in _STATEMENTS:
            compact = " ".join(stmt.split())
            if dry_run:
                logger.info("[apply_dbpr_company_name DRY RUN] %s", compact)
                continue
            logger.info("[apply_dbpr_company_name] %s", compact)
            db.execute(text(stmt))
        if not dry_run:
            db.commit()
            logger.info("[apply_dbpr_company_name] Done — columns/constraint/index in place")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply dbpr_contacts company-name DDL")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
