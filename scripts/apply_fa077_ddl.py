"""Apply fa077 — add description column to building_permits.

Pinellas County does not use a roofing-specific permit_type — roofing jobs
come in as "Express Permit" with roofing details only in the Description field
(e.g. "Reroof Metal", "Shingle and/or Flat", "Tile"). The permit_engine scraper
already captures Description in the CSV but the column was missing from the table.

Idempotent: uses ADD COLUMN IF NOT EXISTS. Safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa077_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        session.execute(text(
            "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS description TEXT"
        ))
        logger.info("OK: description column added to building_permits")
    logger.info("fa077 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
