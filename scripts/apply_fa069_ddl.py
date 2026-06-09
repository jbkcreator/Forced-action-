"""Apply fa069 — court-docket detail columns on legal_proceedings.

Adds the Stage-2 docket-detail enrichment columns written by the per-engine
detail extractor (Pinellas Eviction/Probate/Divorce):

  mailing_address  TEXT          — promoted party mailing address (per record_type)
  docket_detail    JSONB         — full scrape_case() payload (header/parties/events/...)
  balance_due      NUMERIC(12,2) — Financial section Balance Due (commas stripped)
  docket_status    VARCHAR(30)   — ok | case_number_missing | not_found | blocked | error

Idempotent: safe to run multiple times (mirrors apply_ocr_v2_ddl.py).

Usage:
    python scripts/apply_fa069_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    # Docket-detail columns
    "ALTER TABLE legal_proceedings ADD COLUMN IF NOT EXISTS mailing_address TEXT",
    "ALTER TABLE legal_proceedings ADD COLUMN IF NOT EXISTS docket_detail JSONB",
    "ALTER TABLE legal_proceedings ADD COLUMN IF NOT EXISTS balance_due NUMERIC(12,2)",
    "ALTER TABLE legal_proceedings ADD COLUMN IF NOT EXISTS docket_status VARCHAR(30)",
    # Selection index — daily run filters `docket_status IS NULL`
    "CREATE INDEX IF NOT EXISTS idx_proceeding_docket_status ON legal_proceedings(docket_status)",
    # docket_status CHECK constraint (guarded — re-runnable)
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_proceeding_docket_status'
        ) THEN
            ALTER TABLE legal_proceedings ADD CONSTRAINT check_proceeding_docket_status
                CHECK (docket_status IS NULL OR docket_status IN (
                    'ok', 'case_number_missing', 'not_found', 'blocked', 'error'
                ));
        END IF;
    END $$
    """,
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip().splitlines()[0][:80])
    logger.info("fa069 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
