"""Apply fa070 — court-docket detail columns on legal_and_liens (judgments).

Mirrors fa069 (which added these to legal_proceedings). legal_and_liens already
has court_docket_parties/events/scraped_at; this adds the 4 promoted columns the
per-case docket backfill writes:

  mailing_address  TEXT
  docket_detail    JSONB
  balance_due      NUMERIC(12,2)
  docket_status    VARCHAR(30)   CHECK ok|case_number_missing|not_found|blocked|error

Idempotent. Usage: python scripts/apply_fa070_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS mailing_address TEXT",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS docket_detail JSONB",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS balance_due NUMERIC(12,2)",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS docket_status VARCHAR(30)",
    "CREATE INDEX IF NOT EXISTS idx_legal_docket_status ON legal_and_liens(docket_status)",
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='check_legal_docket_status') THEN
            ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_docket_status
                CHECK (docket_status IS NULL OR docket_status IN
                    ('ok','case_number_missing','not_found','blocked','error'));
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
    logger.info("fa070 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
