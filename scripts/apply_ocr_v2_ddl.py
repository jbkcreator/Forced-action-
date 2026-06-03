"""Apply OCR v2 schema — adds PDF extraction columns to legal_and_liens.

Idempotent: safe to run multiple times.

Usage:
    python scripts/apply_ocr_v2_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    # New OCR + identifier columns
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS case_number VARCHAR(100)",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS parcel_id VARCHAR(100)",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS property_address TEXT",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS normalized_property_address TEXT",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS pdf_url TEXT",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS pdf_path TEXT",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS ocr_status VARCHAR(30) DEFAULT 'pending'",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS ocr_confidence NUMERIC(5,4)",
    "ALTER TABLE legal_and_liens ADD COLUMN IF NOT EXISTS ocr_extracted_at TIMESTAMP WITHOUT TIME ZONE",
    # Indexes
    "CREATE INDEX IF NOT EXISTS idx_legal_ocr_status ON legal_and_liens(ocr_status)",
    "CREATE INDEX IF NOT EXISTS idx_legal_parcel_id ON legal_and_liens(parcel_id)",
    "CREATE INDEX IF NOT EXISTS idx_legal_case_number ON legal_and_liens(case_number)",
    # Widen match_method constraint to include parcel_id + normalized_address
    # (DROP is safe because IF NOT EXISTS check on the constraint name)
    """
    DO $$ BEGIN
        ALTER TABLE legal_and_liens DROP CONSTRAINT IF EXISTS check_legal_match_method;
        ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_match_method
            CHECK (match_method IS NULL OR match_method IN (
                'legal_desc', 'owner_name', 'llm_verified', 'address',
                'manual', 'parcel_id', 'normalized_address'
            ));
    END $$
    """,
    # ocr_status constraint
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_legal_ocr_status'
        ) THEN
            ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_ocr_status
                CHECK (ocr_status IS NULL OR ocr_status IN (
                    'pending', 'downloaded', 'extracted', 'low_confidence',
                    'failed_download', 'failed_extraction'
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
    logger.info("OCR v2 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
