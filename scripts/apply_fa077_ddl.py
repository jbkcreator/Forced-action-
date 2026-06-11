"""Apply fa077 — voters table, tax_collector enriched source, owners.direct_mail_eligible.

Companion to alembic/versions/fa077_voters_and_direct_mail.py (schema-of-record).
The alembic CLI is unusable in this tree (divergent multi-head history), so this
script applies the same DDL directly.

Idempotent: CREATE TABLE IF NOT EXISTS / DROP CONSTRAINT IF EXISTS /
ADD COLUMN IF NOT EXISTS. Safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa077_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ENRICHED_SOURCES = "'batch_skip_tracing', 'idi', 'pdl', 'tracerfy', 'tax_collector'"

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS voters (
        id                  SERIAL PRIMARY KEY,
        property_id         INTEGER NOT NULL REFERENCES properties(id),
        county_id           VARCHAR(50) NOT NULL,
        source_voter_id     VARCHAR(20) NOT NULL,
        voter_name          VARCHAR(255),
        first_name          VARCHAR(100),
        middle_name         VARCHAR(100),
        last_name           VARCHAR(100),
        residential_address VARCHAR(500),
        residential_city    VARCHAR(100),
        residential_zip     VARCHAR(10),
        mailing_address     VARCHAR(500),
        registration_status VARCHAR(10),
        registration_date   DATE,
        phones              JSONB,
        phone_1             VARCHAR(20),
        email               VARCHAR(255),
        meta_data           JSONB,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at          TIMESTAMPTZ,
        CONSTRAINT uq_voter_county_source_id UNIQUE (county_id, source_voter_id),
        CONSTRAINT check_voter_registration_status
            CHECK (registration_status IN ('ACT', 'INA') OR registration_status IS NULL)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_voters_property_id ON voters (property_id)",
    "CREATE INDEX IF NOT EXISTS ix_voters_county_id ON voters (county_id)",
    "CREATE INDEX IF NOT EXISTS ix_voters_voter_name ON voters (voter_name)",
    "CREATE INDEX IF NOT EXISTS idx_voter_registration_status ON voters (registration_status)",
    "ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source",
    (
        "ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source "
        f"CHECK (source IN ({_ENRICHED_SOURCES}))"
    ),
    (
        "ALTER TABLE owners ADD COLUMN IF NOT EXISTS "
        "direct_mail_eligible BOOLEAN NOT NULL DEFAULT false"
    ),
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt))
            logger.info("OK: %s", " ".join(stmt.split())[:90])
    logger.info("fa077 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
