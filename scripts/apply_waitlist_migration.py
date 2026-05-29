"""
Apply fa040_waitlist_entries DDL directly via SQLAlchemy text().

Alembic CLI is unusable in this repo (multi-head tree). This script is the
canonical way to apply the waitlist_entries schema change.

Usage:
    python scripts/apply_waitlist_migration.py [--dry-run]
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
    # 1. Create waitlist_entries
    """
    CREATE TABLE IF NOT EXISTS waitlist_entries (
        id                      BIGSERIAL PRIMARY KEY,
        zip_code                VARCHAR(10)  NOT NULL,
        vertical                VARCHAR(50)  NOT NULL,
        county_id               VARCHAR(50)  NOT NULL,
        name                    VARCHAR(120) NOT NULL,
        email                   VARCHAR(255) NOT NULL,
        phone_e164              VARCHAR(20),
        sms_opt_in              BOOLEAN      NOT NULL DEFAULT FALSE,
        waitlist_type           VARCHAR(20)  NOT NULL DEFAULT 'sold_out',
        signup_ip               VARCHAR(45),
        created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        notified_email_at       TIMESTAMPTZ,
        notified_sms_at         TIMESTAMPTZ,
        reactivation_decision_id VARCHAR(36),
        status                  VARCHAR(20)  NOT NULL DEFAULT 'waiting',
        CONSTRAINT ck_waitlist_entries_status
            CHECK (status IN ('waiting','notified','converted','expired','opted_out','lost')),
        CONSTRAINT ck_waitlist_entries_type
            CHECK (waitlist_type IN ('coming_soon','sold_out')),
        CONSTRAINT ck_waitlist_entries_vertical
            CHECK (vertical IN ('roofing','restoration','public_adjusters',
                                'wholesalers','fix_flip','attorneys')),
        CONSTRAINT uq_waitlist_zip_vert_county_email
            UNIQUE (zip_code, vertical, county_id, email)
    )
    """,

    # 2. Indexes
    """
    CREATE INDEX IF NOT EXISTS ix_waitlist_county_status
        ON waitlist_entries (county_id, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_waitlist_county_type_status
        ON waitlist_entries (county_id, waitlist_type, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_waitlist_zip_vertical
        ON waitlist_entries (zip_code, vertical)
    """,

    # 3. Extend sms_opt_ins.source constraint to include waitlist_form.
    #    Drop old constraint, re-add with extended set.
    """
    ALTER TABLE sms_opt_ins
        DROP CONSTRAINT IF EXISTS check_opt_in_source
    """,
    """
    ALTER TABLE sms_opt_ins
        ADD CONSTRAINT check_opt_in_source
        CHECK (source IN ('double_opt_in','manual','import','widget','waitlist_form'))
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
        logger.info("Migration fa040_waitlist_entries applied successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
