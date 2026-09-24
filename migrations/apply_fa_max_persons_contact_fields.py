"""migrations/apply_fa_max_persons_contact_fields.py

Addendum to WP-T2-6 -- adds full_name/email/phone to fa_max_persons.
Nullable so every existing row stays valid; populated going forward by the
Backflip submission-logging modal (see plan Task 19) and any future entry
point that learns a borrower's contact details.

full_name gets a pg_trgm GIN index (matches this codebase's established
fuzzy-name-search convention -- see src/loaders/base.py's ILIKE -> pg_trgm
-> rapidfuzz waterfall) so Task 16's search stays fast as the table grows.

Idempotent -- ADD COLUMN IF NOT EXISTS / safe to re-run, per ADR 0024.
"""
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

        conn.execute(text("ALTER TABLE fa_max_persons ADD COLUMN IF NOT EXISTS full_name TEXT"))
        conn.execute(text("ALTER TABLE fa_max_persons ADD COLUMN IF NOT EXISTS email TEXT"))
        conn.execute(text("ALTER TABLE fa_max_persons ADD COLUMN IF NOT EXISTS phone VARCHAR(20)"))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_fa_max_persons_full_name_trgm
                ON fa_max_persons USING gin (full_name gin_trgm_ops)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_fa_max_persons_email
                ON fa_max_persons (email) WHERE email IS NOT NULL
        """))

        logger.info("fa_max_persons.full_name/email/phone applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
