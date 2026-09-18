"""
Migration: fix lender_box_geographies unique constraint to use NULLS NOT DISTINCT.

The original constraint UNIQUE (program_key, state, county) treats NULL county
values as distinct from each other (standard Postgres behaviour), so
ON CONFLICT DO NOTHING never fires for the 6 state-wide rows (county = NULL).
Every re-run of apply_lender_box.py inserted 6 fresh duplicate rows.

This migration:
  1. Deduplicates lender_box_geographies — keeps the lowest id per
     (program_key, state, county) group, deletes the rest.
  2. Drops the old constraint.
  3. Adds UNIQUE NULLS NOT DISTINCT (program_key, state, county) so
     re-runs of apply_lender_box.py are true no-ops going forward.

Requires Postgres 15+. Idempotent — safe to run more than once.
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

DEDUP_SQL = """
DELETE FROM lender_box_geographies
WHERE id NOT IN (
    SELECT MIN(id)
    FROM lender_box_geographies
    GROUP BY program_key, state, county
)
"""

DROP_OLD_CONSTRAINT = """
ALTER TABLE lender_box_geographies
DROP CONSTRAINT IF EXISTS uq_lender_box_geographies_program_state_county
"""

ADD_NEW_CONSTRAINT = """
ALTER TABLE lender_box_geographies
ADD CONSTRAINT uq_lender_box_geographies_program_state_county
    UNIQUE NULLS NOT DISTINCT (program_key, state, county)
"""

CHECK_CONSTRAINT_EXISTS = """
SELECT 1
FROM pg_constraint
WHERE conname = 'uq_lender_box_geographies_program_state_county'
  AND conrelid = 'lender_box_geographies'::regclass
"""

CHECK_TABLE_EXISTS = """
SELECT 1
FROM information_schema.tables
WHERE table_name = 'lender_box_geographies'
"""


def main() -> int:
    with get_db_context() as db:
        # Skip entirely if the table doesn't exist yet — apply_lender_box.py
        # will create it with the correct constraint from scratch.
        if not db.execute(text(CHECK_TABLE_EXISTS)).fetchone():
            logger.info("lender_box_geographies does not exist — nothing to migrate")
            return 0

        # Step 1: remove duplicate rows (keep lowest id per logical key).
        result = db.execute(text(DEDUP_SQL))
        deleted = result.rowcount
        if deleted:
            logger.info("deduped %d row(s) from lender_box_geographies", deleted)
        else:
            logger.info("lender_box_geographies: no duplicate rows found")

        # Step 2: drop old constraint (IF EXISTS — idempotent).
        db.execute(text(DROP_OLD_CONSTRAINT))
        logger.info("dropped old unique constraint (if present)")

        # Step 3: add NULLS NOT DISTINCT constraint (skip if already present).
        existing = db.execute(text(CHECK_CONSTRAINT_EXISTS)).fetchone()
        if existing:
            logger.info("NULLS NOT DISTINCT constraint already in place — skipping ADD")
        else:
            db.execute(text(ADD_NEW_CONSTRAINT))
            logger.info("added UNIQUE NULLS NOT DISTINCT constraint")

        db.commit()
        logger.info("migration complete")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
