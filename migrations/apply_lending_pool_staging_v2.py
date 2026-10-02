"""Migration: lending.calling_pool_staging v2 — adds line_type.

Adds one column to lending.calling_pool_staging:

  line_type — 'mobile' | 'landline' | 'unknown'; sourced from DBPR's separate
              mobile_phone / landline_phone columns for Pool 2 (List 3),
              'unknown' for pools where the source doesn't distinguish.
              Required by the dialer (mobile-first ring order) and directly
              answers a question Josh asked in his own launch email ("Does
              every phone record carry its line type?").

Idempotent — safe to re-run. Run AFTER apply_lending_pool_staging_schema.py
(the table must already be lending.calling_pool_staging, not the old
public.lending_calling_pool_staging, by the time this runs).

This migration previously also added a `campaign_list` column; that field
was retired during the #318/#320 branch reconciliation in favor of the
`source_tag` column (apply_lending_pool_source_tags.py), which already
covers the same Josh's-List-1-9 purpose and was further along in production.
If `campaign_list` was ever added to a real database by an earlier version
of this script, it's a harmless orphan column — nothing in code reads it;
drop it manually if you want the schema fully clean.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pool_staging_v2.py
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


DDL = """
ALTER TABLE lending.calling_pool_staging
    ADD COLUMN IF NOT EXISTS line_type TEXT
        CHECK (line_type IN ('mobile', 'landline', 'unknown'));
"""


def apply(session) -> None:
    logger.info("Applying lending.calling_pool_staging v2 (line_type)…")
    session.execute(text(DDL))
    session.commit()
    logger.info("Done — line_type column ready.")


def main() -> None:
    with get_db_context() as session:
        apply(session)


if __name__ == "__main__":
    main()
    sys.exit(0)
