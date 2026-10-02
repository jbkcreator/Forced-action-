"""Migration: lending_calling_pool_staging v2 — WP-W0-1 client-doc alignment.

Adds two columns to lending_calling_pool_staging:

  line_type      — 'mobile' | 'landline' | 'unknown'; sourced from DBPR's
                   separate mobile_phone / landline_phone columns for Pool 2,
                   'unknown' for pools where source doesn't distinguish.
                   Required by the dialer (mobile-first ring order).

  campaign_list  — Josh's List 1-9 campaign taxonomy:
                   List 3  = DBPR-licensed builders (active_builder_dbpr)
                   List 7  = NOC/permit property owners (active_builder_noc)
                   List 4  = Mortgage brokers
                   NULL    = wholesaler_flipper (pending Josh confirmation)

Idempotent — safe to re-run.  Requires apply_lending_calling_pool_staging.py
to have been run first.

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
ALTER TABLE lending_calling_pool_staging
    ADD COLUMN IF NOT EXISTS line_type TEXT
        CHECK (line_type IN ('mobile', 'landline', 'unknown'));

ALTER TABLE lending_calling_pool_staging
    ADD COLUMN IF NOT EXISTS campaign_list TEXT;

CREATE INDEX IF NOT EXISTS idx_lcps_campaign_list
    ON lending_calling_pool_staging (campaign_list)
    WHERE campaign_list IS NOT NULL;
"""


def apply(session) -> None:
    logger.info("Applying lending_calling_pool_staging v2 (line_type + campaign_list)…")
    session.execute(text(DDL))
    session.commit()
    logger.info("Done — line_type and campaign_list columns ready.")


def main() -> None:
    with get_db_context() as session:
        apply(session)


if __name__ == "__main__":
    main()
    sys.exit(0)
