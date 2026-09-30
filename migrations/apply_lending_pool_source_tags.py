"""Go Live source lists on the calling-pool staging table.

Adds ``source_tag`` (list_1..list_9, brief section 2.5) and allows the
``auction_winner`` pool (List 6). Idempotent; run after
apply_lending_calling_pool_staging.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pool_source_tags.py
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

POOLS = ("wholesaler_flipper", "active_builder", "mortgage_broker", "auction_winner")


def apply(session, table: str = "lending_calling_pool_staging") -> None:
    allowed = ", ".join(f"'{p}'" for p in POOLS)
    session.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source_tag TEXT"))
    session.execute(text(f"CREATE INDEX IF NOT EXISTS idx_lcps_source_tag ON {table} (source_tag)"))
    session.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS lending_calling_pool_staging_pool_name_check"))
    session.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT lending_calling_pool_staging_pool_name_check "
                         f"CHECK (pool_name IN ({allowed}))"))


def main() -> None:
    with get_db_context() as session:
        apply(session)
        session.commit()
    logger.info("apply_lending_pool_source_tags complete.")


if __name__ == "__main__":
    main()
