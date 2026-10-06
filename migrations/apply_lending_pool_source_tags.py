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

POOLS = ("wholesaler_flipper", "active_builder", "mortgage_broker", "auction_winner", "permit_owner")


def apply(session, table: str = "lending.calling_pool_staging") -> None:
    allowed = ", ".join(f"'{p}'" for p in POOLS)
    session.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source_tag TEXT"))
    session.execute(text(f"CREATE INDEX IF NOT EXISTS idx_lcps_source_tag ON {table} (source_tag)"))
    # The original CHECK carries the pre-move table name (public.lending_calling_pool_staging); a
    # database built from migrations alone gets Postgres' default name for the inline CHECK in
    # apply_lending_calling_pool_staging.py. Drop both so the old 3-pool CHECK never survives.
    for stale in ("lending_calling_pool_staging_pool_name_check", "calling_pool_staging_pool_name_check"):
        session.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {stale}"))
    session.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT lending_calling_pool_staging_pool_name_check "
                         f"CHECK (pool_name IN ({allowed}))"))


def main() -> None:
    with get_db_context() as session:
        apply(session)
        session.commit()
    logger.info("apply_lending_pool_source_tags complete.")


if __name__ == "__main__":
    main()
