"""Apply CDE-09 fix — widen unmatched_records.check_unmatched_match_method.

BaseLoader.find_property_cascade() (src/loaders/base.py) returns the full
cascade vocabulary — parcel_id, normalized_address, owner_name_zip,
owner_name_city, owner_name, legal_desc, llm_verified — but the live
CheckConstraint only allowed ('address','owner_name','legal_desc','parcel_id').
Every existing loader passing a stage-2-through-5 or LLM-verified match_method
into quarantine_unmatched() has been silently failing the constraint and
getting swallowed by quarantine_unmatched()'s broad except — the record is
dropped instead of landing in the review queue.

'address' is kept in the widened list (not dropped) — live data has 295
existing rows with that legacy value from before the cascade's stage-2
constant was renamed to 'normalized_address'; current code no longer writes
it, but ADD CONSTRAINT validates existing rows, so dropping it would make
this migration fail against any DB with that history.

Idempotent — DROP CONSTRAINT IF EXISTS before re-adding.

Usage:
    PYTHONPATH=. python scripts/apply_cde09_unmatched_match_method_fix.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_method;
    """,
    """
    ALTER TABLE unmatched_records ADD CONSTRAINT check_unmatched_match_method CHECK (
        match_method IN (
            'parcel_id', 'address', 'normalized_address', 'owner_name_zip', 'owner_name_city',
            'owner_name', 'legal_desc', 'llm_verified'
        ) OR match_method IS NULL
    );
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("cde09_unmatched_match_method_fix complete — constraint widened to full cascade vocabulary.")


if __name__ == "__main__":
    main()
