"""Apply PR #268 review-fix columns for borrower-intelligence.

Two review findings back these columns:

  1. merge preserves the borrower ledger (was CASCADE-deleted). merge_entities
     now reassigns borrower_ledger_events + borrower_monitor_log absorbed->
     surviving and records the moved ids so unmerge can reverse precisely.
       buyer_entity_merge_log.moved_ledger_event_ids  JSONB
       buyer_entity_merge_log.moved_monitor_log_ids   JSONB

  2. portfolio_expansion fires on crossing a milestone (>=), deduped on the
     highest milestone already alerted rather than exact count equality.
       borrower_monitor_log.monitor_value  INT   (the milestone that fired)

Idempotent — ADD COLUMN IF NOT EXISTS throughout.

Usage:
    PYTHONPATH=. python migrations/apply_borrower_intelligence_review_fixes.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE buyer_entity_merge_log "
    "ADD COLUMN IF NOT EXISTS moved_ledger_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb;",
    "ALTER TABLE buyer_entity_merge_log "
    "ADD COLUMN IF NOT EXISTS moved_monitor_log_ids JSONB NOT NULL DEFAULT '[]'::jsonb;",
    "ALTER TABLE borrower_monitor_log "
    "ADD COLUMN IF NOT EXISTS monitor_value INTEGER;",
]


def apply(engine=None) -> None:
    if engine is None:
        engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))
    logger.info("borrower_intelligence review-fix columns applied.")


if __name__ == "__main__":
    apply()
    print("Done.")
