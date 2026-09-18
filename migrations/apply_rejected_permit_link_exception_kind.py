"""WP-T2-8 — allow kind='rejected_permit_link' on buyer_entity_match_exception.

When an operator rejects a low-confidence permit->entity link in the EXCEPTIONS
Slack lane, the link row is deleted. Without a durable record of that decision,
the next nightly resolution sweep sees the permit as unresolved, recreates the
identical singleton link, and re-alerts — the operator's rejection is lost.

This migration widens the kind CHECK so a durable 'rejected_permit_link' row can
be written. The permit extractor (extract_permit_candidates) anti-joins on these
rows so a rejected permit is never re-resolved to the same entity.

Idempotent: drops the old constraint if present and re-adds the widened one.

Usage:
    PYTHONPATH=. python migrations/apply_rejected_permit_link_exception_kind.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE buyer_entity_match_exception "
    "DROP CONSTRAINT IF EXISTS buyer_entity_match_exception_kind_check;",
    "ALTER TABLE buyer_entity_match_exception "
    "ADD CONSTRAINT buyer_entity_match_exception_kind_check "
    "CHECK (kind IN ('ambiguous_pair', 'multi_anchor_conflict', 'llm_different', "
    "'rejected_permit_link'));",
]


def main() -> None:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))
    logger.info("apply_rejected_permit_link_exception_kind complete.")


if __name__ == "__main__":
    main()
