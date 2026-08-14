"""Idempotent migration — 3m Deal-Room / Territory Hold correctness fixes.

Adds territory-scope columns to deal_rooms (bug #3) and widens the refund_status
CHECK constraint to allow the durable 'pending' state (bug #2).

    PYTHONPATH=. python migrations/apply_deal_room_scope_and_pending_refund.py

Safe to re-run: every statement is guarded with IF [NOT] EXISTS or drops/recreates
the constraint.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

_STATEMENTS = [
    # Bug #3 — scope a hold to (zip_code, vertical, county_id).
    "ALTER TABLE deal_rooms ADD COLUMN IF NOT EXISTS vertical VARCHAR(50)",
    "ALTER TABLE deal_rooms ADD COLUMN IF NOT EXISTS county_id VARCHAR(50)",
    "CREATE INDEX IF NOT EXISTS ix_deal_rooms_vertical ON deal_rooms (vertical)",
    # Bug #2 — allow the durable 'pending' refund state.
    "ALTER TABLE deal_rooms DROP CONSTRAINT IF EXISTS check_deal_room_refund_status",
    (
        "ALTER TABLE deal_rooms ADD CONSTRAINT check_deal_room_refund_status "
        "CHECK (refund_status IS NULL OR refund_status IN "
        "('pending', 'refunded', 'refund_failed'))"
    ),
]


def run() -> None:
    with get_db_context() as db:
        for stmt in _STATEMENTS:
            logger.info("[migration] %s", stmt)
            db.execute(text(stmt))
        db.commit()
    logger.info("[migration] deal_room scope + pending refund applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
