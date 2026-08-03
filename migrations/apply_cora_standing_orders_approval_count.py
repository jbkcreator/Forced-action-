"""
Add approval_count_at_proposal to cora_standing_orders (THROUGH-v2.2 T4 digest).

Idempotent (ADR 0024 — scripts-only). Lets the monthly digest show how many
clean approvals earned each rule so Josh can judge which to keep vs prune.

Apply once against the shared DB:
    PYTHONPATH=. python migrations/apply_cora_standing_orders_approval_count.py
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)


def apply() -> None:
    with get_db_context() as db:
        db.execute(text(
            "ALTER TABLE cora_standing_orders "
            "ADD COLUMN IF NOT EXISTS approval_count_at_proposal INTEGER NOT NULL DEFAULT 0"
        ))
    logger.info("cora_standing_orders.approval_count_at_proposal ensured.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
