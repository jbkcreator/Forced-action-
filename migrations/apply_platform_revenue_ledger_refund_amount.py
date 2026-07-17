"""Add platform_revenue_ledger.refunded_amount_cents.

mark_ledger_refunded previously only recorded a refunded_at timestamp, so a
partial refund excluded a ledger row's FULL amount_cents from revenue
reporting instead of just the refunded portion. This column lets callers
that know the actual refunded amount (Stripe's own amount_refunded) record
it; callers that don't are unaffected — mark_ledger_refunded() falls back to
the row's own amount_cents (full-refund assumption, matching prior behavior).

Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_platform_revenue_ledger_refund_amount.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE platform_revenue_ledger "
    "ADD COLUMN IF NOT EXISTS refunded_amount_cents INTEGER;",
]


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    logger.info("platform_revenue_ledger.refunded_amount_cents applied.")


if __name__ == "__main__":
    main()
