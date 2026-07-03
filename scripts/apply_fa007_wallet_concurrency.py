"""Auto-converted from alembic migration `fa007_wallet_concurrency` (revision fa007_wallet_concurrency).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa007_wallet_concurrency.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE wallet_balances ADD CONSTRAINT credits_nonneg CHECK (credits_remaining >= 0);

ALTER TABLE premium_purchases DROP CONSTRAINT check_premium_status;

ALTER TABLE premium_purchases ADD CONSTRAINT check_premium_status CHECK (status IN ('pending', 'delivered', 'failed', 'refunded', 'disputed'));

ALTER TABLE premium_purchases ADD COLUMN refunded_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE premium_purchases ADD COLUMN refund_reason VARCHAR(100);

ALTER TABLE premium_purchases ADD COLUMN refund_amount_cents INTEGER;

ALTER TABLE premium_purchases ADD COLUMN disputed_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE premium_purchases ADD COLUMN dispute_reason VARCHAR(100);

ALTER TABLE premium_purchases ADD COLUMN stripe_charge_id VARCHAR(100);

CREATE INDEX idx_premium_stripe_charge_id ON premium_purchases (stripe_charge_id);

ALTER TABLE subscribers ADD COLUMN disputed_count INTEGER DEFAULT '0' NOT NULL;

ALTER TABLE subscribers ADD COLUMN disputed_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers DROP CONSTRAINT check_subscriber_status;

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_status CHECK (status IN ('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa007_wallet_concurrency")


if __name__ == "__main__":
    main()
