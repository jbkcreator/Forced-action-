"""Backfilled from alembic migration `p6q7r8s9t0u1_add_bundle_purchases`.

bundle_purchases table. Idempotent. Live DB already has this; kept so every
schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_p6q7r8s9t0u1_add_bundle_purchases.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE IF NOT EXISTS bundle_purchases (
    id                        SERIAL PRIMARY KEY,
    subscriber_id             INTEGER NOT NULL REFERENCES subscribers(id),
    bundle_type               VARCHAR(30) NOT NULL,
    stripe_payment_intent_id  VARCHAR(100) NOT NULL UNIQUE,
    status                    VARCHAR(20) NOT NULL,
    zip_code                  VARCHAR(10),
    vertical                  VARCHAR(50),
    county_id                 VARCHAR(50) NOT NULL,
    credits_awarded           INTEGER NOT NULL,
    lead_ids                  INTEGER[],
    purchased_at              TIMESTAMP,
    expires_at                TIMESTAMP
);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied p6q7r8s9t0u1_add_bundle_purchases")


if __name__ == "__main__":
    main()
