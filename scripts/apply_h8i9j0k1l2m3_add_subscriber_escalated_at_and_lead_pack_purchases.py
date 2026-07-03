"""Auto-converted from alembic migration `h8i9j0k1l2m3_add_subscriber_escalated_at_and_lead_pack_purchases` (revision h8i9j0k1l2m3).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_h8i9j0k1l2m3_add_subscriber_escalated_at_and_lead_pack_purchases.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN escalated_at TIMESTAMP WITHOUT TIME ZONE;

CREATE TABLE lead_pack_purchases (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    zip_code VARCHAR(10) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    stripe_payment_intent_id VARCHAR(100) NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    purchased_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    delivered_at TIMESTAMP WITHOUT TIME ZONE, 
    exclusive_until TIMESTAMP WITHOUT TIME ZONE, 
    lead_ids INTEGER[], 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    CONSTRAINT uq_lead_pack_payment_intent UNIQUE (stripe_payment_intent_id), 
    CONSTRAINT check_lead_pack_status CHECK (status IN ('pending', 'delivered', 'expired'))
);

CREATE INDEX idx_lead_pack_zip_vertical ON lead_pack_purchases (zip_code, vertical);

CREATE INDEX idx_lead_pack_exclusive_until ON lead_pack_purchases (exclusive_until);

CREATE INDEX idx_lead_pack_subscriber ON lead_pack_purchases (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied h8i9j0k1l2m3_add_subscriber_escalated_at_and_lead_pack_purchases")


if __name__ == "__main__":
    main()
