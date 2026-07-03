"""Auto-converted from alembic migration `fa004_premium_purchases` (revision fa004_premium_purchases).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa004_premium_purchases.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE premium_purchases (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    sku VARCHAR(30) NOT NULL, 
    paid_via VARCHAR(10) NOT NULL, 
    amount_cents INTEGER, 
    credits_spent INTEGER, 
    stripe_payment_intent_id VARCHAR(100), 
    property_id INTEGER, 
    target_address VARCHAR(255), 
    output_ref VARCHAR(255), 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    purchased_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    delivered_at TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id), 
    CONSTRAINT uq_premium_purchase_pi UNIQUE (stripe_payment_intent_id), 
    CONSTRAINT check_premium_sku CHECK (sku IN ('report', 'brief', 'transfer', 'byol')), 
    CONSTRAINT check_premium_paid_via CHECK (paid_via IN ('credits', 'card')), 
    CONSTRAINT check_premium_status CHECK (status IN ('pending', 'delivered', 'failed'))
);

CREATE INDEX idx_premium_purchase_sub_sku ON premium_purchases (subscriber_id, sku);

CREATE INDEX ix_premium_purchases_subscriber_id ON premium_purchases (subscriber_id);

CREATE INDEX ix_premium_purchases_property_id ON premium_purchases (property_id);

CREATE INDEX ix_premium_purchases_stripe_payment_intent_id ON premium_purchases (stripe_payment_intent_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa004_premium_purchases")


if __name__ == "__main__":
    main()
