"""Auto-converted from alembic migration `fa016_accelerated_wallet_push` (revision fa016_accel_wallet_push).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa016_accelerated_wallet_push.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN phone VARCHAR(20);

ALTER TABLE subscribers ADD CONSTRAINT uq_subscribers_phone UNIQUE (phone);

CREATE INDEX idx_subscribers_phone ON subscribers (phone);

ALTER TABLE subscribers ADD COLUMN wallet_opt_out BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE subscribers ADD COLUMN missed_lead_count INTEGER DEFAULT '0' NOT NULL;

CREATE TABLE wallet_push_offers (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    decision_id VARCHAR(36), 
    framing_variant VARCHAR(20) NOT NULL, 
    ab_variant VARCHAR(1), 
    tier VARCHAR(20) DEFAULT 'starter_wallet' NOT NULL, 
    status VARCHAR(20) DEFAULT 'offered' NOT NULL, 
    stripe_subscription_id VARCHAR(100), 
    offered_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    accepted_at TIMESTAMP WITHOUT TIME ZONE, 
    declined_at TIMESTAMP WITHOUT TIME ZONE, 
    activated_at TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT check_wallet_push_offer_status CHECK (status IN ('offered','accepted','declined','activated','expired','failed')), 
    CONSTRAINT check_wallet_push_framing_variant CHECK (framing_variant IN ('missing_leads','credits_ready')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id) ON DELETE CASCADE
);

CREATE INDEX idx_wallet_push_offers_subscriber_offered ON wallet_push_offers (subscriber_id, offered_at);

CREATE INDEX idx_wallet_push_offers_decision ON wallet_push_offers (decision_id);

CREATE INDEX idx_wallet_push_offers_subscription ON wallet_push_offers (stripe_subscription_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa016_accelerated_wallet_push")


if __name__ == "__main__":
    main()
