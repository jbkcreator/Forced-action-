"""Auto-converted from alembic migration `s3t4u5v6w7x8_add_sandbox_outbox` (revision s3t4u5v6w7x8).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_s3t4u5v6w7x8_add_sandbox_outbox.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE sandbox_outbox (
    id BIGSERIAL NOT NULL, 
    channel VARCHAR(20) NOT NULL, 
    to_number VARCHAR(64), 
    body TEXT NOT NULL, 
    campaign VARCHAR(100), 
    variant_id VARCHAR(100), 
    subscriber_id INTEGER, 
    decision_id VARCHAR(36), 
    compliance_allowed BOOLEAN DEFAULT 'true' NOT NULL, 
    compliance_reason VARCHAR(60), 
    would_have_delivered BOOLEAN DEFAULT 'true' NOT NULL, 
    sandbox_flag VARCHAR(40) DEFAULT 'twilio_sandbox' NOT NULL, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_sandbox_outbox_channel CHECK (channel IN ('sms', 'voice', 'email')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX ix_sandbox_outbox_subscriber_id ON sandbox_outbox (subscriber_id);

CREATE INDEX ix_sandbox_outbox_campaign ON sandbox_outbox (campaign);

CREATE INDEX ix_sandbox_outbox_decision_id ON sandbox_outbox (decision_id);

CREATE INDEX ix_sandbox_outbox_created_at ON sandbox_outbox (created_at);

CREATE INDEX idx_sandbox_outbox_sub_created ON sandbox_outbox (subscriber_id, created_at);

CREATE INDEX idx_sandbox_outbox_campaign_created ON sandbox_outbox (campaign, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied s3t4u5v6w7x8_add_sandbox_outbox")


if __name__ == "__main__":
    main()
