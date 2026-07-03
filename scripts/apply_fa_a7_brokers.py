"""Auto-converted from alembic migration `fa_a7_brokers` (revision fa_a7_brokers).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa_a7_brokers.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE brokers (
    broker_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    email VARCHAR NOT NULL, 
    name VARCHAR NOT NULL, 
    password_hash VARCHAR, 
    role VARCHAR(20) DEFAULT 'broker' NOT NULL, 
    is_active BOOLEAN DEFAULT 'true' NOT NULL, 
    reset_token VARCHAR, 
    reset_token_expires_at TIMESTAMP WITH TIME ZONE, 
    last_login_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (broker_id), 
    CONSTRAINT brokers_email_key UNIQUE (email), 
    CONSTRAINT ck_brokers_role CHECK (role = 'broker')
);

CREATE INDEX ix_brokers_reset_token ON brokers (reset_token);

CREATE INDEX ix_brokers_is_active ON brokers (is_active);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa_a7_brokers")


if __name__ == "__main__":
    main()
