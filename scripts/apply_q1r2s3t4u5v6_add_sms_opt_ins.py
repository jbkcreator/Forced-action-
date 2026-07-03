"""Auto-converted from alembic migration `q1r2s3t4u5v6_add_sms_opt_ins` (revision q1r2s3t4u5v6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_q1r2s3t4u5v6_add_sms_opt_ins.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE sms_opt_ins (
    id SERIAL NOT NULL, 
    phone VARCHAR(20) NOT NULL, 
    subscriber_id INTEGER, 
    keyword_used VARCHAR(20), 
    source VARCHAR(30) NOT NULL, 
    opt_in_message TEXT, 
    opted_in_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    ip_address VARCHAR(50), 
    PRIMARY KEY (id), 
    CONSTRAINT check_opt_in_source CHECK (source IN ('double_opt_in', 'manual', 'import', 'widget')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    UNIQUE (phone)
);

CREATE INDEX idx_sms_opt_in_phone ON sms_opt_ins (phone);

CREATE INDEX idx_sms_opt_in_subscriber ON sms_opt_ins (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied q1r2s3t4u5v6_add_sms_opt_ins")


if __name__ == "__main__":
    main()
