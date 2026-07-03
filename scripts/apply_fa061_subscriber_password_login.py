"""Auto-converted from alembic migration `fa061_subscriber_password_login` (revision fa061).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa061_subscriber_password_login.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN password_hash VARCHAR(255);

ALTER TABLE subscribers ADD COLUMN password_set_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE subscribers ADD COLUMN reset_token_hash VARCHAR(64);

ALTER TABLE subscribers ADD COLUMN reset_token_expires_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_subscribers_reset_token_hash ON subscribers (reset_token_hash);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa061_subscriber_password_login")


if __name__ == "__main__":
    main()
