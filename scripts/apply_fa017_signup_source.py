"""Auto-converted from alembic migration `fa017_signup_source` (revision fa017_signup_source).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa017_signup_source.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN signup_source VARCHAR(30);

ALTER TABLE subscribers ADD COLUMN utm_source VARCHAR(100);

ALTER TABLE subscribers ADD COLUMN utm_medium VARCHAR(100);

ALTER TABLE subscribers ADD COLUMN utm_campaign VARCHAR(100);

ALTER TABLE subscribers ADD COLUMN campaign_id VARCHAR(50);

ALTER TABLE subscribers ADD COLUMN attribution_token VARCHAR(200);

UPDATE subscribers SET signup_source = 'unknown' WHERE signup_source IS NULL;

ALTER TABLE subscribers ALTER COLUMN signup_source SET NOT NULL;

ALTER TABLE subscribers ALTER COLUMN signup_source SET DEFAULT 'direct';

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_signup_source CHECK (signup_source IN ('direct','landing_page','dbpr_email','lifecycle_sms','missed_call','referral','admin','unknown'));

CREATE INDEX idx_subscriber_signup_source ON subscribers (signup_source);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa017_signup_source")


if __name__ == "__main__":
    main()
