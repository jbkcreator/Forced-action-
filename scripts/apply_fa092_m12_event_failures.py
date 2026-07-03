"""Auto-converted from alembic migration `fa092_m12_event_failures` (revision fa092_m12_event_failures).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa092_m12_event_failures.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE event_failures (
    event_id UUID NOT NULL, 
    consumer VARCHAR(100) NOT NULL, 
    retry_count INTEGER DEFAULT '0' NOT NULL, 
    last_error TEXT, 
    failed_permanently BOOLEAN DEFAULT 'false' NOT NULL, 
    last_attempt_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (event_id, consumer), 
    FOREIGN KEY(event_id) REFERENCES events (event_id)
);

CREATE INDEX idx_event_failures_consumer_permanent ON event_failures (consumer, failed_permanently);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa092_m12_event_failures")


if __name__ == "__main__":
    main()
