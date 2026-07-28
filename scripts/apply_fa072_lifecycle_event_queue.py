"""Auto-converted from alembic migration `fa072_lifecycle_event_queue` (revision fa072_lifecycle_event_queue).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa072_lifecycle_event_queue.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE lifecycle_event_queue (
    id BIGSERIAL NOT NULL, 
    event_type TEXT NOT NULL, 
    subscriber_id INTEGER, 
    payload JSONB DEFAULT '{}' NOT NULL, 
    idempotency_key TEXT, 
    status TEXT DEFAULT 'pending' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    processed_at TIMESTAMP WITH TIME ZONE, 
    error TEXT, 
    PRIMARY KEY (id), 
    UNIQUE (idempotency_key)
);

CREATE INDEX idx_lifecycle_event_queue_status_created ON lifecycle_event_queue (status, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa072_lifecycle_event_queue")


if __name__ == "__main__":
    main()
