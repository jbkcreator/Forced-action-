"""Auto-converted from alembic migration `4fd02fc1f9a4_add_stripe_webhook_events_idempotency_` (revision 4fd02fc1f9a4).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_4fd02fc1f9a4_add_stripe_webhook_events_idempotency_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE stripe_webhook_events (
    id SERIAL NOT NULL, 
    event_id VARCHAR(100) NOT NULL, 
    event_type VARCHAR(100) NOT NULL, 
    processed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX ix_stripe_webhook_events_event_id ON stripe_webhook_events (event_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 4fd02fc1f9a4_add_stripe_webhook_events_idempotency_")


if __name__ == "__main__":
    main()
