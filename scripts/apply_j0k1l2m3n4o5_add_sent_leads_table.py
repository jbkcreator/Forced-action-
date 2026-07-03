"""Auto-converted from alembic migration `j0k1l2m3n4o5_add_sent_leads_table` (revision j0k1l2m3n4o5).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_j0k1l2m3n4o5_add_sent_leads_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE sent_leads (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    property_id INTEGER NOT NULL, 
    sent_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id), 
    CONSTRAINT uq_sent_lead UNIQUE (subscriber_id, property_id)
);

CREATE INDEX idx_sent_lead_subscriber_sent_at ON sent_leads (subscriber_id, sent_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied j0k1l2m3n4o5_add_sent_leads_table")


if __name__ == "__main__":
    main()
