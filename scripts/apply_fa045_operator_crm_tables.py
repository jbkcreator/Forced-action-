"""Auto-converted from alembic migration `fa045_operator_crm_tables` (revision fa045_operator_crm).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa045_operator_crm_tables.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE subscriber_notes (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    author_email VARCHAR(255) NOT NULL, 
    body TEXT NOT NULL, 
    pinned BOOLEAN DEFAULT false NOT NULL, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_subscriber_notes_sub_pinned ON subscriber_notes (subscriber_id, pinned, created_at);

CREATE TABLE deal_pipeline_events (
    id SERIAL NOT NULL, 
    deal_id INTEGER NOT NULL, 
    from_stage VARCHAR(30), 
    to_stage VARCHAR(30) NOT NULL, 
    changed_by VARCHAR(255) NOT NULL, 
    note TEXT, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(deal_id) REFERENCES deal_outcomes (id)
);

CREATE INDEX idx_deal_pipeline_events_deal_created ON deal_pipeline_events (deal_id, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa045_operator_crm_tables")


if __name__ == "__main__":
    main()
