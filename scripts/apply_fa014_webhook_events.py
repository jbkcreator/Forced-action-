"""Auto-converted from alembic migration `fa014_webhook_events` (revision fa014_webhook_events).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa014_webhook_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE webhook_events (
    id SERIAL NOT NULL, 
    source VARCHAR(30) NOT NULL, 
    event_type VARCHAR(80) NOT NULL, 
    direction VARCHAR(10) DEFAULT 'inbound' NOT NULL, 
    source_event_id VARCHAR(120), 
    status VARCHAR(20) DEFAULT 'received' NOT NULL, 
    status_detail TEXT, 
    subscriber_id INTEGER, 
    property_id INTEGER, 
    payload_summary JSONB, 
    duration_ms INTEGER, 
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_webhook_event_direction CHECK (direction IN ('inbound', 'outbound')), 
    CONSTRAINT check_webhook_event_status CHECK (status IN ('received', 'processed', 'failed', 'duplicate', 'skipped')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX ix_webhook_events_source ON webhook_events (source);

CREATE INDEX ix_webhook_events_event_type ON webhook_events (event_type);

CREATE INDEX ix_webhook_events_source_event_id ON webhook_events (source_event_id);

CREATE INDEX ix_webhook_events_subscriber_id ON webhook_events (subscriber_id);

CREATE INDEX ix_webhook_events_property_id ON webhook_events (property_id);

CREATE INDEX ix_webhook_events_processed_at ON webhook_events (processed_at);

CREATE INDEX idx_webhook_events_source_processed ON webhook_events (source, processed_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa014_webhook_events")


if __name__ == "__main__":
    main()
