"""Auto-converted from alembic migration `p7q8r9s0t1u2_add_lead_quality_snapshots` (revision p7q8r9s0t1u2).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_p7q8r9s0t1u2_add_lead_quality_snapshots.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE lead_quality_snapshots (
    id SERIAL NOT NULL, 
    property_id INTEGER NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    sent_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    snapshot_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    score_at_send NUMERIC(5, 2), 
    tier_at_send VARCHAR(20), 
    signals_at_send JSONB, 
    score_at_snapshot NUMERIC(5, 2), 
    tier_at_snapshot VARCHAR(20), 
    still_gold_plus BOOLEAN NOT NULL, 
    has_deed_transfer BOOLEAN NOT NULL, 
    has_resolved_signals BOOLEAN NOT NULL, 
    outcome VARCHAR(20) NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    CONSTRAINT uq_lead_quality_snapshot UNIQUE (property_id, subscriber_id, sent_at)
);

CREATE INDEX idx_lqs_snapshot_at ON lead_quality_snapshots (snapshot_at);

CREATE INDEX idx_lqs_outcome ON lead_quality_snapshots (outcome);

CREATE INDEX ix_lead_quality_snapshots_property_id ON lead_quality_snapshots (property_id);

CREATE INDEX ix_lead_quality_snapshots_subscriber_id ON lead_quality_snapshots (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied p7q8r9s0t1u2_add_lead_quality_snapshots")


if __name__ == "__main__":
    main()
