"""Auto-converted from alembic migration `fa022_county_launch` (revision fa022).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa022_county_launch.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE expansion_candidates (
    id SERIAL NOT NULL, 
    county_id VARCHAR(64) NOT NULL, 
    priority INTEGER DEFAULT '100' NOT NULL, 
    status VARCHAR(16) DEFAULT 'queued' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), 
    last_slack_posted_at TIMESTAMP WITH TIME ZONE, 
    last_slack_message_ts VARCHAR(32), 
    approved_at TIMESTAMP WITH TIME ZONE, 
    approved_by_slack_user VARCHAR(32), 
    launched_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_expansion_candidates_status CHECK (status IN ('queued','approved','launching','launched','aborted','skipped')), 
    UNIQUE (county_id)
);

CREATE INDEX ix_expansion_candidates_status_priority ON expansion_candidates (status, priority);

CREATE TABLE county_launch_audit (
    id BIGSERIAL NOT NULL, 
    county_id VARCHAR(64) NOT NULL, 
    event_type VARCHAR(32) NOT NULL, 
    actor VARCHAR(64) NOT NULL, 
    gate_snapshot JSONB, 
    detail JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), 
    PRIMARY KEY (id), 
    CONSTRAINT ck_county_launch_audit_event CHECK (event_type IN ('evaluated','posted','approved','rejected','launch_started','launch_aborted_gate_red','launched','cooldown_skipped'))
);

CREATE INDEX ix_county_launch_audit_county_time ON county_launch_audit (county_id, created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa022_county_launch")


if __name__ == "__main__":
    main()
