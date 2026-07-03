"""Auto-converted from alembic migration `r2s3t4u5v6w7_add_agent_decisions` (revision r2s3t4u5v6w7).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_r2s3t4u5v6w7_add_agent_decisions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE agent_decisions (
    decision_id VARCHAR(36) NOT NULL, 
    graph_name VARCHAR(60) NOT NULL, 
    subscriber_id INTEGER, 
    event_type VARCHAR(60), 
    started_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    completed_at TIMESTAMP WITHOUT TIME ZONE, 
    terminal_status VARCHAR(20), 
    tokens_used INTEGER DEFAULT '0' NOT NULL, 
    cost_usd NUMERIC(10, 6) DEFAULT '0' NOT NULL, 
    summary JSONB, 
    PRIMARY KEY (decision_id), 
    CONSTRAINT check_agent_terminal_status CHECK (terminal_status IS NULL OR terminal_status IN ('completed', 'aborted', 'escalated', 'failed')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX ix_agent_decisions_graph_name ON agent_decisions (graph_name);

CREATE INDEX ix_agent_decisions_subscriber_id ON agent_decisions (subscriber_id);

CREATE INDEX ix_agent_decisions_event_type ON agent_decisions (event_type);

CREATE INDEX ix_agent_decisions_started_at ON agent_decisions (started_at);

CREATE INDEX idx_agent_decisions_graph_started ON agent_decisions (graph_name, started_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied r2s3t4u5v6w7_add_agent_decisions")


if __name__ == "__main__":
    main()
