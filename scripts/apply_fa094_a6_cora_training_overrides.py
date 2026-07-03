"""Auto-converted from alembic migration `fa094_a6_cora_training_overrides` (revision fa094_a6_cora_training_overrides).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa094_a6_cora_training_overrides.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE cora_training_overrides (
    id BIGSERIAL NOT NULL, 
    source VARCHAR(30) NOT NULL, 
    subject_type VARCHAR(30) NOT NULL, 
    subject_id INTEGER NOT NULL, 
    closer_call_id INTEGER, 
    correction_reason VARCHAR(40) NOT NULL, 
    signal_type VARCHAR(40), 
    note TEXT, 
    dampener_active BOOLEAN DEFAULT 'true' NOT NULL, 
    queue_status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    created_by VARCHAR(120) NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_cora_overrides_source CHECK (source IN ('closer_teach', 'feedback_ritual')), 
    CONSTRAINT ck_cora_overrides_queue_status CHECK (queue_status IN ('pending', 'exported', 'discarded')), 
    FOREIGN KEY(closer_call_id) REFERENCES closer_calls (id) ON DELETE SET NULL
);

CREATE INDEX idx_cora_overrides_subject_active ON cora_training_overrides (subject_type, subject_id, dampener_active);

CREATE INDEX idx_cora_overrides_queue_status ON cora_training_overrides (queue_status);

CREATE UNIQUE INDEX uq_cora_override_active
        ON cora_training_overrides (
            subject_id,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa094_a6_cora_training_overrides")


if __name__ == "__main__":
    main()
