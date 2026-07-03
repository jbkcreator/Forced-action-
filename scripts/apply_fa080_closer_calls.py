"""Auto-converted from alembic migration `fa080_closer_calls` (revision fa080_closer_calls).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa080_closer_calls.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE closer_calls (
    id BIGSERIAL NOT NULL, 
    aircall_call_id VARCHAR(40) NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    escalation_id INTEGER, 
    closer_aircall_user_id VARCHAR(40), 
    closer_name VARCHAR(120), 
    direction VARCHAR(12), 
    dialed_e164 VARCHAR(20), 
    duration_sec INTEGER, 
    started_at TIMESTAMP WITH TIME ZONE, 
    ended_at TIMESTAMP WITH TIME ZONE, 
    transcript_text TEXT, 
    transcript_fetched_at TIMESTAMP WITH TIME ZONE, 
    sentiment VARCHAR(12), 
    topics JSONB, 
    objections JSONB, 
    objection_resolved VARCHAR(12), 
    call_outcome VARCHAR(30), 
    follow_ups JSONB, 
    tagged_at TIMESTAMP WITH TIME ZONE, 
    objection_type VARCHAR(40), 
    pitch_variant VARCHAR(40), 
    lead_quality_rating INTEGER, 
    feedback_by VARCHAR(120), 
    feedback_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_closer_calls_lead_quality CHECK (lead_quality_rating IS NULL OR (lead_quality_rating BETWEEN 1 AND 5)), 
    CONSTRAINT ck_closer_calls_outcome CHECK (call_outcome IS NULL OR call_outcome IN ('committed','callback_scheduled','undecided','declined','no_meaningful_conversation')), 
    CONSTRAINT ck_closer_calls_obj_resolved CHECK (objection_resolved IS NULL OR objection_resolved IN ('resolved','unresolved','none')), 
    CONSTRAINT ck_closer_calls_sentiment CHECK (sentiment IS NULL OR sentiment IN ('positive','neutral','negative','mixed')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(escalation_id) REFERENCES human_close_escalations (id)
);

ALTER TABLE closer_calls ADD CONSTRAINT uq_closer_calls_aircall_id UNIQUE (aircall_call_id);

CREATE INDEX idx_closer_calls_subscriber ON closer_calls (subscriber_id);

CREATE INDEX idx_closer_calls_closer_started ON closer_calls (closer_aircall_user_id, started_at);

CREATE INDEX idx_closer_calls_tagged_at ON closer_calls (tagged_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa080_closer_calls")


if __name__ == "__main__":
    main()
