"""Auto-converted from alembic migration `fa081_quora_questions` (revision fa081_quora_questions).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa081_quora_questions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE quora_questions (
    id BIGSERIAL NOT NULL, 
    qid BIGINT, 
    slug TEXT, 
    url TEXT NOT NULL, 
    title TEXT NOT NULL, 
    answer_count INTEGER, 
    follower_count INTEGER, 
    view_count INTEGER, 
    is_locked BOOLEAN DEFAULT 'false' NOT NULL, 
    is_sensitive BOOLEAN DEFAULT 'false' NOT NULL, 
    topics TEXT[], 
    created_time TIMESTAMP WITH TIME ZONE, 
    deterministic_score INTEGER, 
    deterministic_reasons TEXT[], 
    matched_keyword TEXT, 
    lifecycle_decision_id VARCHAR(36), 
    intent_lane VARCHAR(60), 
    recommended_action VARCHAR(40), 
    priority_score INTEGER, 
    risk_level VARCHAR(20), 
    lifecycle_classification JSONB, 
    answer_draft JSONB, 
    answer_status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    published_at TIMESTAMP WITH TIME ZONE, 
    quora_answer_id TEXT, 
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    last_classified_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_quora_answer_status CHECK (answer_status IN ('pending','drafted','skipped','published','failed'))
);

CREATE UNIQUE INDEX idx_quora_questions_qid ON quora_questions (qid);

CREATE INDEX idx_quora_questions_keyword ON quora_questions (matched_keyword);

CREATE INDEX idx_quora_questions_recommended_action ON quora_questions (recommended_action);

CREATE INDEX idx_quora_questions_answer_status ON quora_questions (answer_status);

CREATE INDEX idx_quora_questions_action_priority ON quora_questions (recommended_action, priority_score);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa081_quora_questions")


if __name__ == "__main__":
    main()
