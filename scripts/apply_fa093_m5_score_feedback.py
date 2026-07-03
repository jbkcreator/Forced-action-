"""Auto-converted from alembic migration `fa093_m5_score_feedback` (revision fa093_m5_score_feedback).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa093_m5_score_feedback.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE score_feedback (
    score_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    prospect_id UUID NOT NULL, 
    closer_call_id BIGINT, 
    predicted_tier VARCHAR NOT NULL, 
    predicted_rate NUMERIC(6, 4), 
    realized_outcome VARCHAR, 
    delta NUMERIC(8, 4), 
    scored_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    resolved_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (score_id), 
    CONSTRAINT uq_score_feedback_prospect_id UNIQUE (prospect_id), 
    CONSTRAINT ck_score_feedback_predicted_tier CHECK (predicted_tier IN ('Bronze','Silver','Gold','Platinum','Ultra','sub_grade')), 
    CONSTRAINT ck_score_feedback_realized_outcome CHECK (realized_outcome IS NULL OR realized_outcome IN ('contacted','converted','funded','dead')), 
    FOREIGN KEY(prospect_id) REFERENCES prospects (prospect_id) ON DELETE RESTRICT
);

CREATE INDEX idx_score_feedback_prospect_id ON score_feedback (prospect_id);

CREATE INDEX idx_score_feedback_predicted_tier ON score_feedback (predicted_tier);

CREATE INDEX idx_score_feedback_closer_call_id ON score_feedback (closer_call_id) WHERE closer_call_id IS NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa093_m5_score_feedback")


if __name__ == "__main__":
    main()
