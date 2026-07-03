"""Auto-converted from alembic migration `fa089_prospect_merge_events` (revision fa089_prospect_merge_events).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa089_prospect_merge_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE prospects ADD COLUMN merged_into_id UUID;

ALTER TABLE prospects ADD CONSTRAINT fk_prospects_merged_into_id FOREIGN KEY(merged_into_id) REFERENCES prospects (prospect_id) ON DELETE SET NULL;

CREATE INDEX idx_prospects_merged_into ON prospects (merged_into_id) WHERE merged_into_id IS NOT NULL;

CREATE TABLE merge_events (
    merge_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    surviving_id UUID NOT NULL, 
    merged_id UUID NOT NULL, 
    field_decisions JSONB DEFAULT '{}'::jsonb NOT NULL, 
    merged_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (merge_id), 
    CONSTRAINT fk_merge_events_surviving_id FOREIGN KEY(surviving_id) REFERENCES prospects (prospect_id), 
    CONSTRAINT fk_merge_events_merged_id FOREIGN KEY(merged_id) REFERENCES prospects (prospect_id)
);

CREATE INDEX idx_merge_events_surviving_id ON merge_events (surviving_id);

CREATE INDEX idx_merge_events_merged_id ON merge_events (merged_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa089_prospect_merge_events")


if __name__ == "__main__":
    main()
