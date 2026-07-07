"""CDE-11 — Outcome Confidence Tier on deal_outcomes.

Makes subscriber_id nullable (founder-import and public-record-inferred outcomes
have no subscriber — see ADR 0025) and adds:
  - confidence_tier  (founder_verified > subscriber_reported > public_record_inferred)
  - outcome_source   (free text; finer provenance, no CHECK so new connectors
                      need no DDL)
Existing rows backfill to subscriber_reported / subscriber_tap.

Idempotent. Usage:
    PYTHONPATH=. python scripts/apply_cde11_outcome_confidence.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE deal_outcomes ALTER COLUMN subscriber_id DROP NOT NULL;

ALTER TABLE deal_outcomes
    ADD COLUMN IF NOT EXISTS confidence_tier TEXT NOT NULL DEFAULT 'subscriber_reported';

ALTER TABLE deal_outcomes
    ADD COLUMN IF NOT EXISTS outcome_source TEXT;

UPDATE deal_outcomes SET outcome_source = 'subscriber_tap' WHERE outcome_source IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_deal_outcomes_confidence_tier'
    ) THEN
        ALTER TABLE deal_outcomes ADD CONSTRAINT ck_deal_outcomes_confidence_tier
            CHECK (confidence_tier IN
                ('founder_verified', 'subscriber_reported', 'public_record_inferred'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_deal_outcomes_confidence_tier
    ON deal_outcomes (confidence_tier);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied cde11_outcome_confidence")


if __name__ == "__main__":
    main()
