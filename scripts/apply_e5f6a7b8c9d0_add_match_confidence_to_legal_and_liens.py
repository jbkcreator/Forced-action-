"""Auto-converted from alembic migration `e5f6a7b8c9d0_add_match_confidence_to_legal_and_liens` (revision e5f6a7b8c9d0).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_e5f6a7b8c9d0_add_match_confidence_to_legal_and_liens.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE legal_and_liens ADD COLUMN match_confidence INTEGER;

COMMENT ON COLUMN legal_and_liens.match_confidence IS 'Rapidfuzz score 0-100 at time of property match';

ALTER TABLE legal_and_liens ADD COLUMN match_method VARCHAR(30);

COMMENT ON COLUMN legal_and_liens.match_method IS 'How the property was matched: legal_desc | owner_name | llm_verified | address | manual';

CREATE INDEX idx_legal_match_method ON legal_and_liens (match_method);

ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_match_method CHECK (match_method IN ('legal_desc', 'owner_name', 'llm_verified', 'address', 'manual'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied e5f6a7b8c9d0_add_match_confidence_to_legal_and_liens")


if __name__ == "__main__":
    main()
