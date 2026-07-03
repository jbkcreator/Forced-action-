"""Auto-converted from alembic migration `y9z0a1b2c3d4_add_raw_response_to_enriched_contacts` (revision y9z0a1b2c3d4_add_raw_response).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_y9z0a1b2c3d4_add_raw_response_to_enriched_contacts.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE enriched_contacts ADD COLUMN raw_response JSONB;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied y9z0a1b2c3d4_add_raw_response_to_enriched_contacts")


if __name__ == "__main__":
    main()
