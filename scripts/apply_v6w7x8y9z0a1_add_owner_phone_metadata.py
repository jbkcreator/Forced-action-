"""Auto-converted from alembic migration `v6w7x8y9z0a1_add_owner_phone_metadata` (revision fa003_phone_metadata).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_v6w7x8y9z0a1_add_owner_phone_metadata.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE owners ADD COLUMN phone_metadata JSONB;

CREATE INDEX idx_owner_phone_metadata ON owners USING gin (phone_metadata);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied v6w7x8y9z0a1_add_owner_phone_metadata")


if __name__ == "__main__":
    main()
