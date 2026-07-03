"""Auto-converted from alembic migration `fa021_rename_manager_to_registered_agent` (revision fa021).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa021_rename_manager_to_registered_agent.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE owners RENAME manager_name TO registered_agent_name;

ALTER TABLE owners ALTER COLUMN manager_title TYPE VARCHAR(500);

ALTER TABLE owners RENAME manager_title TO registered_agent_address;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa021_rename_manager_to_registered_agent")


if __name__ == "__main__":
    main()
