"""Auto-converted from alembic migration `fa027_agent_decisions_variant_id` (revision fa027_agent_decisions_variant_id).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa027_agent_decisions_variant_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE agent_decisions ADD COLUMN variant_id VARCHAR(80);

CREATE INDEX ix_agent_decisions_variant_id ON agent_decisions (variant_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa027_agent_decisions_variant_id")


if __name__ == "__main__":
    main()
