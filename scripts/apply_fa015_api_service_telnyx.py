"""Auto-converted from alembic migration `fa015_api_service_telnyx` (revision fa015_api_telnyx).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa015_api_service_telnyx.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE api_usage_logs DROP CONSTRAINT check_api_service;

ALTER TABLE api_usage_logs ADD CONSTRAINT check_api_service CHECK (service IN ('claude', 'telnyx', 'stripe', 'twilio'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa015_api_service_telnyx")


if __name__ == "__main__":
    main()
