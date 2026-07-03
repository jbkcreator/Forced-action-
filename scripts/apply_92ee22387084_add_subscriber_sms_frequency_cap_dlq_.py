"""Auto-converted from alembic migration `92ee22387084_add_subscriber_sms_frequency_cap_dlq_` (revision 92ee22387084).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_92ee22387084_add_subscriber_sms_frequency_cap_dlq_.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sms_dead_letters DROP CONSTRAINT check_dlq_reason;

ALTER TABLE sms_dead_letters ADD CONSTRAINT check_dlq_reason CHECK (reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 92ee22387084_add_subscriber_sms_frequency_cap_dlq_")


if __name__ == "__main__":
    main()
