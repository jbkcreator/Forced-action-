"""Auto-converted from alembic migration `fa039_remove_sandbox_outbox` (revision fa039_remove_sandbox_outbox).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa039_remove_sandbox_outbox.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP INDEX idx_sandbox_outbox_campaign_created;

DROP INDEX idx_sandbox_outbox_sub_created;

DROP INDEX ix_sandbox_outbox_created_at;

DROP INDEX ix_sandbox_outbox_decision_id;

DROP INDEX ix_sandbox_outbox_campaign;

DROP INDEX ix_sandbox_outbox_subscriber_id;

DROP TABLE sandbox_outbox;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa039_remove_sandbox_outbox")


if __name__ == "__main__":
    main()
