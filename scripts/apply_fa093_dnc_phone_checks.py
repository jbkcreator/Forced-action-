"""Auto-converted from alembic migration `fa093_dnc_phone_checks` (revision fa093_dnc_phone_checks).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa093_dnc_phone_checks.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE dnc_phone_checks (
    phone VARCHAR(20) NOT NULL, 
    national_dnc BOOLEAN DEFAULT false NOT NULL, 
    litigator BOOLEAN DEFAULT false NOT NULL, 
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    source VARCHAR(40) DEFAULT 'tracerfy_dnc_refresh' NOT NULL, 
    raw_result JSONB, 
    CONSTRAINT pk_dnc_phone_checks PRIMARY KEY (phone)
);

CREATE INDEX idx_dnc_phone_checks_checked_at ON dnc_phone_checks (checked_at);

CREATE INDEX idx_dnc_phone_checks_clean_fresh ON dnc_phone_checks (checked_at) WHERE national_dnc = false AND litigator = false;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa093_dnc_phone_checks")


if __name__ == "__main__":
    main()
