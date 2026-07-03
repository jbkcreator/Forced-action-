"""Backfilled from alembic migration `fa063_campaign_instantly_settings` (revision fa063).

Adds email_campaigns.instantly_settings JSONB. Idempotent. Live DB already has
it; kept so every schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_fa063_campaign_instantly_settings.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE email_campaigns
    ADD COLUMN IF NOT EXISTS instantly_settings JSONB NOT NULL DEFAULT '{}'::jsonb;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa063_campaign_instantly_settings")


if __name__ == "__main__":
    main()
