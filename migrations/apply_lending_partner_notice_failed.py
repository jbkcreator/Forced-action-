"""Partner Door (P-1): failed-notice stamps.

``lending.loan_files.notice_failed_at`` and ``lending.partner_submissions.confirm_failed_at`` record that a
partner notice was still unsent when its retry window (``NOTICE_RETRY_HOURS``) ran out, and that Slack was
alerted once (``partner_pipeline.flag_lapsed_notices``). Models: ``LendingLoanFile``,
``LendingPartnerSubmission`` in ``src/lending/models.py``. Idempotent; run after
``apply_lending_partner_door.py``.

DEPLOY ORDER: run this BEFORE deploying the code that uses it. ``partners.record_status_change`` writes
``notice_failed_at`` on every status change, so new code on an old schema stops every routing status update.

Usage:
    PYTHONPATH=. python migrations/apply_lending_partner_notice_failed.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    for ddl in (
        f'ALTER TABLE "{schema}".loan_files ADD COLUMN IF NOT EXISTS notice_failed_at timestamptz',
        f'ALTER TABLE "{schema}".partner_submissions ADD COLUMN IF NOT EXISTS confirm_failed_at timestamptz',
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_partner_notice_failed complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
