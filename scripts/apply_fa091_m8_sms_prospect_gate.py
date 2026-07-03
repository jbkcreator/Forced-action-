"""Auto-converted from alembic migration `fa091_m8_sms_prospect_gate` (revision fa091_m8_sms_prospect_gate).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa091_m8_sms_prospect_gate.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sms_send_logs ADD COLUMN prospect_id UUID;

ALTER TABLE sms_send_logs ADD FOREIGN KEY(prospect_id) REFERENCES prospects (prospect_id) ON DELETE SET NULL;

CREATE INDEX idx_sms_send_logs_prospect_id ON sms_send_logs (prospect_id);

ALTER TABLE sms_dead_letters DROP CONSTRAINT check_dlq_reason;

ALTER TABLE sms_dead_letters ADD CONSTRAINT check_dlq_reason CHECK (reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap', 'do_not_text_tag', 'prospect_not_contactable', 'prospect_sms_consent_withdrawn'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa091_m8_sms_prospect_gate")


if __name__ == "__main__":
    main()
