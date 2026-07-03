"""Auto-converted from alembic migration `fa071_agent_decision_override_reason_code` (revision fa071_agent_decision_override_reason_code).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa071_agent_decision_override_reason_code.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE agent_decisions ADD COLUMN override_reason_code VARCHAR(40);

ALTER TABLE agent_decisions ADD CONSTRAINT check_agent_override_reason_code CHECK (override_reason_code IS NULL OR override_reason_code IN ('factual_error', 'compliance_risk', 'wrong_audience', 'bad_timing', 'low_lead_quality', 'offer_mismatch', 'tone_or_brand_risk', 'duplicate_or_redundant', 'customer_context_missing', 'operator_strategy', 'other'));

CREATE INDEX idx_agent_decisions_override_reason_code ON agent_decisions (override_reason_code);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa071_agent_decision_override_reason_code")


if __name__ == "__main__":
    main()
