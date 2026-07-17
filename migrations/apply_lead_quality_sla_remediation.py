"""B1-03 — SLA auto-remediation columns + delivery rejection reasons.

src/tasks/lead_quality_monitor.py now writes remediation outcomes onto
lead_quality_snapshots (delivery_id, remediation_action, remediated_at) and
reject_delivery() can set rejection_reason to 'sold_before_delivery' or
'signals_resolved' — neither existed in the production schema when this PR
was written. Without this migration, the monitor's snapshot insert fails
for entitlement-model (Delivery-sourced) rows, and reject_delivery() trips
the deliveries table's ck_delivery_reason check.

Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_lead_quality_sla_remediation.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE lead_quality_snapshots ADD COLUMN IF NOT EXISTS delivery_id BIGINT REFERENCES deliveries(id);",
    "ALTER TABLE lead_quality_snapshots ADD COLUMN IF NOT EXISTS remediation_action VARCHAR(30);",
    "ALTER TABLE lead_quality_snapshots ADD COLUMN IF NOT EXISTS remediated_at TIMESTAMPTZ;",
    "CREATE INDEX IF NOT EXISTS ix_lead_quality_snapshots_delivery_id ON lead_quality_snapshots(delivery_id);",
    "ALTER TABLE lead_quality_snapshots DROP CONSTRAINT IF EXISTS ck_lqs_remediation_action;",
    """
    ALTER TABLE lead_quality_snapshots ADD CONSTRAINT ck_lqs_remediation_action
        CHECK (remediation_action IS NULL OR remediation_action IN
            ('credit_issued', 'refund_issued', 'refund_failed', 'not_applicable'));
    """,
    "ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS ck_delivery_reason;",
    """
    ALTER TABLE deliveries ADD CONSTRAINT ck_delivery_reason
        CHECK (rejection_reason IS NULL OR rejection_reason IN
            ('disconnected', 'wrong_party', 'deceased', 'duplicate', 'other',
             'sold_before_delivery', 'signals_resolved'));
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("lead_quality_sla_remediation migration complete.")


if __name__ == "__main__":
    main()
