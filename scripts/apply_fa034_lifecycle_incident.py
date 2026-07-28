"""Auto-converted from alembic migration `fa034_lifecycle_incident` (revision fa034_lifecycle_incident).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa034_lifecycle_incident.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE lifecycle_incident (
    id BIGSERIAL NOT NULL, 
    metric_name VARCHAR(64) NOT NULL, 
    county_id VARCHAR(50), 
    feature_name VARCHAR(64), 
    severity VARCHAR(16) NOT NULL, 
    observed_value NUMERIC(10, 4) NOT NULL, 
    threshold_value NUMERIC(10, 4) NOT NULL, 
    baseline_value NUMERIC(10, 4), 
    breach_started TIMESTAMP WITH TIME ZONE NOT NULL, 
    breach_resolved TIMESTAMP WITH TIME ZONE, 
    duration_hours INTEGER, 
    action_taken VARCHAR(32) DEFAULT 'no_op' NOT NULL, 
    action_details JSONB, 
    decision_id UUID, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_lifecycle_incident_severity CHECK (severity IN ('yellow','red')), 
    CONSTRAINT check_lifecycle_incident_action CHECK (action_taken IN ('no_op','fallback_enabled','auto_paused','human_escalated','feature_killed','resolved'))
);

CREATE INDEX idx_lifecycle_incident_metric_open
            ON lifecycle_incident(metric_name, county_id, feature_name)
            WHERE breach_resolved IS NULL;

CREATE INDEX idx_lifecycle_incident_unresolved
            ON lifecycle_incident(severity, breach_started)
            WHERE breach_resolved IS NULL;

CREATE INDEX idx_lifecycle_incident_breach_started ON lifecycle_incident USING btree (breach_started);

ALTER TABLE platform_daily_stats ADD COLUMN sms_reply_rate NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN offer_acceptance_rate NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN first_payment_rate NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN saved_card_rate NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN wallet_adoption NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN lock_conversion NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN retention_30d NUMERIC(6, 4);

ALTER TABLE platform_daily_stats ADD COLUMN cac_paid_channels NUMERIC(10, 2);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa034_lifecycle_incident")


if __name__ == "__main__":
    main()
