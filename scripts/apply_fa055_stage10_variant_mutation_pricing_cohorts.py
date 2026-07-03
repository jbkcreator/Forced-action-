"""Auto-converted from alembic migration `fa055_stage10_variant_mutation_pricing_cohorts` (revision fa055).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa055_stage10_variant_mutation_pricing_cohorts.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE message_variant_tests (
    id SERIAL NOT NULL, 
    sequence_name VARCHAR(100) NOT NULL, 
    segment VARCHAR(50), 
    traffic_pct INTEGER DEFAULT '10' NOT NULL, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    slot_a_body TEXT NOT NULL, 
    slot_a_sends INTEGER DEFAULT '0' NOT NULL, 
    slot_a_conversions INTEGER DEFAULT '0' NOT NULL, 
    slot_a_replies INTEGER DEFAULT '0' NOT NULL, 
    slot_a_status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    slot_a_retired_at TIMESTAMP WITH TIME ZONE, 
    slot_b_body TEXT NOT NULL, 
    slot_b_sends INTEGER DEFAULT '0' NOT NULL, 
    slot_b_conversions INTEGER DEFAULT '0' NOT NULL, 
    slot_b_replies INTEGER DEFAULT '0' NOT NULL, 
    slot_b_status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    slot_b_retired_at TIMESTAMP WITH TIME ZONE, 
    slot_c_body TEXT NOT NULL, 
    slot_c_sends INTEGER DEFAULT '0' NOT NULL, 
    slot_c_conversions INTEGER DEFAULT '0' NOT NULL, 
    slot_c_replies INTEGER DEFAULT '0' NOT NULL, 
    slot_c_status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    slot_c_retired_at TIMESTAMP WITH TIME ZONE, 
    proving_slot VARCHAR(5), 
    proving_baseline_conv_rate NUMERIC(8, 6), 
    proving_started_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_mvt_status CHECK (status IN ('active','paused','completed')), 
    CONSTRAINT check_mvt_traffic_cap CHECK (traffic_pct BETWEEN 1 AND 10), 
    CONSTRAINT check_mvt_slot_statuses CHECK (slot_a_status IN ('active','retired') AND slot_b_status IN ('active','retired') AND slot_c_status IN ('active','retired')), 
    CONSTRAINT check_mvt_proving_slot CHECK (proving_slot IS NULL OR proving_slot IN ('a','b','c')), 
    UNIQUE (sequence_name)
);

CREATE INDEX idx_mvt_status ON message_variant_tests (status);

CREATE INDEX idx_mvt_sequence_name ON message_variant_tests (sequence_name);

CREATE TABLE variant_retirement_log (
    id SERIAL NOT NULL, 
    test_id INTEGER NOT NULL, 
    action VARCHAR(30) NOT NULL, 
    slot VARCHAR(5) NOT NULL, 
    old_body TEXT, 
    new_body TEXT, 
    old_conversion_rate NUMERIC(8, 6), 
    new_conversion_rate NUMERIC(8, 6), 
    reason TEXT, 
    idempotency_key VARCHAR(120) NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_vrl_action CHECK (action IN ('retired','replaced','reverted','promoted','rollback')), 
    CONSTRAINT check_vrl_slot CHECK (slot IN ('a','b','c')), 
    FOREIGN KEY(test_id) REFERENCES message_variant_tests (id) ON DELETE CASCADE, 
    UNIQUE (idempotency_key)
);

CREATE INDEX idx_vrl_test_id ON variant_retirement_log (test_id);

CREATE INDEX idx_vrl_idempotency_key ON variant_retirement_log (idempotency_key);

CREATE TABLE pricing_cohorts (
    id SERIAL NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    trade_vertical VARCHAR(50) NOT NULL, 
    price_type VARCHAR(30) NOT NULL, 
    base_price_cents INTEGER NOT NULL, 
    adjusted_price_cents INTEGER NOT NULL, 
    adjustment_pct NUMERIC(6, 2) NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    activation_reason TEXT, 
    rollback_reason TEXT, 
    deal_weeks INTEGER, 
    deal_count INTEGER, 
    activated_at TIMESTAMP WITH TIME ZONE, 
    rolled_back_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_pc_status CHECK (status IN ('pending','active','rolled_back')), 
    CONSTRAINT check_pc_adjustment_bounds CHECK (adjustment_pct BETWEEN -25 AND 25)
);

CREATE INDEX idx_pc_county_vertical_type ON pricing_cohorts (county_id, trade_vertical, price_type);

CREATE INDEX idx_pc_status ON pricing_cohorts (status);

CREATE UNIQUE INDEX idx_pc_active_unique
            ON pricing_cohorts(county_id, trade_vertical, price_type)
            WHERE status = 'active';
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa055_stage10_variant_mutation_pricing_cohorts")


if __name__ == "__main__":
    main()
