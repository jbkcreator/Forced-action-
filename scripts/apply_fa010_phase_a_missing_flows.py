"""Auto-converted from alembic migration `fa010_phase_a_missing_flows` (revision fa010_phase_a_missing_flows).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa010_phase_a_missing_flows.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN lock_candidate_zip VARCHAR(10);

ALTER TABLE subscribers ADD COLUMN lock_candidate_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN ap_lite_candidate_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN paused_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN pause_resume_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN escalation_routed_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE subscribers ADD COLUMN escalation_channel VARCHAR(20);

CREATE INDEX idx_sub_lock_candidate ON subscribers (lock_candidate_at) WHERE lock_candidate_at IS NOT NULL;

CREATE INDEX idx_sub_paused ON subscribers (paused_at) WHERE paused_at IS NOT NULL;

ALTER TABLE wallet_transactions ADD COLUMN zip_code VARCHAR(10);

CREATE INDEX idx_wallet_txn_sub_zip_created ON wallet_transactions (subscriber_id, zip_code, created_at);

CREATE TABLE manual_action_log (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    action_type VARCHAR(40) NOT NULL, 
    week_start DATE NOT NULL, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_mal_sub_week ON manual_action_log (subscriber_id, week_start);

CREATE INDEX idx_mal_created ON manual_action_log (created_at);

CREATE TABLE human_close_escalations (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    decision_id VARCHAR(40) NOT NULL, 
    revenue_signal_score INTEGER NOT NULL, 
    interactions_count INTEGER NOT NULL, 
    target_tier VARCHAR(20) NOT NULL, 
    channel VARCHAR(20) NOT NULL, 
    routed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW() NOT NULL, 
    closer_assigned VARCHAR(80), 
    outcome VARCHAR(20), 
    outcome_at TIMESTAMP WITHOUT TIME ZONE, 
    context_json JSONB, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_hce_sub_decision UNIQUE (subscriber_id, decision_id), 
    CONSTRAINT check_hce_channel CHECK (channel IN ('slack', 'ghl', 'sms', 'email')), 
    CONSTRAINT check_hce_outcome CHECK (outcome IN ('won', 'lost', 'no_response', 'rescheduled') OR outcome IS NULL), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_hce_routed ON human_close_escalations (routed_at);

CREATE INDEX idx_hce_open ON human_close_escalations (outcome, routed_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa010_phase_a_missing_flows")


if __name__ == "__main__":
    main()
