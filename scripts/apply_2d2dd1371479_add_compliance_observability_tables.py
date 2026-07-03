"""Auto-converted from alembic migration `2d2dd1371479_add_compliance_observability_tables` (revision 2d2dd1371479).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_2d2dd1371479_add_compliance_observability_tables.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE sms_dead_letters (
    id SERIAL NOT NULL, 
    phone VARCHAR(20), 
    reason VARCHAR(50) NOT NULL, 
    payload JSONB, 
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    reviewed_at TIMESTAMP WITHOUT TIME ZONE, 
    reviewed_by VARCHAR(100), 
    PRIMARY KEY (id), 
    CONSTRAINT check_dlq_reason CHECK (reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable'))
);

CREATE INDEX idx_dlq_reviewed ON sms_dead_letters (reviewed_at);

CREATE INDEX ix_sms_dead_letters_phone ON sms_dead_letters (phone);

CREATE TABLE sms_opt_outs (
    id SERIAL NOT NULL, 
    phone VARCHAR(20) NOT NULL, 
    keyword_used VARCHAR(20), 
    source VARCHAR(30) NOT NULL, 
    opted_out_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX ix_sms_opt_outs_phone ON sms_opt_outs (phone);

CREATE TABLE api_usage_logs (
    id SERIAL NOT NULL, 
    service VARCHAR(20) NOT NULL, 
    model VARCHAR(60), 
    input_tokens INTEGER, 
    output_tokens INTEGER, 
    cost_usd NUMERIC(10, 6), 
    task_type VARCHAR(60), 
    subscriber_id INTEGER, 
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_api_service CHECK (service IN ('claude', 'twilio', 'stripe')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_api_usage_service_created ON api_usage_logs (service, created_at);

CREATE INDEX idx_api_usage_task_created ON api_usage_logs (task_type, created_at);

CREATE INDEX ix_api_usage_logs_subscriber_id ON api_usage_logs (subscriber_id);

ALTER TABLE ab_assignments ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_ab_assignment_subscriber;

DROP INDEX idx_ab_assignment_test;

CREATE INDEX ix_ab_assignments_subscriber_id ON ab_assignments (subscriber_id);

CREATE INDEX ix_ab_assignments_test_id ON ab_assignments (test_id);

ALTER TABLE ab_tests ALTER COLUMN traffic_pct DROP DEFAULT;

ALTER TABLE ab_tests ALTER COLUMN status DROP DEFAULT;

ALTER TABLE ab_tests ALTER COLUMN started_at DROP DEFAULT;

ALTER TABLE deal_outcomes ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_deal_outcome_property;

DROP INDEX idx_deal_outcome_subscriber;

CREATE INDEX ix_deal_outcomes_property_id ON deal_outcomes (property_id);

CREATE INDEX ix_deal_outcomes_subscriber_id ON deal_outcomes (subscriber_id);

ALTER TABLE learning_cards ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_learning_card_date;

CREATE INDEX ix_learning_cards_card_date ON learning_cards (card_date);

ALTER TABLE message_outcomes ALTER COLUMN sent_at DROP DEFAULT;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_4h SET NOT NULL;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_4h DROP DEFAULT;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_24h SET NOT NULL;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_24h DROP DEFAULT;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_48h SET NOT NULL;

ALTER TABLE message_outcomes ALTER COLUMN conversion_within_48h DROP DEFAULT;

ALTER TABLE message_outcomes ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_msg_outcome_subscriber;

DROP INDEX idx_msg_outcome_variant;

CREATE INDEX ix_message_outcomes_subscriber_id ON message_outcomes (subscriber_id);

CREATE INDEX ix_message_outcomes_variant_id ON message_outcomes (variant_id);

ALTER TABLE referral_events ALTER COLUMN status DROP DEFAULT;

ALTER TABLE referral_events ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_referral_code;

DROP INDEX idx_referral_referee;

DROP INDEX idx_referral_referrer;

CREATE INDEX ix_referral_events_referee_subscriber_id ON referral_events (referee_subscriber_id);

CREATE INDEX ix_referral_events_referral_code ON referral_events (referral_code);

CREATE INDEX ix_referral_events_referrer_subscriber_id ON referral_events (referrer_subscriber_id);

ALTER TABLE subscribers ALTER COLUMN has_saved_card DROP DEFAULT;

ALTER TABLE subscribers ALTER COLUMN auto_mode_enabled DROP DEFAULT;

DROP INDEX idx_subscriber_referral_code;

DROP INDEX uq_subscriber_email_vertical_active;

ALTER TABLE subscribers DROP CONSTRAINT uq_subscriber_referral_code;

CREATE UNIQUE INDEX ix_subscribers_referral_code ON subscribers (referral_code);

ALTER TABLE user_segments ALTER COLUMN revenue_signal_score DROP DEFAULT;

ALTER TABLE user_segments ALTER COLUMN last_classified_at DROP DEFAULT;

ALTER TABLE user_segments ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE user_segments ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_user_segment_subscriber;

ALTER TABLE user_segments DROP CONSTRAINT user_segments_subscriber_id_key;

CREATE UNIQUE INDEX ix_user_segments_subscriber_id ON user_segments (subscriber_id);

ALTER TABLE wallet_balances ALTER COLUMN credits_remaining DROP DEFAULT;

ALTER TABLE wallet_balances ALTER COLUMN credits_used_total DROP DEFAULT;

ALTER TABLE wallet_balances ALTER COLUMN auto_reload_enabled DROP DEFAULT;

ALTER TABLE wallet_balances ALTER COLUMN created_at DROP DEFAULT;

ALTER TABLE wallet_balances ALTER COLUMN updated_at DROP DEFAULT;

DROP INDEX idx_wallet_balance_subscriber;

ALTER TABLE wallet_balances DROP CONSTRAINT wallet_balances_subscriber_id_key;

CREATE UNIQUE INDEX ix_wallet_balances_subscriber_id ON wallet_balances (subscriber_id);

ALTER TABLE wallet_transactions ALTER COLUMN created_at DROP DEFAULT;

DROP INDEX idx_wallet_txn_stripe_charge;

DROP INDEX idx_wallet_txn_subscriber;

DROP INDEX idx_wallet_txn_wallet;

CREATE INDEX ix_wallet_transactions_stripe_charge_id ON wallet_transactions (stripe_charge_id);

CREATE INDEX ix_wallet_transactions_subscriber_id ON wallet_transactions (subscriber_id);

CREATE INDEX ix_wallet_transactions_wallet_id ON wallet_transactions (wallet_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 2d2dd1371479_add_compliance_observability_tables")


if __name__ == "__main__":
    main()
