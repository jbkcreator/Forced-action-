"""Auto-converted from alembic migration `m3n4o5p6q7r8_2b_schema_foundation` (revision m3n4o5p6q7r8).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_m3n4o5p6q7r8_2b_schema_foundation.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers DROP CONSTRAINT check_subscriber_tier;

ALTER TABLE subscribers DROP CONSTRAINT check_subscriber_status;

ALTER TABLE subscribers ADD COLUMN has_saved_card BOOLEAN DEFAULT 'false' NOT NULL;

ALTER TABLE subscribers ADD COLUMN stripe_payment_method_id VARCHAR(100);

ALTER TABLE subscribers ADD COLUMN referral_code VARCHAR(20);

ALTER TABLE subscribers ADD COLUMN auto_mode_enabled BOOLEAN DEFAULT 'false' NOT NULL;

CREATE UNIQUE INDEX idx_subscriber_referral_code ON subscribers (referral_code) WHERE referral_code IS NOT NULL;

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_tier CHECK (tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner'));

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_status CHECK (status IN ('active', 'grace', 'churned', 'cancelled', 'paused'));

CREATE TABLE wallet_balances (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    wallet_tier VARCHAR(20) NOT NULL, 
    credits_remaining INTEGER DEFAULT '0' NOT NULL, 
    credits_used_total INTEGER DEFAULT '0' NOT NULL, 
    auto_reload_enabled BOOLEAN DEFAULT 'true' NOT NULL, 
    last_reload_at TIMESTAMP WITHOUT TIME ZONE, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_wallet_tier CHECK (wallet_tier IN ('starter_wallet', 'growth', 'power')), 
    CONSTRAINT uq_wallet_balance_subscriber UNIQUE (subscriber_id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_wallet_balance_subscriber ON wallet_balances (subscriber_id);

CREATE TABLE wallet_transactions (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    wallet_id INTEGER NOT NULL, 
    txn_type VARCHAR(20) NOT NULL, 
    amount INTEGER NOT NULL, 
    balance_after INTEGER NOT NULL, 
    description VARCHAR(255), 
    stripe_charge_id VARCHAR(100), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_wallet_txn_type CHECK (txn_type IN ('credit', 'debit', 'reload', 'bonus', 'refund')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(wallet_id) REFERENCES wallet_balances (id)
);

CREATE INDEX idx_wallet_txn_subscriber ON wallet_transactions (subscriber_id);

CREATE INDEX idx_wallet_txn_wallet ON wallet_transactions (wallet_id);

CREATE INDEX idx_wallet_txn_charge ON wallet_transactions (stripe_charge_id);

CREATE INDEX idx_wallet_txn_sub_created ON wallet_transactions (subscriber_id, created_at);

CREATE TABLE user_segments (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    segment VARCHAR(30) NOT NULL, 
    revenue_signal_score INTEGER DEFAULT '0', 
    last_classified_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    classification_reason VARCHAR(255), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_user_segment CHECK (segment IN ('new', 'browsing', 'engaged', 'wallet_active', 'high_intent', 'lock_candidate', 'at_risk', 'churned')), 
    CONSTRAINT uq_user_segment_subscriber UNIQUE (subscriber_id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_user_segment_subscriber ON user_segments (subscriber_id);

CREATE TABLE message_outcomes (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER, 
    message_type VARCHAR(20) NOT NULL, 
    template_id VARCHAR(100), 
    variant_id VARCHAR(100), 
    channel VARCHAR(50), 
    sent_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    delivered_at TIMESTAMP WITHOUT TIME ZONE, 
    opened_at TIMESTAMP WITHOUT TIME ZONE, 
    clicked_at TIMESTAMP WITHOUT TIME ZONE, 
    replied_at TIMESTAMP WITHOUT TIME ZONE, 
    conversion_type VARCHAR(30), 
    conversion_within_4h BOOLEAN DEFAULT 'false', 
    conversion_within_24h BOOLEAN DEFAULT 'false', 
    conversion_within_48h BOOLEAN DEFAULT 'false', 
    revenue_attributed NUMERIC(10, 2), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_message_type CHECK (message_type IN ('sms', 'email', 'voice')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_message_outcome_subscriber ON message_outcomes (subscriber_id);

CREATE INDEX idx_message_outcome_sub_sent ON message_outcomes (subscriber_id, sent_at);

CREATE INDEX idx_message_outcome_variant ON message_outcomes (variant_id);

CREATE TABLE deal_outcomes (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    property_id INTEGER, 
    deal_size_bucket VARCHAR(20), 
    deal_amount NUMERIC(12, 2), 
    deal_date DATE, 
    lead_source VARCHAR(50), 
    days_to_close INTEGER, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_deal_size_bucket CHECK (deal_size_bucket IN ('5_10k', '10_25k', '25k_plus', 'skip')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX idx_deal_outcome_subscriber ON deal_outcomes (subscriber_id);

CREATE INDEX idx_deal_outcome_property ON deal_outcomes (property_id);

CREATE INDEX idx_deal_outcome_sub_date ON deal_outcomes (subscriber_id, deal_date);

CREATE TABLE learning_cards (
    id SERIAL NOT NULL, 
    card_date DATE NOT NULL, 
    card_type VARCHAR(30) NOT NULL, 
    summary_text TEXT NOT NULL, 
    data_json JSONB, 
    action_taken VARCHAR(255), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_learning_card_type CHECK (card_type IN ('message_perf', 'deal_pattern', 'ab_result', 'churn_signal', 'pricing_test', 'general')), 
    CONSTRAINT uq_learning_card_date_type UNIQUE (card_date, card_type)
);

CREATE INDEX idx_learning_card_date ON learning_cards (card_date);

CREATE TABLE referral_events (
    id SERIAL NOT NULL, 
    referrer_subscriber_id INTEGER NOT NULL, 
    referee_subscriber_id INTEGER, 
    referral_code VARCHAR(20) NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    reward_type VARCHAR(30), 
    reward_value VARCHAR(50), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    confirmed_at TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT check_referral_status CHECK (status IN ('pending', 'confirmed', 'rewarded', 'expired')), 
    FOREIGN KEY(referrer_subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(referee_subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_referral_referrer ON referral_events (referrer_subscriber_id);

CREATE INDEX idx_referral_referee ON referral_events (referee_subscriber_id);

CREATE INDEX idx_referral_code ON referral_events (referral_code);

CREATE INDEX idx_referral_referrer_status ON referral_events (referrer_subscriber_id, status);

CREATE TABLE ab_tests (
    id SERIAL NOT NULL, 
    test_name VARCHAR(100) NOT NULL, 
    segment VARCHAR(30), 
    variant_a JSONB NOT NULL, 
    variant_b JSONB NOT NULL, 
    traffic_pct INTEGER DEFAULT '10' NOT NULL, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    started_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    ended_at TIMESTAMP WITHOUT TIME ZONE, 
    winner VARCHAR(10), 
    PRIMARY KEY (id), 
    CONSTRAINT check_ab_test_status CHECK (status IN ('active', 'completed', 'rolled_back')), 
    CONSTRAINT check_ab_traffic_pct CHECK (traffic_pct BETWEEN 1 AND 100), 
    CONSTRAINT uq_ab_test_name UNIQUE (test_name)
);

CREATE TABLE ab_assignments (
    id SERIAL NOT NULL, 
    test_id INTEGER NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    variant VARCHAR(10) NOT NULL, 
    outcome VARCHAR(30), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_ab_assignment UNIQUE (test_id, subscriber_id), 
    FOREIGN KEY(test_id) REFERENCES ab_tests (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_ab_assignment_test ON ab_assignments (test_id);

CREATE INDEX idx_ab_assignment_subscriber ON ab_assignments (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied m3n4o5p6q7r8_2b_schema_foundation")


if __name__ == "__main__":
    main()
