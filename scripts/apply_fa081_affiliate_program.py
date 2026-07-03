"""Auto-converted from alembic migration `fa081_affiliate_program` (revision fa081_affiliate_program).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa081_affiliate_program.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE affiliates (
    id SERIAL NOT NULL, 
    ref_code VARCHAR(40) NOT NULL, 
    name VARCHAR(200) NOT NULL, 
    contact_email VARCHAR(255), 
    contact_phone VARCHAR(20), 
    commission_rate NUMERIC(5, 4) DEFAULT 0.20 NOT NULL, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_affiliate_status CHECK (status IN ('active', 'disabled'))
);

CREATE UNIQUE INDEX ix_affiliates_ref_code ON affiliates (ref_code);

CREATE TABLE affiliate_referrals (
    id SERIAL NOT NULL, 
    affiliate_id INTEGER NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    attributed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    confirmed_at TIMESTAMP WITHOUT TIME ZONE, 
    paid_tenure_start TIMESTAMP WITHOUT TIME ZONE, 
    window_end TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT check_affiliate_referral_status CHECK (status IN ('pending', 'active', 'expired')), 
    FOREIGN KEY(affiliate_id) REFERENCES affiliates (id), 
    UNIQUE (subscriber_id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX ix_affiliate_referrals_affiliate_id ON affiliate_referrals (affiliate_id);

CREATE TABLE subscription_invoices (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    stripe_invoice_id VARCHAR(255) NOT NULL, 
    stripe_payment_intent_id VARCHAR(255), 
    amount_collected_cents INTEGER NOT NULL, 
    period_month DATE NOT NULL, 
    paid_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    reversed_at TIMESTAMP WITHOUT TIME ZONE, 
    reversed_reason VARCHAR(20), 
    PRIMARY KEY (id), 
    CONSTRAINT check_subscription_invoice_reversed_reason CHECK (reversed_reason IN ('refund', 'dispute')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    UNIQUE (stripe_invoice_id)
);

CREATE INDEX ix_subscription_invoices_subscriber_id ON subscription_invoices (subscriber_id);

CREATE INDEX ix_subscription_invoices_period_month ON subscription_invoices (period_month);

CREATE INDEX ix_subscription_invoices_stripe_payment_intent_id ON subscription_invoices (stripe_payment_intent_id);

CREATE TABLE affiliate_payout_ledger (
    id SERIAL NOT NULL, 
    affiliate_id INTEGER NOT NULL, 
    affiliate_referral_id INTEGER NOT NULL, 
    period_month DATE NOT NULL, 
    line_type VARCHAR(20) NOT NULL, 
    amount_cents INTEGER NOT NULL, 
    source_invoice_id INTEGER, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_affiliate_ledger_period_line UNIQUE (affiliate_referral_id, period_month, line_type), 
    CONSTRAINT check_affiliate_ledger_line_type CHECK (line_type IN ('accrual', 'clawback')), 
    FOREIGN KEY(affiliate_id) REFERENCES affiliates (id), 
    FOREIGN KEY(affiliate_referral_id) REFERENCES affiliate_referrals (id), 
    FOREIGN KEY(source_invoice_id) REFERENCES subscription_invoices (id)
);

CREATE INDEX ix_affiliate_payout_ledger_affiliate_id ON affiliate_payout_ledger (affiliate_id);

CREATE INDEX ix_affiliate_payout_ledger_referral_id ON affiliate_payout_ledger (affiliate_referral_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa081_affiliate_program")


if __name__ == "__main__":
    main()
