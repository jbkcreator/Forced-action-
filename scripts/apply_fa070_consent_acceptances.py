"""Auto-converted from alembic migration `fa070_consent_acceptances` (revision fa070_consent_acceptances).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa070_consent_acceptances.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE consent_acceptances (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER, 
    waitlist_entry_id BIGINT, 
    phone VARCHAR(20), 
    email VARCHAR(255) NOT NULL, 
    terms_version VARCHAR(20) NOT NULL, 
    privacy_version VARCHAR(20) NOT NULL, 
    accepted_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    source_flow VARCHAR(30) DEFAULT 'waitlist' NOT NULL, 
    ip_address VARCHAR(45), 
    user_agent TEXT, 
    modal_opened_at TIMESTAMP WITH TIME ZONE, 
    modal_scrolled_to_end_at TIMESTAMP WITH TIME ZONE, 
    accepted_text_hash VARCHAR(64) NOT NULL, 
    tcpa_consent_text TEXT, 
    tcpa_consent_version VARCHAR(20), 
    tcpa_checked_at TIMESTAMP WITH TIME ZONE, 
    consent_scope VARCHAR(30), 
    not_condition_of_purchase_ack BOOLEAN, 
    county_id VARCHAR(50) DEFAULT 'hillsborough', 
    created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_consent_source_flow CHECK (source_flow IN ('waitlist','signup','checkout','county_launch','free_signup')), 
    CONSTRAINT ck_consent_scope CHECK (consent_scope IS NULL OR consent_scope IN ('marketing','waitlist_notify','lead_alerts')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    FOREIGN KEY(waitlist_entry_id) REFERENCES waitlist_entries (id)
);

CREATE INDEX ix_consent_acceptances_phone ON consent_acceptances (phone);

CREATE INDEX ix_consent_acceptances_subscriber_id ON consent_acceptances (subscriber_id);

CREATE INDEX ix_consent_acceptances_email ON consent_acceptances (email);

CREATE INDEX ix_consent_acceptances_waitlist_entry_id ON consent_acceptances (waitlist_entry_id);

CREATE INDEX idx_consent_email ON consent_acceptances (email);

CREATE INDEX idx_consent_accepted_at ON consent_acceptances (accepted_at);

CREATE INDEX idx_consent_subscriber ON consent_acceptances (subscriber_id);

CREATE INDEX idx_consent_waitlist ON consent_acceptances (waitlist_entry_id);

ALTER TABLE sms_opt_ins DROP CONSTRAINT IF EXISTS check_opt_in_source;

ALTER TABLE sms_opt_ins ADD CONSTRAINT check_opt_in_source CHECK (source IN ('double_opt_in','manual','import','widget','waitlist_form','synthflow_inbound','missed_call_inbound','consent_form'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa070_consent_acceptances")


if __name__ == "__main__":
    main()
