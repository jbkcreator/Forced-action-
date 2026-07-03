"""Auto-converted from alembic migration `fa040_waitlist_entries` (revision fa040_waitlist_entries).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa040_waitlist_entries.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE waitlist_entries (
    id BIGSERIAL NOT NULL, 
    zip_code VARCHAR(10) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    name VARCHAR(120) NOT NULL, 
    email VARCHAR(255) NOT NULL, 
    phone_e164 VARCHAR(20), 
    sms_opt_in BOOLEAN DEFAULT false NOT NULL, 
    waitlist_type VARCHAR(20) DEFAULT 'sold_out' NOT NULL, 
    signup_ip VARCHAR(45), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    notified_email_at TIMESTAMP WITH TIME ZONE, 
    notified_sms_at TIMESTAMP WITH TIME ZONE, 
    reactivation_decision_id VARCHAR(36), 
    status VARCHAR(20) DEFAULT 'waiting' NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_waitlist_entries_status CHECK (status IN ('waiting','notified','converted','expired','opted_out','lost')), 
    CONSTRAINT ck_waitlist_entries_type CHECK (waitlist_type IN ('coming_soon','sold_out')), 
    CONSTRAINT ck_waitlist_entries_vertical CHECK (vertical IN ('roofing','restoration','public_adjusters','wholesalers','fix_flip','attorneys')), 
    CONSTRAINT uq_waitlist_zip_vert_county_email UNIQUE (zip_code, vertical, county_id, email)
);

CREATE INDEX ix_waitlist_county_status ON waitlist_entries (county_id, status);

CREATE INDEX ix_waitlist_county_type_status ON waitlist_entries (county_id, waitlist_type, status);

CREATE INDEX ix_waitlist_zip_vertical ON waitlist_entries (zip_code, vertical);

ALTER TABLE sms_opt_ins DROP CONSTRAINT check_opt_in_source;

ALTER TABLE sms_opt_ins ADD CONSTRAINT check_opt_in_source CHECK (source IN ('double_opt_in','manual','import','widget','waitlist_form'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa040_waitlist_entries")


if __name__ == "__main__":
    main()
