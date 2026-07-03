"""Auto-converted from alembic migration `fa031_sunbiz_owner_extension` (revision fa031_sunbiz_owner_extension).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa031_sunbiz_owner_extension.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE owners ADD COLUMN sunbiz_doc_number TEXT;

ALTER TABLE owners ADD COLUMN principal_address TEXT;

ALTER TABLE owners ADD COLUMN registered_agent_email VARCHAR(255);

ALTER TABLE owners ADD COLUMN entity_status VARCHAR(20);

ALTER TABLE owners ADD COLUMN formation_date DATE;

ALTER TABLE owners ADD COLUMN managing_members JSONB;

ALTER TABLE owners ADD COLUMN sunbiz_enriched_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE owners ADD COLUMN sunbiz_status VARCHAR(20) DEFAULT 'pending' NOT NULL;

ALTER TABLE owners ADD CONSTRAINT check_sunbiz_status CHECK (sunbiz_status IN ('pending','matched','not_found','ambiguous','parser_failed','not_an_llc'));

CREATE INDEX ix_owners_sunbiz_doc ON owners(sunbiz_doc_number) WHERE sunbiz_doc_number IS NOT NULL;

CREATE INDEX ix_owners_sunbiz_status ON owners (sunbiz_status);

CREATE INDEX ix_owners_sunbiz_enriched ON owners(sunbiz_enriched_at) WHERE sunbiz_status = 'matched';

CREATE INDEX ix_owners_managing_members ON owners USING gin (managing_members);

CREATE TABLE sunbiz_snapshots (
    id BIGSERIAL NOT NULL, 
    sunbiz_doc_number TEXT NOT NULL, 
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    raw_html TEXT, 
    raw_jsonb JSONB NOT NULL, 
    parser_version VARCHAR(40) NOT NULL, 
    status VARCHAR(20) NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_snapshot_status CHECK (status IN ('ok','partial','parser_failed'))
);

CREATE INDEX ix_sbsnap_doc_recent ON sunbiz_snapshots(sunbiz_doc_number, scraped_at DESC);

ALTER TABLE sms_opt_ins ADD COLUMN consent_scope VARCHAR(30) DEFAULT 'subscriber' NOT NULL;

ALTER TABLE sms_opt_ins ADD CONSTRAINT check_opt_in_consent_scope CHECK (consent_scope IN ('subscriber','managing_member_direct','agent_direct','other'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa031_sunbiz_owner_extension")


if __name__ == "__main__":
    main()
