"""Auto-converted from alembic migration `fa024_county_schema_catchup` (revision fa024_county_catchup).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa024_county_schema_catchup.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE county_sources ALTER COLUMN url DROP NOT NULL;

ALTER TABLE county_sources ADD COLUMN scrape_mode VARCHAR(32) DEFAULT 'ai_only' NOT NULL;

ALTER TABLE county_sources ADD CONSTRAINT check_county_sources_scrape_mode CHECK (scrape_mode IN ('ai_only','playwright_only','playwright_then_ai','static_download','api'));

ALTER TABLE county_sources ADD COLUMN playwright_code TEXT;

ALTER TABLE county_sources ADD COLUMN playwright_code_version VARCHAR(32);

ALTER TABLE county_sources ADD COLUMN playwright_code_approved BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE county_column_mappings ADD COLUMN sample_rows JSONB;

ALTER TABLE county_column_mappings ADD COLUMN reject_feedback TEXT;

ALTER TABLE county_column_mappings ADD COLUMN mapped_by VARCHAR(10) DEFAULT 'llm';

ALTER TABLE county_column_mappings ADD COLUMN post_processors JSONB;

ALTER TABLE county_column_mappings ADD COLUMN value_maps JSONB;

ALTER TABLE county_column_mappings ADD COLUMN row_routing JSONB;

CREATE TABLE playwright_code_history (
    id SERIAL NOT NULL, 
    source_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    code TEXT, 
    prompt_version VARCHAR(20), 
    reason VARCHAR(40) NOT NULL, 
    is_approved BOOLEAN DEFAULT false NOT NULL, 
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(source_id) REFERENCES county_sources (id)
);

CREATE INDEX idx_pwc_history_source_generated ON playwright_code_history (source_id, generated_at);

CREATE INDEX ix_playwright_code_history_county_id ON playwright_code_history (county_id);

CREATE INDEX ix_playwright_code_history_source_id ON playwright_code_history (source_id);

CREATE TABLE cf_bypass_profiles (
    id SERIAL NOT NULL, 
    profile_name VARCHAR(80) NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    portal_url TEXT NOT NULL, 
    status VARCHAR(20) DEFAULT 'unwarmed' NOT NULL, 
    last_warmed_at TIMESTAMP WITH TIME ZONE, 
    last_validated_at TIMESTAMP WITH TIME ZONE, 
    last_failure_at TIMESTAMP WITH TIME ZONE, 
    last_failure_reason TEXT, 
    profile_dir_path TEXT NOT NULL, 
    validation_ttl_minutes INTEGER DEFAULT '540' NOT NULL, 
    profile_blob BYTEA, 
    profile_blob_size INTEGER, 
    profile_blob_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_cf_bypass_profile_name UNIQUE (profile_name), 
    CONSTRAINT check_cf_profile_status CHECK (status IN ('unwarmed','ready','warming','expired','failed'))
);

CREATE INDEX ix_cf_bypass_profiles_county_id ON cf_bypass_profiles (county_id);

CREATE INDEX ix_cf_bypass_profiles_status_lookup ON cf_bypass_profiles (status);

ALTER TABLE sms_opt_outs ALTER COLUMN source SET NOT NULL;

ALTER TABLE sms_opt_outs ALTER COLUMN source SET DEFAULT 'inbound_sms';
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa024_county_schema_catchup")


if __name__ == "__main__":
    main()
