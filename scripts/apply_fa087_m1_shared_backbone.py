"""Auto-converted from alembic migration `fa087_m1_shared_backbone` (revision fa087_m1_shared_backbone).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa087_m1_shared_backbone.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE OR REPLACE FUNCTION generate_uuidv7() RETURNS UUID
        LANGUAGE plpgsql AS $$
        DECLARE
            ts_ms  BIGINT := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
            ts_hex TEXT   := lpad(to_hex(ts_ms), 12, '0');
            r1     TEXT   := lpad(to_hex((random() * 4095)::INT), 3, '0');
            r2     TEXT   := lpad(to_hex((random() * 63)::INT | 128), 2, '0');
            r3     TEXT   := lpad(to_hex((random() * 281474976710655)::BIGINT), 14, '0');
        BEGIN
            RETURN (
                substring(ts_hex, 1, 8) || '-' ||
                substring(ts_hex, 9, 4) || '-' ||
                '7' || r1 || '-' ||
                r2 || substring(r3, 1, 2) || '-' ||
                substring(r3, 3, 12)
            )::UUID;
        END;
        $$;

CREATE TABLE prospects (
    prospect_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    property_id INTEGER NOT NULL, 
    contactability_state VARCHAR DEFAULT 'unknown' NOT NULL, 
    channel_consent JSONB DEFAULT '{}'::jsonb NOT NULL, 
    contact_attempts INTEGER DEFAULT '0' NOT NULL, 
    successful_contacts INTEGER DEFAULT '0' NOT NULL, 
    contactability_rate NUMERIC(5, 4) GENERATED ALWAYS AS (CASE WHEN contact_attempts >= 5 THEN successful_contacts::numeric / contact_attempts ELSE NULL END) STORED, 
    cohort_key VARCHAR, 
    last_touch_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (prospect_id), 
    CONSTRAINT fk_prospects_property_id FOREIGN KEY(property_id) REFERENCES properties (id), 
    CONSTRAINT uq_prospects_property_id UNIQUE (property_id), 
    CONSTRAINT ck_prospects_contactability_state CHECK (contactability_state IN ('unknown','enriching','contactable','invalid','exhausted'))
);

CREATE INDEX idx_prospects_property_id ON prospects (property_id);

CREATE INDEX idx_prospects_contactable ON prospects (prospect_id) WHERE contactability_state = 'contactable';

CREATE INDEX idx_prospects_cohort ON prospects (cohort_key) WHERE cohort_key IS NOT NULL;

CREATE TABLE enrichment_provenance (
    id UUID DEFAULT generate_uuidv7() NOT NULL, 
    prospect_id UUID NOT NULL, 
    field_name VARCHAR NOT NULL, 
    source VARCHAR NOT NULL, 
    cost_cents INTEGER DEFAULT '0' NOT NULL, 
    confidence NUMERIC(5, 4), 
    acquired_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT fk_enrichment_provenance_prospect_id FOREIGN KEY(prospect_id) REFERENCES prospects (prospect_id) ON DELETE CASCADE, 
    CONSTRAINT ck_enrichment_provenance_source CHECK (source IN ('voter','appraiser','tracerfy','batchdata','idi')), 
    CONSTRAINT ck_enrichment_provenance_confidence CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1))
);

CREATE INDEX idx_enrichment_provenance_prospect_id ON enrichment_provenance (prospect_id);

CREATE TABLE events (
    event_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    prospect_id UUID, 
    event_type VARCHAR NOT NULL, 
    actor VARCHAR NOT NULL, 
    payload JSONB DEFAULT '{}'::jsonb NOT NULL, 
    occurred_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    source_component VARCHAR NOT NULL, 
    PRIMARY KEY (event_id), 
    CONSTRAINT fk_events_prospect_id FOREIGN KEY(prospect_id) REFERENCES prospects (prospect_id), 
    CONSTRAINT ck_events_event_type CHECK (event_type IN ('prospect.created','enrichment.completed','enrichment.failed','cds.scored','truth.verdict','lane.entry','lane.advance','lane.stall','lane.close','broker.transition','sms.sent','sms.reply','commission.posted','delivery.sent'))
);

CREATE INDEX idx_events_prospect_id ON events (prospect_id);

CREATE INDEX idx_events_occurred_at ON events USING btree (occurred_at);

CREATE INDEX idx_events_type ON events (event_type);

CREATE TABLE processed_events (
    event_id UUID NOT NULL, 
    consumer VARCHAR NOT NULL, 
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    CONSTRAINT pk_processed_events PRIMARY KEY (event_id, consumer), 
    CONSTRAINT fk_processed_events_event_id FOREIGN KEY(event_id) REFERENCES events (event_id) ON DELETE CASCADE
);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa087_m1_shared_backbone")


if __name__ == "__main__":
    main()
