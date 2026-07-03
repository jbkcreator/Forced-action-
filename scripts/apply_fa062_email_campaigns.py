"""Backfilled from alembic migration `fa062_email_campaigns` (revision fa062).

Schema DDL hand-extracted from a mixed schema+data migration that could not
auto-render offline (used `upgrade(conn)`). Idempotent — `IF NOT EXISTS`,
guarded constraint, and value-guarded backfill UPDATEs. Live DB already has
this; kept so every schema change lives in scripts/ (ADR 0024).

Usage:
    PYTHONPATH=. python scripts/apply_fa062_email_campaigns.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE dbpr_contacts
    ADD COLUMN IF NOT EXISTS email_source        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS email_verified      BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS email_verified_at   TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS clay_enriched_at    TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS is_opted_out        BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_hard_bounced     BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_signed_up        BOOLEAN NOT NULL DEFAULT FALSE;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_dbpr_email_source') THEN
        ALTER TABLE dbpr_contacts ADD CONSTRAINT check_dbpr_email_source
            CHECK (email_source IS NULL OR email_source IN ('clay', 'batchdata', 'raw'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_dbpr_is_opted_out    ON dbpr_contacts (is_opted_out);
CREATE INDEX IF NOT EXISTS idx_dbpr_is_hard_bounced ON dbpr_contacts (is_hard_bounced);
CREATE INDEX IF NOT EXISTS idx_dbpr_is_signed_up    ON dbpr_contacts (is_signed_up);

UPDATE dbpr_contacts SET is_opted_out    = TRUE WHERE email_status = 'opted_out' AND is_opted_out    = FALSE;
UPDATE dbpr_contacts SET is_hard_bounced = TRUE WHERE email_status = 'bounced'   AND is_hard_bounced = FALSE;
UPDATE dbpr_contacts SET is_signed_up    = TRUE WHERE email_status = 'signed_up' AND is_signed_up    = FALSE;

CREATE TABLE IF NOT EXISTS email_sequence_templates (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL UNIQUE,
    steps           JSONB        NOT NULL DEFAULT '[]',
    variables_used  JSONB        NOT NULL DEFAULT '[]',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS email_campaigns (
    id                      SERIAL PRIMARY KEY,
    name                    VARCHAR(255) NOT NULL,
    instantly_campaign_id   VARCHAR(100) UNIQUE,
    template_id             INTEGER REFERENCES email_sequence_templates(id) ON DELETE RESTRICT,
    county_id               VARCHAR(50),
    geo_filter              JSONB NOT NULL DEFAULT '{}',
    vertical                VARCHAR(50),
    max_contacts            INTEGER,
    start_date              DATE,
    end_date                DATE,
    send_schedule           JSONB NOT NULL DEFAULT '{}',
    status                  VARCHAR(12) NOT NULL DEFAULT 'draft',
    last_synced_at          TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT check_campaign_status CHECK (status IN ('draft', 'active', 'paused', 'completed'))
);
CREATE INDEX IF NOT EXISTS idx_email_campaign_status   ON email_campaigns (status);
CREATE INDEX IF NOT EXISTS idx_email_campaign_county   ON email_campaigns (county_id);
CREATE INDEX IF NOT EXISTS idx_email_campaign_vertical ON email_campaigns (vertical);

CREATE TABLE IF NOT EXISTS campaign_contacts (
    id                  SERIAL PRIMARY KEY,
    campaign_id         INTEGER NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
    dbpr_contact_id     INTEGER NOT NULL REFERENCES dbpr_contacts(id)   ON DELETE CASCADE,
    instantly_lead_id   VARCHAR(100),
    engagement_status   VARCHAR(20) NOT NULL DEFAULT 'active',
    added_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_activity_at    TIMESTAMP WITH TIME ZONE,
    converted_at        TIMESTAMP WITH TIME ZONE,
    CONSTRAINT check_engagement_status CHECK (engagement_status IN
        ('active', 'completed', 'bounced', 'unsubscribed', 'interested', 'not_interested')),
    CONSTRAINT uq_campaign_contact UNIQUE (campaign_id, dbpr_contact_id)
);
CREATE INDEX IF NOT EXISTS idx_cc_campaign_status ON campaign_contacts (campaign_id, engagement_status);
CREATE INDEX IF NOT EXISTS idx_cc_dbpr_contact    ON campaign_contacts (dbpr_contact_id);

CREATE TABLE IF NOT EXISTS campaign_daily_analytics (
    id              SERIAL PRIMARY KEY,
    campaign_id     INTEGER NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
    snapshot_date   DATE    NOT NULL,
    total_contacts  INTEGER NOT NULL DEFAULT 0,
    emails_sent     INTEGER NOT NULL DEFAULT 0,
    opens           INTEGER NOT NULL DEFAULT 0,
    open_rate       NUMERIC(6,4) NOT NULL DEFAULT 0,
    replies         INTEGER NOT NULL DEFAULT 0,
    reply_rate      NUMERIC(6,4) NOT NULL DEFAULT 0,
    clicks          INTEGER NOT NULL DEFAULT 0,
    bounces         INTEGER NOT NULL DEFAULT 0,
    unsubscribes    INTEGER NOT NULL DEFAULT 0,
    interested      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_campaign_snapshot UNIQUE (campaign_id, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_cda_campaign_date ON campaign_daily_analytics (campaign_id, snapshot_date DESC);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa062_email_campaigns")


if __name__ == "__main__":
    main()
