"""fa062_email_campaigns

Email Campaign feature (Phase B1).

Changes:
  1. dbpr_contacts — add Clay enrichment columns + global suppression booleans
     (replaces per-campaign meaning of email_status).
  2. email_sequence_templates — FA-side reusable sequence templates.
  3. email_campaigns — one FA campaign = one Instantly campaign (1:1).
  4. campaign_contacts — M:N junction; concurrent multi-campaign membership.
  5. campaign_daily_analytics — daily snapshot rows for dashboard/history.

Data migration:
  - Derive is_opted_out / is_hard_bounced / is_signed_up from existing
    dbpr_contacts.email_status values.

Purely additive (new tables + new nullable/default columns on dbpr_contacts).
Safe to apply during normal hours.

Revision ID: fa062
Revises:     fa059
Create Date: 2026-06-02
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa062"
down_revision: Union[str, None] = "fa059"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade(conn) -> None:
    # ------------------------------------------------------------------
    # 1. dbpr_contacts — Clay + global suppression columns
    # ------------------------------------------------------------------
    conn.execute(sa.text("""
        ALTER TABLE dbpr_contacts
            ADD COLUMN IF NOT EXISTS email_source        VARCHAR(20),
            ADD COLUMN IF NOT EXISTS email_verified      BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS email_verified_at   TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS clay_enriched_at    TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS is_opted_out        BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS is_hard_bounced     BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS is_signed_up        BOOLEAN NOT NULL DEFAULT FALSE
    """))

    # Add CHECK constraint for email_source
    conn.execute(sa.text("""
        ALTER TABLE dbpr_contacts
            ADD CONSTRAINT check_dbpr_email_source
            CHECK (email_source IS NULL OR email_source IN ('clay','batchdata','raw'))
    """))

    # Indexes for suppression columns (used heavily in eligibility query)
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dbpr_is_opted_out    ON dbpr_contacts (is_opted_out)"))
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dbpr_is_hard_bounced ON dbpr_contacts (is_hard_bounced)"))
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_dbpr_is_signed_up    ON dbpr_contacts (is_signed_up)"))

    # Backfill suppression booleans from existing email_status
    conn.execute(sa.text("""
        UPDATE dbpr_contacts SET is_opted_out    = TRUE WHERE email_status = 'opted_out'
    """))
    conn.execute(sa.text("""
        UPDATE dbpr_contacts SET is_hard_bounced = TRUE WHERE email_status = 'bounced'
    """))
    conn.execute(sa.text("""
        UPDATE dbpr_contacts SET is_signed_up    = TRUE WHERE email_status = 'signed_up'
    """))

    # ------------------------------------------------------------------
    # 2. email_sequence_templates
    # ------------------------------------------------------------------
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS email_sequence_templates (
            id              SERIAL PRIMARY KEY,
            name            VARCHAR(255) NOT NULL UNIQUE,
            steps           JSONB        NOT NULL DEFAULT '[]',
            variables_used  JSONB        NOT NULL DEFAULT '[]',
            created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """))

    # ------------------------------------------------------------------
    # 3. email_campaigns
    # ------------------------------------------------------------------
    conn.execute(sa.text("""
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
            CONSTRAINT check_campaign_status
                CHECK (status IN ('draft','active','paused','completed'))
        )
    """))
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_email_campaign_status    ON email_campaigns (status)"))
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_email_campaign_county    ON email_campaigns (county_id)"))
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_email_campaign_vertical  ON email_campaigns (vertical)"))

    # ------------------------------------------------------------------
    # 4. campaign_contacts (M:N junction)
    # ------------------------------------------------------------------
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS campaign_contacts (
            id                  SERIAL PRIMARY KEY,
            campaign_id         INTEGER NOT NULL REFERENCES email_campaigns(id) ON DELETE CASCADE,
            dbpr_contact_id     INTEGER NOT NULL REFERENCES dbpr_contacts(id)   ON DELETE CASCADE,
            instantly_lead_id   VARCHAR(100),
            engagement_status   VARCHAR(20) NOT NULL DEFAULT 'active',
            added_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            last_activity_at    TIMESTAMP WITH TIME ZONE,
            converted_at        TIMESTAMP WITH TIME ZONE,
            CONSTRAINT check_engagement_status
                CHECK (engagement_status IN
                    ('active','completed','bounced','unsubscribed','interested','not_interested')),
            CONSTRAINT uq_campaign_contact
                UNIQUE (campaign_id, dbpr_contact_id)
        )
    """))
    conn.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_cc_campaign_status
            ON campaign_contacts (campaign_id, engagement_status)
    """))
    conn.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_cc_dbpr_contact
            ON campaign_contacts (dbpr_contact_id)
    """))

    # ------------------------------------------------------------------
    # 5. campaign_daily_analytics
    # ------------------------------------------------------------------
    conn.execute(sa.text("""
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
        )
    """))
    conn.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_cda_campaign_date
            ON campaign_daily_analytics (campaign_id, snapshot_date DESC)
    """))


def downgrade(conn) -> None:
    conn.execute(sa.text("DROP TABLE IF EXISTS campaign_daily_analytics CASCADE"))
    conn.execute(sa.text("DROP TABLE IF EXISTS campaign_contacts CASCADE"))
    conn.execute(sa.text("DROP TABLE IF EXISTS email_campaigns CASCADE"))
    conn.execute(sa.text("DROP TABLE IF EXISTS email_sequence_templates CASCADE"))

    conn.execute(sa.text("DROP INDEX IF EXISTS idx_dbpr_is_opted_out"))
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_dbpr_is_hard_bounced"))
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_dbpr_is_signed_up"))
    conn.execute(sa.text("""
        ALTER TABLE dbpr_contacts
            DROP CONSTRAINT IF EXISTS check_dbpr_email_source,
            DROP COLUMN IF EXISTS email_source,
            DROP COLUMN IF EXISTS email_verified,
            DROP COLUMN IF EXISTS email_verified_at,
            DROP COLUMN IF EXISTS clay_enriched_at,
            DROP COLUMN IF EXISTS is_opted_out,
            DROP COLUMN IF EXISTS is_hard_bounced,
            DROP COLUMN IF EXISTS is_signed_up
    """))
