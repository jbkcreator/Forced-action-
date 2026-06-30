"""commission_splits + commission_ledger — Layer 3E (Commission Ledger / WS-C)

Config-driven split rules + append-only broker-earnings ledger. One ledger row
per closed_won transition; disputes flip status and post an offsetting entry.

Uses IF NOT EXISTS so the migration is safe to run against a DB where these
objects were already created directly (the live deploy uses
scripts/apply_fa_s1_commission_ledger.py — alembic CLI is unusable on this
multi-head tree).

Revision ID: fa_s1_commission_ledger
Revises:     fa099_broker_transitions
Create Date: 2026-06-30
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa_s1_commission_ledger"
down_revision = "fa099_broker_transitions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS commission_splits (
            split_config_id  VARCHAR(100) PRIMARY KEY,
            name             VARCHAR(255) NOT NULL,
            parties          JSONB NOT NULL,
            is_active        BOOLEAN NOT NULL DEFAULT TRUE
        );
    """))
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS commission_ledger (
            entry_id                UUID PRIMARY KEY DEFAULT generate_uuidv7(),
            prospect_id             UUID NOT NULL REFERENCES prospects(prospect_id),
            lane_id                 UUID NOT NULL REFERENCES lanes(lane_id),
            broker_id               UUID NOT NULL REFERENCES brokers(broker_id),
            trigger_transition_id   UUID UNIQUE REFERENCES broker_transitions(transition_id),
            gross_amount_cents      BIGINT NOT NULL,
            split_config_id         VARCHAR(100) NOT NULL REFERENCES commission_splits(split_config_id),
            net_lines               JSONB NOT NULL,
            status                  VARCHAR(20) NOT NULL DEFAULT 'posted',
            posted_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            CONSTRAINT ck_cl_gross_nonneg CHECK (gross_amount_cents >= 0),
            CONSTRAINT ck_cl_status CHECK (status IN ('posted','disputed','reconciled'))
        );
    """))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_cl_lane_id ON commission_ledger (lane_id);"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS idx_cl_broker_id ON commission_ledger (broker_id);"))
    op.execute(sa.text("""
        INSERT INTO commission_splits (split_config_id, name, parties, is_active)
        VALUES (
            'platform_50_broker_50',
            'Platform 50 / Broker 50',
            '[{"party": "platform", "pct": 50}, {"party": "broker", "pct": 50}]'::jsonb,
            TRUE
        )
        ON CONFLICT (split_config_id) DO NOTHING;
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS commission_ledger;"))
    op.execute(sa.text("DROP TABLE IF EXISTS commission_splits;"))
