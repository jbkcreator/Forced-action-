"""subscriber_session_metrics + churn_defense_leads — Task 6.3 (Predictive Engagement & Churn Defense)

Portal-engagement telemetry + churn-defense staging for Phase 6. The weekly
worker (src/tasks/churn_defense_engagement_decay.py) recomputes rolling
engagement counts from webhook_events, snapshots them per subscriber, and stages
at-risk accounts for retention outreach.

Uses IF NOT EXISTS so the migration is safe to run against a DB where these
objects were already created directly (the live deploy uses
scripts/apply_fa_6_3_churn_defense.py — the alembic CLI is unusable on this
multi-head tree).

Revision ID: fa_6_3_churn_defense
Revises:     fa_s1_commission_ledger
Create Date: 2026-07-02
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa_6_3_churn_defense"
down_revision = "fa_s1_commission_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS subscriber_session_metrics (
            id                      SERIAL PRIMARY KEY,
            subscriber_id           INTEGER NOT NULL UNIQUE REFERENCES subscribers(id) ON DELETE CASCADE,
            last_login_at           TIMESTAMP WITH TIME ZONE,
            dashboard_views_7_day   INTEGER NOT NULL DEFAULT 0,
            lead_downloads_7_day    INTEGER NOT NULL DEFAULT 0,
            auth_intervals_seconds  INTEGER NOT NULL DEFAULT 0,
            engagement_decay_scalar NUMERIC(3, 2) NOT NULL DEFAULT 1.00,
            updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """))
    op.execute(sa.text(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_subscriber_session_metrics_subscriber "
        "ON subscriber_session_metrics (subscriber_id);"
    ))
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS churn_defense_leads (
            id              SERIAL PRIMARY KEY,
            subscriber_id   INTEGER NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
            risk_score      NUMERIC(4, 3) NOT NULL,
            outreach_status VARCHAR(50) NOT NULL DEFAULT 'STAGED',
            triggered_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """))
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_subscriber "
        "ON churn_defense_leads (subscriber_id);"
    ))
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_triggered_at "
        "ON churn_defense_leads (triggered_at);"
    ))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS churn_defense_leads;"))
    op.execute(sa.text("DROP TABLE IF EXISTS subscriber_session_metrics;"))
