"""fa018_nws_alerts_table — Idempotent NWS alert store

One row per unique NWS alert ID. Prevents duplicate storm-pack triggers and
Cora urgency messages across poll cycles. Also gates the revenue trigger on
nws_weather_enabled / storm_pack_enabled / nws_cora_urgency_enabled flags.

Revision ID: fa018_nws_alerts_table
Revises:     fa017_owner_manager_fields
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "fa018_nws_alerts_table"
down_revision = "fa017_owner_manager_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nws_alerts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("alert_id", sa.String(200), unique=True, nullable=False),
        sa.Column("event", sa.String(100), nullable=False),
        sa.Column("severity", sa.String(30), nullable=True),
        sa.Column("urgency", sa.String(30), nullable=True),
        sa.Column("certainty", sa.String(30), nullable=True),
        sa.Column("headline", sa.Text, nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("instruction", sa.Text, nullable=True),
        sa.Column("area_desc", sa.Text, nullable=True),
        sa.Column("same_codes", postgresql.JSONB, nullable=True),
        sa.Column("ugc_codes", postgresql.JSONB, nullable=True),
        sa.Column("affected_zips", postgresql.JSONB, nullable=True),
        sa.Column("effective", sa.DateTime(timezone=True), nullable=True),
        sa.Column("onset", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ends", sa.DateTime(timezone=True), nullable=True),
        sa.Column("county_id", sa.String(50), nullable=False, server_default="hillsborough"),
        sa.Column("storm_pack_triggered", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("cora_urgency_sent", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("subscriber_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("raw_payload", postgresql.JSONB, nullable=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )

    op.create_index("ix_nws_alerts_alert_id", "nws_alerts", ["alert_id"], unique=True)
    op.create_index("ix_nws_alerts_county_id", "nws_alerts", ["county_id"])
    op.create_index("ix_nws_alerts_processed_at", "nws_alerts", ["processed_at"])
    op.create_index("ix_nws_alerts_event_processed", "nws_alerts", ["event", "processed_at"])
    op.create_index(
        "ix_nws_alerts_affected_zips_gin",
        "nws_alerts",
        ["affected_zips"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_nws_alerts_affected_zips_gin", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_event_processed", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_processed_at", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_county_id", table_name="nws_alerts")
    op.drop_index("ix_nws_alerts_alert_id", table_name="nws_alerts")
    op.drop_table("nws_alerts")
