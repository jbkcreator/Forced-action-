"""fa008_enrichment_usage_logs

Per-call cost ledger for third-party enrichment vendors (BatchData, Twilio
Lookup, etc.). One row per lookup, drives per-SKU margin dashboards and
the daily Revenue Pulse line.

Revision ID: fa008_enrichment_usage_logs
Revises:     fa007_wallet_concurrency
Create Date: 2026-05-04
"""

import sqlalchemy as sa
from alembic import op


revision = "fa008_enrichment_usage_logs"
down_revision = "fa007_wallet_concurrency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "enrichment_usage_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("vendor", sa.String(length=30), nullable=False),
        sa.Column("purpose", sa.String(length=40), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("target_address", sa.String(length=255), nullable=True),
        sa.Column("cost_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("success", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("error", sa.String(length=255), nullable=True),
        sa.Column("request_ref", sa.String(length=100), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_enrichment_subscriber_id", "enrichment_usage_logs", ["subscriber_id"])
    op.create_index("ix_enrichment_property_id", "enrichment_usage_logs", ["property_id"])
    op.create_index("ix_enrichment_created_at", "enrichment_usage_logs", ["created_at"])
    op.create_index("idx_enrichment_purpose_created", "enrichment_usage_logs", ["purpose", "created_at"])
    op.create_index("idx_enrichment_vendor_created", "enrichment_usage_logs", ["vendor", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_enrichment_vendor_created", table_name="enrichment_usage_logs")
    op.drop_index("idx_enrichment_purpose_created", table_name="enrichment_usage_logs")
    op.drop_index("ix_enrichment_created_at", table_name="enrichment_usage_logs")
    op.drop_index("ix_enrichment_property_id", table_name="enrichment_usage_logs")
    op.drop_index("ix_enrichment_subscriber_id", table_name="enrichment_usage_logs")
    op.drop_table("enrichment_usage_logs")
