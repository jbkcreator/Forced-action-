"""vendor_cost_pause_monitor

Creates vendor_cost_pauses table and extends api_usage_logs with columns
needed for vendor cost monitoring: pause_target, graph_name, blocked_by_pause,
and block_reason.

Revision ID: vendor_cost_pause_monitor
Revises:     y9z0a1b2c3d4_add_raw_response
Create Date: 2026-05-26
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "vendor_cost_pause_monitor"
down_revision = "y9z0a1b2c3d4_add_raw_response"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- vendor_cost_pauses table -------------------------------------------------
    op.create_table(
        "vendor_cost_pauses",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("vendor", sa.String(30), nullable=False),
        sa.Column("pause_target", sa.String(80), nullable=False),
        sa.Column("source_table", sa.String(80), nullable=True),
        sa.Column("source_key", sa.String(120), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("anomaly_score", sa.Numeric(10, 4), nullable=True),
        sa.Column("today_cost_usd", sa.Numeric(10, 6), nullable=True),
        sa.Column("baseline_avg_usd", sa.Numeric(10, 6), nullable=True),
        sa.Column("baseline_stddev_usd", sa.Numeric(10, 6), nullable=True),
        sa.Column("threshold_usd", sa.Numeric(10, 6), nullable=True),
        sa.Column("sample_n", sa.Integer(), nullable=True),
        sa.Column("window_days", sa.Integer(), nullable=False, server_default="14"),
        sa.Column("paused_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("auto_resume_at", sa.DateTime(), nullable=True),
        sa.Column("resumed_at", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("created_by", sa.String(40), nullable=False, server_default="cost_monitor"),
        sa.Column("resumed_by", sa.String(80), nullable=True),
        sa.Column("metadata_json", JSONB(), nullable=True, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('active', 'auto_resumed', 'manually_resumed', 'superseded')",
            name="check_vendor_cost_pause_status",
        ),
    )
    op.create_index("idx_vendor_cost_pause_vendor", "vendor_cost_pauses", ["vendor"])
    op.create_index("idx_vendor_cost_pause_target", "vendor_cost_pauses", ["pause_target"])
    op.create_index(
        "idx_vendor_cost_pause_vendor_target",
        "vendor_cost_pauses",
        ["vendor", "pause_target"],
    )
    op.create_index(
        "idx_vendor_cost_pause_status_resume",
        "vendor_cost_pauses",
        ["status", "auto_resume_at"],
    )
    op.create_index(
        "idx_vendor_cost_pause_paused_at",
        "vendor_cost_pauses",
        ["paused_at"],
    )

    # -- api_usage_logs column additions -----------------------------------------
    op.add_column(
        "api_usage_logs",
        sa.Column("graph_name", sa.String(60), nullable=True),
    )
    op.add_column(
        "api_usage_logs",
        sa.Column("pause_target", sa.String(80), nullable=True),
    )
    op.add_column(
        "api_usage_logs",
        sa.Column("blocked_by_pause", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "api_usage_logs",
        sa.Column("block_reason", sa.String(120), nullable=True),
    )

    op.create_index("idx_api_usage_graph_name", "api_usage_logs", ["graph_name"])
    op.create_index("idx_api_usage_pause_target", "api_usage_logs", ["pause_target"])
    op.create_index(
        "idx_api_usage_pause_created",
        "api_usage_logs",
        ["pause_target", "created_at"],
    )


def downgrade() -> None:
    # api_usage_logs column removals
    op.drop_index("idx_api_usage_pause_created", table_name="api_usage_logs")
    op.drop_index("idx_api_usage_pause_target", table_name="api_usage_logs")
    op.drop_index("idx_api_usage_graph_name", table_name="api_usage_logs")
    op.drop_column("api_usage_logs", "block_reason")
    op.drop_column("api_usage_logs", "blocked_by_pause")
    op.drop_column("api_usage_logs", "pause_target")
    op.drop_column("api_usage_logs", "graph_name")

    # vendor_cost_pauses table removal
    op.drop_index("idx_vendor_cost_pause_paused_at", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_status_resume", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_vendor_target", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_target", table_name="vendor_cost_pauses")
    op.drop_index("idx_vendor_cost_pause_vendor", table_name="vendor_cost_pauses")
    op.drop_table("vendor_cost_pauses")