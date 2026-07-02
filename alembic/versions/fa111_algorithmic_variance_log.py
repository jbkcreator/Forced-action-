"""Add algorithmic_variance_log — Task 6.2 cost-control gate audit trail.

Revision ID: fa111_algorithmic_variance_log
Revises:     fa110_platform_revenue_ledger
Create Date: 2026-07-02
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa111_algorithmic_variance_log"
down_revision = "fa110_platform_revenue_ledger"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "algorithmic_variance_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lead_id", sa.Integer(), nullable=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("county", sa.String(length=50), nullable=True),
        sa.Column("vertical", sa.String(length=50), nullable=True),
        sa.Column("lead_tier", sa.String(length=20), nullable=True),
        sa.Column("spend_ratio", sa.Numeric(14, 6), nullable=True),
        sa.Column("threshold", sa.Numeric(14, 6), nullable=False),
        sa.Column("selected_path", sa.String(length=20), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=True),
        sa.Column("paid_lookup_allowed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("routing_reason", sa.String(length=32), nullable=False),
        sa.Column("lookup_success", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("cost_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "selected_path IN ('paid_trace','free_cross_match','blocked','override_paid')",
            name="check_avl_selected_path",
        ),
        sa.CheckConstraint(
            "routing_reason IN ('spend_ratio_safe','spend_ratio_exceeded',"
            "'zero_revenue_guard','missing_telemetry_guard','manual_override')",
            name="check_avl_routing_reason",
        ),
    )
    op.create_index("idx_avl_lead_id", "algorithmic_variance_log", ["lead_id"])
    op.create_index("idx_avl_property_id", "algorithmic_variance_log", ["property_id"])
    op.create_index("idx_avl_subscriber_id", "algorithmic_variance_log", ["subscriber_id"])
    op.create_index("idx_avl_created_at", "algorithmic_variance_log", ["created_at"])
    op.create_index("idx_avl_subscriber_created", "algorithmic_variance_log", ["subscriber_id", "created_at"])
    op.create_index("idx_avl_property_created", "algorithmic_variance_log", ["property_id", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_avl_property_created", table_name="algorithmic_variance_log")
    op.drop_index("idx_avl_subscriber_created", table_name="algorithmic_variance_log")
    op.drop_index("idx_avl_created_at", table_name="algorithmic_variance_log")
    op.drop_index("idx_avl_subscriber_id", table_name="algorithmic_variance_log")
    op.drop_index("idx_avl_property_id", table_name="algorithmic_variance_log")
    op.drop_index("idx_avl_lead_id", table_name="algorithmic_variance_log")
    op.drop_table("algorithmic_variance_log")
