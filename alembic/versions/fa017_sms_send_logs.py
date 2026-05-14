"""Add sms_send_logs audit table (V3 compliance baseline).

Revision ID: fa017_sms_send_logs
Revises: fa020_dlq_reason_widen
Create Date: 2026-05-14
"""

from alembic import op
import sqlalchemy as sa

revision = "fa017_sms_send_logs"
down_revision = "fa020_dlq_reason_widen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sms_send_logs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("phone", sa.String(20), nullable=True),
        sa.Column("subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("task_type", sa.String(80), nullable=True),
        sa.Column("message_type", sa.String(20), nullable=False),
        sa.Column("outcome", sa.String(20), nullable=False),
        sa.Column("suppress_reason", sa.String(40), nullable=True),
        sa.Column("vendor_message_id", sa.String(80), nullable=True),
        sa.Column("vendor", sa.String(20), nullable=False, server_default="telnyx"),
        sa.Column("campaign", sa.String(100), nullable=True),
        sa.Column("variant_id", sa.String(100), nullable=True),
        sa.Column("decision_id", sa.String(36), nullable=True),
        sa.Column("body_preview", sa.String(160), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.CheckConstraint(
            "outcome IN ('sent', 'suppressed', 'dry_run', 'failed')",
            name="check_ssl_outcome",
        ),
        sa.CheckConstraint(
            "message_type IN ('marketing', 'transactional', 'opt_in_prompt')",
            name="check_ssl_message_type",
        ),
    )
    op.create_index("idx_ssl_phone", "sms_send_logs", ["phone"])
    op.create_index("idx_ssl_sub_created", "sms_send_logs", ["subscriber_id", "created_at"])
    op.create_index("idx_ssl_outcome_created", "sms_send_logs", ["outcome", "created_at"])
    op.create_index("idx_ssl_vendor_msg_id", "sms_send_logs", ["vendor_message_id"])


def downgrade() -> None:
    op.drop_index("idx_ssl_vendor_msg_id", table_name="sms_send_logs")
    op.drop_index("idx_ssl_outcome_created", table_name="sms_send_logs")
    op.drop_index("idx_ssl_sub_created", table_name="sms_send_logs")
    op.drop_index("idx_ssl_phone", table_name="sms_send_logs")
    op.drop_table("sms_send_logs")
