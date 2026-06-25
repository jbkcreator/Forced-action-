"""fa097 - subscriber memory summary table

Creates the `subscriber_memory_summary` table used by the unified memory
projection for fast operational reads.

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply operationally via:

    PYTHONPATH=. python scripts/apply_fa097_subscriber_memory_summary.py
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa097_subscriber_memory_summary"
down_revision: Union[str, Sequence[str], None] = "fa096_unified_subscriber_memory"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "subscriber_memory_summary",
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), primary_key=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_type", sa.String(length=100), nullable=True),
        sa.Column("last_stripe_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stripe_event_type", sa.String(length=100), nullable=True),
        sa.Column("latest_payment_state", sa.String(length=100), nullable=True),
        sa.Column("latest_checkout_state", sa.String(length=100), nullable=True),
        sa.Column("latest_crm_status", sa.String(length=100), nullable=True),
        sa.Column("latest_crm_stage", sa.String(length=100), nullable=True),
        sa.Column("last_sms_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sms_event_type", sa.String(length=100), nullable=True),
        sa.Column("latest_sms_state", sa.String(length=100), nullable=True),
        sa.Column("last_sms_reply_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sms_opted_out", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("last_voice_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_voice_event_type", sa.String(length=100), nullable=True),
        sa.Column("last_underwriting_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_underwriting_event_type", sa.String(length=100), nullable=True),
        sa.Column("latest_underwriting_state", sa.String(length=100), nullable=True),
        sa.Column("latest_underwriting_milestone", sa.String(length=100), nullable=True),
        sa.Column("last_lead_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_sms_last_event", "subscriber_memory_summary", ["last_sms_event_at"])
    op.create_index("idx_usm_summary_last_event", "subscriber_memory_summary", ["last_event_at"])


def downgrade() -> None:
    op.drop_index("idx_usm_summary_last_event", table_name="subscriber_memory_summary")
    op.drop_index("idx_sms_last_event", table_name="subscriber_memory_summary")
    op.drop_table("subscriber_memory_summary")
