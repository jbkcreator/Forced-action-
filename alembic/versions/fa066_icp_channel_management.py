"""fa066_icp_channel_management

ICP Channel Launch Support schema additions.

  1. subscribers.icp_channel_key — explicit ICP attribution per subscriber.
     Defaults to 'contractor' for all existing rows (backfill via server_default).
     Verticals alone are NOT safe for ICP scoping (they overlap between ICPs).
     contractor_mrr.py TODO is resolved by this column.

  2. icp_daily_stats — raw metric counts per (run_date, county_id, icp_channel_key).
     Kill-switch percentages (first_payment_rate, saved_card_rate, etc.) are computed
     at read time from raw counts so the kill-switch score is fully auditable.

  3. icp_channel_launch_audit — immutable event log for every ICP channel status
     transition including force activations (with reason + gate snapshot).

  4. Drop the hardcoded vertical CHECK constraint on waitlist_entries — validity
     is enforced at the application layer via VALID_VERTICALS (config/scoring.py).
     This allows new verticals to be added without a migration.

Revision ID: fa066
Revises:     fa065
Create Date: 2026-06-02
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa066"
down_revision: Union[str, Sequence[str], None] = "fa065"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. subscribers.icp_channel_key ────────────────────────────────────
    op.add_column(
        "subscribers",
        sa.Column(
            "icp_channel_key",
            sa.String(40),
            nullable=False,
            server_default="contractor",
        ),
    )
    op.create_index(
        "idx_subscribers_icp_channel_key",
        "subscribers",
        ["icp_channel_key"],
    )

    # ── 2. icp_daily_stats ────────────────────────────────────────────────
    op.create_table(
        "icp_daily_stats",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("run_date", sa.Date, nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("icp_channel_key", sa.String(40), nullable=False),
        # Raw counts — percentages derived at read time, not stored
        sa.Column("signup_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("payer_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("saved_card_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("sms_sent_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("sms_reply_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("active_subscriber_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("cancel_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("refund_count", sa.Integer, nullable=False, server_default="0"),
        # MRR in cents to avoid float precision issues
        sa.Column("mrr_cents", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.UniqueConstraint(
            "run_date", "county_id", "icp_channel_key",
            name="uq_icp_daily_stats_date_county_channel",
        ),
    )
    op.create_index(
        "idx_icp_daily_stats_channel_date",
        "icp_daily_stats",
        ["icp_channel_key", "run_date"],
    )

    # ── 3. icp_channel_launch_audit ───────────────────────────────────────
    op.create_table(
        "icp_channel_launch_audit",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("channel_key", sa.String(40), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("actor", sa.String(100), nullable=False),
        sa.Column("is_force_activate", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("force_reason", sa.Text, nullable=True),
        sa.Column("gate_snapshot", JSONB, nullable=True),
        sa.Column("prev_status", sa.String(20), nullable=True),
        sa.Column("new_status", sa.String(20), nullable=True),
        sa.Column("detail", JSONB, nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'activated','paused','killed','force_activated',"
            "'config_updated','gate_evaluated','created'"
            ")",
            name="ck_icp_audit_event_type",
        ),
    )
    op.create_index(
        "idx_icp_audit_channel_created",
        "icp_channel_launch_audit",
        ["channel_key", "created_at"],
    )

    # ── 4. Drop hardcoded vertical CHECK on waitlist_entries ─────────────
    # The constraint name from the original migration (fa022) is
    # ck_waitlist_entries_vertical. Drop it so new verticals don't require
    # a migration — VALID_VERTICALS in config/scoring.py enforces this.
    op.drop_constraint(
        "ck_waitlist_entries_vertical",
        "waitlist_entries",
        type_="check",
    )


def downgrade() -> None:
    # Re-add the vertical CHECK (exact original SQL)
    op.create_check_constraint(
        "ck_waitlist_entries_vertical",
        "waitlist_entries",
        "vertical IN ('roofing','restoration','public_adjusters',"
        "'wholesalers','fix_flip','attorneys')",
    )

    op.drop_index("idx_icp_audit_channel_created", table_name="icp_channel_launch_audit")
    op.drop_table("icp_channel_launch_audit")

    op.drop_index("idx_icp_daily_stats_channel_date", table_name="icp_daily_stats")
    op.drop_table("icp_daily_stats")

    op.drop_index("idx_subscribers_icp_channel_key", table_name="subscribers")
    op.drop_column("subscribers", "icp_channel_key")
