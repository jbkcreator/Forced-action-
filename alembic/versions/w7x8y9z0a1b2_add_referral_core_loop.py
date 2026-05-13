"""add_referral_core_loop

Phase 1 of Referral Core Loop:
- New table: referral_milestone_awards (idempotent milestone grants)
- New table: referral_forward_copy (weekly Claude-generated share copy per vertical)
- Add subscribers.bonus_zip_slots (integer, default 0)
- Extend referral_events.status check constraint to include 'revoked'

Revision ID: fa004_referral_core_loop
Revises:     fa003_phone_metadata
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op

revision = "fa004_referral_core_loop"
down_revision = "fa003_phone_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── referral_milestone_awards ──────────────────────────────────────
    op.create_table(
        "referral_milestone_awards",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("referrer_subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("milestone", sa.String(30), nullable=False),
        sa.Column("awarded_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("triggering_referral_event_id", sa.Integer, sa.ForeignKey("referral_events.id"), nullable=True),
        sa.Column("grant_ref", sa.Text, nullable=True),
        sa.Column("notified_at", sa.DateTime, nullable=True),
        sa.UniqueConstraint("referrer_subscriber_id", "milestone", name="uq_referral_milestone_per_referrer"),
        sa.CheckConstraint(
            "milestone IN ('free_month_3', 'lock_slot_5')",
            name="check_referral_milestone",
        ),
    )
    op.create_index(
        "idx_referral_milestone_referrer",
        "referral_milestone_awards",
        ["referrer_subscriber_id"],
    )

    # ── referral_forward_copy ─────────────────────────────────────────
    op.create_table(
        "referral_forward_copy",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("vertical", sa.String(50), nullable=False),
        sa.Column("week_start", sa.Date, nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("generated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("vertical", "week_start", name="uq_referral_forward_copy_vertical_week"),
    )

    # ── subscribers.bonus_zip_slots ───────────────────────────────────
    op.add_column(
        "subscribers",
        sa.Column("bonus_zip_slots", sa.Integer, nullable=False, server_default="0"),
    )

    # ── extend referral_events.status check constraint ────────────────
    op.drop_constraint("check_referral_status", "referral_events", type_="check")
    op.create_check_constraint(
        "check_referral_status",
        "referral_events",
        "status IN ('pending', 'confirmed', 'rewarded', 'expired', 'revoked')",
    )


def downgrade() -> None:
    # Restore original status constraint
    op.drop_constraint("check_referral_status", "referral_events", type_="check")
    op.create_check_constraint(
        "check_referral_status",
        "referral_events",
        "status IN ('pending', 'confirmed', 'rewarded', 'expired')",
    )

    op.drop_column("subscribers", "bonus_zip_slots")
    op.drop_table("referral_forward_copy")
    op.drop_index("idx_referral_milestone_referrer", table_name="referral_milestone_awards")
    op.drop_table("referral_milestone_awards")
