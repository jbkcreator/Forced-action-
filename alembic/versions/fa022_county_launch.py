"""County launch: expansion_candidates + county_launch_audit tables.

Revision ID: fa022
Revises: fa021_rename_manager_to_registered_agent
Create Date: 2026-05-18
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "fa022"
down_revision = "fa021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expansion_candidates",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("county_id", sa.String(64), nullable=False, unique=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_slack_posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_slack_message_ts", sa.String(32), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approved_by_slack_user", sa.String(32), nullable=True),
        sa.Column("launched_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('queued','approved','launching','launched','aborted','skipped')",
            name="ck_expansion_candidates_status",
        ),
    )
    op.create_index(
        "ix_expansion_candidates_status_priority",
        "expansion_candidates",
        ["status", "priority"],
    )

    op.create_table(
        "county_launch_audit",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("county_id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("actor", sa.String(64), nullable=False),
        sa.Column("gate_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint(
            "event_type IN ('evaluated','posted','approved','rejected','launch_started',"
            "'launch_aborted_gate_red','launched','cooldown_skipped')",
            name="ck_county_launch_audit_event",
        ),
    )
    op.create_index(
        "ix_county_launch_audit_county_time",
        "county_launch_audit",
        ["county_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_county_launch_audit_county_time", "county_launch_audit")
    op.drop_table("county_launch_audit")
    op.drop_index("ix_expansion_candidates_status_priority", "expansion_candidates")
    op.drop_table("expansion_candidates")
