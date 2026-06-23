"""Add approved_by + slack_message_ts to win_story_assets; set is_public default to false.

Revision ID: fa085_win_story_approval_gate
Revises: fa084_add_past_due_to_subscriber_status
Create Date: 2026-06-22
"""

from alembic import op
import sqlalchemy as sa

revision = "fa085_win_story_approval_gate"
down_revision = "fa084_add_past_due_to_subscriber_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "win_story_assets",
        sa.Column("approved_by", sa.String(100), nullable=True),
    )
    op.add_column(
        "win_story_assets",
        sa.Column("slack_message_ts", sa.String(50), nullable=True),
    )
    op.alter_column(
        "win_story_assets",
        "is_public",
        server_default=sa.false(),
        existing_type=sa.Boolean(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "win_story_assets",
        "is_public",
        server_default=sa.true(),
        existing_type=sa.Boolean(),
        existing_nullable=False,
    )
    op.drop_column("win_story_assets", "slack_message_ts")
    op.drop_column("win_story_assets", "approved_by")
