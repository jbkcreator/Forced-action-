"""fa048_cora_suppressions

Adds subscriber-level Cora outbound suppression.

Revision ID: fa048_cora_suppressions
Revises:     fa047_cora_message_hold_review
Create Date: 2026-05-28
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa048_cora_suppressions"
down_revision: Union[str, Sequence[str], None] = "fa047_cora_message_hold_review"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cora_suppressions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("reason", sa.String(40), nullable=False),
        sa.Column("source", sa.String(40), nullable=False),
        sa.Column("source_id", sa.String(100), nullable=True),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_by", sa.String(100), nullable=True),
        sa.Column("notes", sa.String(255), nullable=True),
    )
    op.create_index("idx_cora_suppression_active_sub", "cora_suppressions", ["subscriber_id", "is_active"])
    op.create_index("idx_cora_suppression_reason", "cora_suppressions", ["reason"])


def downgrade() -> None:
    op.drop_index("idx_cora_suppression_reason", table_name="cora_suppressions")
    op.drop_index("idx_cora_suppression_active_sub", table_name="cora_suppressions")
    op.drop_table("cora_suppressions")
