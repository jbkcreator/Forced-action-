"""quora_questions post-tracking columns

Revision ID: fa082_quora_post_tracking
Revises: fa081_quora_questions
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa082_quora_post_tracking"
down_revision: Union[str, Sequence[str]] = "fa081_quora_questions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("quora_questions", sa.Column("post_attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("quora_questions", sa.Column("error_log", sa.Text(), nullable=True))
    op.add_column("quora_questions", sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("quora_questions", "posted_at")
    op.drop_column("quora_questions", "error_log")
    op.drop_column("quora_questions", "post_attempts")
