"""stage6: add subscriber payment_failed_at + recovery flags

Revision ID: fa012_stage6
Revises: 97efacd72d79
Create Date: 2026-05-08
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = 'fa012_stage6'
down_revision: str | None = '2ad0c9f6597c'
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column('subscribers', sa.Column('payment_failed_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('recovery_day1_sent', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('subscribers', sa.Column('recovery_day3_sent', sa.Boolean(), server_default='false', nullable=False))


def downgrade() -> None:
    op.drop_column('subscribers', 'recovery_day3_sent')
    op.drop_column('subscribers', 'recovery_day1_sent')
    op.drop_column('subscribers', 'payment_failed_at')
