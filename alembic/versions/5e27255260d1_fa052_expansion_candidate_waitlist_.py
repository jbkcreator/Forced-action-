"""fa052_expansion_candidate_waitlist_notified_at

Revision ID: 5e27255260d1
Revises: fa051_merge_open_heads
Create Date: 2026-05-29 15:59:10.437765

Adds waitlist_notified_at to expansion_candidates.
Stamped by county_waitlist_notifier after all waiting entries for a launched
county have been notified. Acts as county-level idempotency guard.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '5e27255260d1'
down_revision: Union[str, None] = 'fa051_merge_open_heads'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'expansion_candidates',
        sa.Column('waitlist_notified_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('expansion_candidates', 'waitlist_notified_at')
