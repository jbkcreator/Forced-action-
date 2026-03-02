"""add_vertical_scores_to_distress_scores

Revision ID: 5ac36f946603
Revises: f33a46e27984
Create Date: 2026-03-02 07:23:39.288150

Adds vertical_scores JSONB column to distress_scores to store
per-buyer-vertical CDS scores from the multi-vertical scoring engine.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '5ac36f946603'
down_revision: Union[str, Sequence[str], None] = 'f33a46e27984'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'distress_scores',
        sa.Column('vertical_scores', postgresql.JSONB(), nullable=True),
    )
    op.create_index(
        'ix_distress_scores_vertical_scores',
        'distress_scores',
        ['vertical_scores'],
        postgresql_using='gin',
    )


def downgrade() -> None:
    op.drop_index('ix_distress_scores_vertical_scores', table_name='distress_scores')
    op.drop_column('distress_scores', 'vertical_scores')
