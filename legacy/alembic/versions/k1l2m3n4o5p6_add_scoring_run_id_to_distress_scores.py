"""add_scoring_run_id_to_distress_scores

Revision ID: k1l2m3n4o5p6
Revises: j0k1l2m3n4o5
Create Date: 2026-04-02 00:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'k1l2m3n4o5p6'
down_revision: Union[str, None] = 'j0k1l2m3n4o5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'distress_scores',
        sa.Column('scoring_run_id', sa.Integer(), nullable=True),
    )
    op.create_index(
        'idx_score_scoring_run_id',
        'distress_scores',
        ['scoring_run_id'],
    )


def downgrade() -> None:
    op.drop_index('idx_score_scoring_run_id', table_name='distress_scores')
    op.drop_column('distress_scores', 'scoring_run_id')
