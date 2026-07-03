"""add_match_confidence_and_method_to_legal_and_liens

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-03-19 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add match_confidence and match_method columns to legal_and_liens.

    ADD COLUMN ... nullable=True is a metadata-only operation in PostgreSQL 11+
    (instant, no table rewrite).
    """
    op.add_column(
        'legal_and_liens',
        sa.Column('match_confidence', sa.Integer(), nullable=True,
                  comment='Rapidfuzz score 0-100 at time of property match'),
    )
    op.add_column(
        'legal_and_liens',
        sa.Column('match_method', sa.String(30), nullable=True,
                  comment='How the property was matched: legal_desc | owner_name | llm_verified | address | manual'),
    )
    op.create_index('idx_legal_match_method', 'legal_and_liens', ['match_method'])
    op.create_check_constraint(
        'check_legal_match_method',
        'legal_and_liens',
        "match_method IN ('legal_desc', 'owner_name', 'llm_verified', 'address', 'manual')",
    )


def downgrade() -> None:
    """
    Remove match_confidence and match_method columns.

    PostgreSQL does not support ALTER CONSTRAINT — drop-and-recreate pattern
    (matches the approach used in d4e5f6a7b8c9 migration).
    """
    op.drop_constraint('check_legal_match_method', 'legal_and_liens', type_='check')
    op.drop_index('idx_legal_match_method', table_name='legal_and_liens')
    op.drop_column('legal_and_liens', 'match_method')
    op.drop_column('legal_and_liens', 'match_confidence')
