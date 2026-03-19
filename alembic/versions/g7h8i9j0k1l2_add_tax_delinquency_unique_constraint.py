"""add_tax_delinquency_unique_constraint

Revision ID: g7h8i9j0k1l2
Revises: f6a7b8c9d0e1
Create Date: 2026-03-19 00:00:00.000000

Adds a unique constraint on (property_id, tax_year) to the tax_delinquencies
table so that duplicate delinquency records for the same property-year pair
are rejected at the DB level (not just app level).
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'g7h8i9j0k1l2'
down_revision: Union[str, None] = 'f6a7b8c9d0e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Remove duplicate rows first (keep the row with the lowest id for each pair)
    op.execute(
        """
        DELETE FROM tax_delinquencies
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM tax_delinquencies
            GROUP BY property_id, tax_year
        )
        """
    )
    op.create_unique_constraint(
        'uq_tax_delinquency_property_year',
        'tax_delinquencies',
        ['property_id', 'tax_year'],
    )


def downgrade() -> None:
    op.drop_constraint('uq_tax_delinquency_property_year', 'tax_delinquencies', type_='unique')
