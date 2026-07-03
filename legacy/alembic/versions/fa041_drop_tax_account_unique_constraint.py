"""fa041_drop_tax_account_unique_constraint

Revision ID: fa041_drop_tax_account_unique
Revises: a2ef1375cd79
Create Date: 2026-05-26

The unique constraint on (source_account_number, county_id) is wrong —
Hillsborough account numbers are A+folio and repeat across tax years for the
same property. Deduplication is already enforced by uq_tax_delinquency_property_year
(property_id, tax_year). The index is kept for lookup speed.
"""
from typing import Sequence, Union
from alembic import op

revision: str = 'fa041_drop_tax_account_unique'
down_revision: Union[str, Sequence[str], None] = 'a2ef1375cd79'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('uq_tax_delinquency_account_county', 'tax_delinquencies', type_='unique')


def downgrade() -> None:
    op.create_unique_constraint(
        'uq_tax_delinquency_account_county',
        'tax_delinquencies', ['source_account_number', 'county_id'],
    )
