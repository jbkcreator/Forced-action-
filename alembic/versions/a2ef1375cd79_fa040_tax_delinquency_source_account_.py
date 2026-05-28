"""fa040_tax_delinquency_source_account_number

Revision ID: a2ef1375cd79
Revises: fa039_remove_sandbox_outbox
Create Date: 2026-05-26 12:42:44.941782

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a2ef1375cd79'
down_revision: Union[str, Sequence[str], None] = 'fa039_remove_sandbox_outbox'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tax_delinquencies',
        sa.Column('source_account_number', sa.String(length=50), nullable=True)
    )
    op.create_index(
        'ix_tax_delinquencies_source_account_number',
        'tax_delinquencies', ['source_account_number'],
    )
    op.create_unique_constraint(
        'uq_tax_delinquency_account_county',
        'tax_delinquencies', ['source_account_number', 'county_id'],
    )


def downgrade() -> None:
    op.drop_constraint('uq_tax_delinquency_account_county', 'tax_delinquencies', type_='unique')
    op.drop_index('ix_tax_delinquencies_source_account_number', table_name='tax_delinquencies')
    op.drop_column('tax_delinquencies', 'source_account_number')
