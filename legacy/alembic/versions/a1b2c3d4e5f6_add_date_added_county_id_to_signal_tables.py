"""add_date_added_county_id_to_signal_tables

Revision ID: a1b2c3d4e5f6
Revises: 5ac36f946603
Create Date: 2026-03-03

Adds two fields to all 8 distress signal tables:
  - date_added (Date): calendar date the record was loaded — enables today/yesterday filtering
  - county_id  (String): county source identifier — enables multi-county routing

Backfills existing rows: date_added = CURRENT_DATE, county_id = 'hillsborough'
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '5ac36f946603'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = [
    'code_violations',
    'legal_and_liens',
    'deeds',
    'legal_proceedings',
    'tax_delinquencies',
    'foreclosures',
    'building_permits',
    'incidents',
]


def upgrade() -> None:
    for table in TABLES:
        op.add_column(table, sa.Column('date_added', sa.Date(), nullable=True))
        op.create_index(f'idx_{table}_date_added', table, ['date_added'])

        op.add_column(table, sa.Column('county_id', sa.String(50), nullable=True))
        op.create_index(f'idx_{table}_county_id', table, ['county_id'])

        # Backfill existing rows
        op.execute(f"UPDATE {table} SET date_added = CURRENT_DATE WHERE date_added IS NULL")
        op.execute(f"UPDATE {table} SET county_id = 'hillsborough' WHERE county_id IS NULL")


def downgrade() -> None:
    for table in TABLES:
        op.drop_index(f'idx_{table}_county_id', table_name=table)
        op.drop_column(table, 'county_id')
        op.drop_index(f'idx_{table}_date_added', table_name=table)
        op.drop_column(table, 'date_added')
