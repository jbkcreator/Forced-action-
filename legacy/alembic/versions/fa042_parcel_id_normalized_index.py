"""fa042_parcel_id_normalized_index

Revision ID: fa042_parcel_id_normalized
Revises: fa041_drop_tax_account_unique
Create Date: 2026-05-26

Function-based index on regexp_replace(parcel_id, '[^A-Za-z0-9]', '', 'g')
so separator-agnostic parcel matching (slash vs hyphen) hits the index
instead of doing a full table scan on the 522k-row properties table.
"""
from typing import Sequence, Union
from alembic import op

revision: str = 'fa042_parcel_id_normalized'
down_revision: Union[str, Sequence[str], None] = 'fa041_drop_tax_account_unique'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_property_parcel_id_normalized
            ON properties (regexp_replace(parcel_id, '[^A-Za-z0-9]', '', 'g'), county_id)
        """)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_property_parcel_id_normalized")
