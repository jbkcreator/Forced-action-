"""fa054_subscriber_county_id_index

Revision ID: fa054
Revises: fa053
Create Date: 2026-05-29

Adds idx_subscriber_county_id on subscribers(county_id).
Without this index every county-scoped query (active count, churn, landing stats)
performs a full table scan on the subscribers table.
"""
from typing import Sequence, Union

from alembic import op

revision: str = 'fa054'
down_revision: Union[str, None] = 'fa053'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'idx_subscriber_county_id',
        'subscribers',
        ['county_id'],
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index('idx_subscriber_county_id', table_name='subscribers')
