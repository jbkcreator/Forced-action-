"""add_is_enforcement_permit_to_building_permits

Revision ID: i9j0k1l2m3n4
Revises: h8i9j0k1l2m3
Create Date: 2026-03-29 00:00:00.000000

Adds is_enforcement_permit boolean column to building_permits.
Tags stop work orders, after-the-fact permits, failed/expired/revoked/suspended
permits for higher CDS scoring weight vs routine building permits.

All existing rows default to FALSE — run the backfill SQL below after upgrading
to tag existing enforcement permits:

    UPDATE building_permits SET is_enforcement_permit = TRUE
    WHERE
        permit_type LIKE '%Code Compliance Case%'
        OR status IN ('Withdrawn', 'Cancel');
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'i9j0k1l2m3n4'
down_revision: Union[str, None] = 'h8i9j0k1l2m3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'building_permits',
        sa.Column(
            'is_enforcement_permit',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_index(
        'idx_building_permits_is_enforcement',
        'building_permits',
        ['is_enforcement_permit'],
    )


def downgrade() -> None:
    op.drop_index('idx_building_permits_is_enforcement', table_name='building_permits')
    op.drop_column('building_permits', 'is_enforcement_permit')
