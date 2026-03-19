"""expand_sync_status_constraint_for_ghl_decoupling

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-03-19 00:00:00.000000

Adds 'pending_sync' and 'sync_failed' to the check_sync_status constraint
on the properties table to support the decoupled GHL sync task.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'f6a7b8c9d0e1'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop and recreate — PostgreSQL doesn't support ALTER CONSTRAINT
    op.drop_constraint('check_sync_status', 'properties', type_='check')
    op.create_check_constraint(
        'check_sync_status',
        'properties',
        "sync_status IN ('pending', 'pending_sync', 'synced', 'sync_failed', 'error')",
    )


def downgrade() -> None:
    # First reset any rows that use the new values back to 'pending'
    op.execute(
        "UPDATE properties SET sync_status = 'pending' "
        "WHERE sync_status IN ('pending_sync', 'sync_failed')"
    )
    op.drop_constraint('check_sync_status', 'properties', type_='check')
    op.create_check_constraint(
        'check_sync_status',
        'properties',
        "sync_status IN ('pending', 'synced', 'error')",
    )
