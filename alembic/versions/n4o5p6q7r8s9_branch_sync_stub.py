"""branch_sync_stub

Stub migration to reconcile DB version from a prior branch.
The database was already at n4o5p6q7r8s9 when this branch was checked out;
this file restores the chain without touching any tables.

Revision ID: n4o5p6q7r8s9
Revises: m3n4o5p6q7r8
Create Date: 2026-04-21 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op

revision: str = "n4o5p6q7r8s9"
down_revision: Union[str, None] = "m3n4o5p6q7r8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
