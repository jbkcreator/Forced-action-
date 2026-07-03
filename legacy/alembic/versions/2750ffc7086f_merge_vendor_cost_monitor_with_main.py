"""merge_vendor_cost_monitor_with_main

Revision ID: 2750ffc7086f
Revises: b4c8ae3057fa, vendor_cost_pause_monitor
Create Date: 2026-05-27 10:56:17.554122

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2750ffc7086f'
down_revision: Union[str, Sequence[str], None] = ('b4c8ae3057fa', 'vendor_cost_pause_monitor')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
