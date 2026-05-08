"""merge_fa011_and_u5v6

Revision ID: 97efacd72d79
Revises: fa011_phase_b, u5v6w7x8y9z0
Create Date: 2026-05-06 12:50:49.267126

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97efacd72d79'
down_revision: Union[str, Sequence[str], None] = ('fa011_phase_b', 'u5v6w7x8y9z0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
