"""merge_fa010_and_divorce_branch

Revision ID: 2ad0c9f6597c
Revises: fa010_county_config_tables, u5v6w7x8y9z0
Create Date: 2026-05-06 15:57:33.417543

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2ad0c9f6597c'
down_revision: Union[str, Sequence[str], None] = ('fa010_county_config_tables', 'u5v6w7x8y9z0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
