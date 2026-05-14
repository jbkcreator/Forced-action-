"""merge fa017 signup_source + owner_manager_fields

Revision ID: 5b3cff139150
Revises: fa017_signup_source, fa017_owner_manager_fields
Create Date: 2026-05-13 18:47:51.151735

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5b3cff139150'
down_revision: Union[str, Sequence[str], None] = ('fa017_signup_source', 'fa017_owner_manager_fields')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
