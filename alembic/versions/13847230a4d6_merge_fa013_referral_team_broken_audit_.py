"""merge fa013_referral_team_broken_audit into main lineage

Revision ID: 13847230a4d6
Revises: 5b3cff139150, fa013_referral_team_broken_audit
Create Date: 2026-05-13 18:48:27.324504

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '13847230a4d6'
down_revision: Union[str, Sequence[str], None] = ('5b3cff139150', 'fa013_referral_team_broken_audit')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
