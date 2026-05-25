"""merge_sunbiz_skiptrace_with_retune_migrations

Revision ID: b4c8ae3057fa
Revises: fa031_sunbiz_owner_extension, fa031_skip_trace_waterfall, fa033_shadow_composite_index
Create Date: 2026-05-25 14:58:50.902541

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b4c8ae3057fa'
down_revision: Union[str, Sequence[str], None] = ('fa031_sunbiz_owner_extension', 'fa031_skip_trace_waterfall', 'fa033_shadow_composite_index')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
