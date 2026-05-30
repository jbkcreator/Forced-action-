"""fa050_expansion_icp_channels — applied out-of-band; stub anchors history.

Adds expansion_icp_channels table with REI Investor seed row.
DDL applied via scripts/apply_fa050_ddl.py.

Revision ID: fa050_expansion_icp_channels
Revises:     fa049_merge_all_heads
Create Date: 2026-05-30
"""

from typing import Sequence, Union

revision: str = "fa050_expansion_icp_channels"
down_revision: Union[str, Sequence[str], None] = "fa049_merge_all_heads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
