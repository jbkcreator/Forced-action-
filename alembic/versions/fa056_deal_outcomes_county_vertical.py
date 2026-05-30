"""fa056_deal_outcomes_county_vertical

Adds county_id and trade_vertical to deal_outcomes for Stage 10 pricing
cohort activation gates.

The activation gate in pricing_cohort_engine.py counts distinct weeks of
closed_won deals per (county_id, trade_vertical) to decide when per-trade
pricing adjustments can activate. Without these columns the gate can't be
evaluated per county/vertical.

Purely additive — nullable columns, no backfill required.
Existing rows get NULL (treated as unclassified by the gate query).

Revision ID: fa056
Revises:     fa055
Create Date: 2026-05-30
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa056"
down_revision: Union[str, Sequence[str], None] = "fa055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("deal_outcomes", sa.Column("county_id", sa.String(50), nullable=True))
    op.add_column("deal_outcomes", sa.Column("trade_vertical", sa.String(50), nullable=True))
    op.create_index(
        "idx_deal_outcomes_county_vertical",
        "deal_outcomes",
        ["county_id", "trade_vertical"],
    )


def downgrade() -> None:
    op.drop_index("idx_deal_outcomes_county_vertical", table_name="deal_outcomes")
    op.drop_column("deal_outcomes", "trade_vertical")
    op.drop_column("deal_outcomes", "county_id")
