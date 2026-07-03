"""fa035_widen_baseline_metric_cols

Widens the 7 percent-metric columns on `platform_daily_stats` from
NUMERIC(6, 4) to NUMERIC(7, 4).

fa034 added these columns at NUMERIC(6, 4), which only allows absolute
values < 100 — but the metrics are stored as percent in 0-100 range (e.g.
retention_30d=100.0, first_payment_rate=68.8 — see
src/tasks/kill_switch_metric_ingest.py). NUMERIC(7, 4) admits up to
999.9999, comfortable headroom while preserving 4 decimal-place precision.

`cac_paid_channels` is already NUMERIC(10, 2) — dollars — and unaffected.

ALTER COLUMN TYPE NUMERIC(7,4) is a metadata-only change when the new
precision/scale are a strict superset of the old, so this is fast on a
populated table and safe to run during normal hours.

Revision ID: fa035_widen_baseline_metric_cols
Revises:     fa034_cora_incident
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa035_widen_baseline_metric_cols"
down_revision: Union[str, Sequence[str], None] = "fa034_cora_incident"
branch_labels = None
depends_on = None


_PERCENT_COLUMNS = [
    "sms_reply_rate",
    "offer_acceptance_rate",
    "first_payment_rate",
    "saved_card_rate",
    "wallet_adoption",
    "lock_conversion",
    "retention_30d",
]


def upgrade() -> None:
    for col in _PERCENT_COLUMNS:
        op.alter_column(
            "platform_daily_stats", col,
            existing_type=sa.Numeric(6, 4),
            type_=sa.Numeric(7, 4),
            existing_nullable=True,
        )


def downgrade() -> None:
    # Going BACK requires confirming no values exceed 99.9999 — guard with a
    # check, otherwise the ALTER would fail mid-flight anyway.
    for col in _PERCENT_COLUMNS:
        op.execute(sa.text(
            f"DELETE FROM platform_daily_stats WHERE {col} >= 100"
        ))
        op.alter_column(
            "platform_daily_stats", col,
            existing_type=sa.Numeric(7, 4),
            type_=sa.Numeric(6, 4),
            existing_nullable=True,
        )
