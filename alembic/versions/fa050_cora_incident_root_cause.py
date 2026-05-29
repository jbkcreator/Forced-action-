"""fa050 — add root_cause to cora_incident

Adds a nullable ``root_cause TEXT`` column to ``cora_incident`` so the daily
dashboard's Data Quality Alerts table can show a human-readable cause per
breach. Existing rows stay NULL; the dashboard derives a fallback from
``metric_name``/``county_id`` when the column is unset.

Revision ID: fa050_cora_incident_root_cause
Revises:     fa049_merge_all_heads
Create Date: 2026-05-29
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa050_cora_incident_root_cause"
down_revision: Union[str, Sequence[str], None] = "fa049_merge_all_heads"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "cora_incident",
        sa.Column("root_cause", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("cora_incident", "root_cause")
