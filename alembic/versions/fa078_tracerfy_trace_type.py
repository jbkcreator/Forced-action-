"""Add trace_type to enriched_contacts and enrichment_usage_logs

Revision ID: fa078_tracerfy_trace_type
Revises: fa077_master_weekly_refresh
Create Date: 2026-06-11
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "fa078_tracerfy_trace_type"
down_revision: Union[str, Sequence[str], None] = "fa077_master_weekly_refresh"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("enriched_contacts", sa.Column("trace_type", sa.String(20), nullable=True))
    op.create_index("idx_ec_trace_type", "enriched_contacts", ["trace_type"])


def downgrade() -> None:
    op.drop_index("idx_ec_trace_type", table_name="enriched_contacts")
    op.drop_column("enriched_contacts", "trace_type")
