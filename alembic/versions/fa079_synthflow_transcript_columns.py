"""synthflow_calls transcript columns for IVR learning capture

Revision ID: fa079_synthflow_transcript_columns
Revises: fa078_tracerfy_trace_type
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa079_synthflow_transcript_columns"
down_revision: Union[str, None] = "fa078_tracerfy_trace_type"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("synthflow_calls", sa.Column("call_id", sa.String(100), nullable=True))
    op.add_column("synthflow_calls", sa.Column("transcript_text", sa.Text, nullable=True))
    op.add_column("synthflow_calls", sa.Column("recording_url", sa.String(500), nullable=True))
    op.add_column("synthflow_calls", sa.Column("duration_seconds", sa.Integer, nullable=True))
    # UNIQUE constraint creates an implicit unique index in PostgreSQL — no separate create_index needed.
    op.create_unique_constraint("uq_synthflow_calls_call_id", "synthflow_calls", ["call_id"])


def downgrade() -> None:
    op.drop_constraint("uq_synthflow_calls_call_id", "synthflow_calls", type_="unique")
    op.drop_column("synthflow_calls", "duration_seconds")
    op.drop_column("synthflow_calls", "recording_url")
    op.drop_column("synthflow_calls", "transcript_text")
    op.drop_column("synthflow_calls", "call_id")
