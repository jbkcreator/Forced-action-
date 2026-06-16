"""Stub for fa078_tracerfy_trace_type — DDL was applied directly to DB, file was never committed.

Revision ID: fa078_tracerfy_trace_type
Revises: (none — orphan stub; DDL already in DB, no upgrade needed)

This file exists solely so Alembic can build its revision map.
The upgrade/downgrade functions are no-ops.
"""
from typing import Sequence, Union

revision: str = "fa078_tracerfy_trace_type"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
