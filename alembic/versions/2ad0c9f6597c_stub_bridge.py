"""stub bridge for orphaned DB revision

Revision ID: 2ad0c9f6597c
Revises: 97efacd72d79
Create Date: 2026-05-08

This file exists only to reconcile an orphaned revision in the alembic_version
table. The revision was applied to the DB but its migration file was not
committed. All schema changes it represented are already in the DB.
No DDL operations needed.
"""
from __future__ import annotations

from alembic import op

revision: str = '2ad0c9f6597c'
down_revision: str | None = '97efacd72d79'
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


def upgrade() -> None:
    pass  # Schema already applied; stub for alembic history continuity


def downgrade() -> None:
    pass
