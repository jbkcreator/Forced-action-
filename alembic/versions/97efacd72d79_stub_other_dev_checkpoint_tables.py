"""97efacd72d79_stub_other_dev_checkpoint_tables

Stub migration representing changes applied by another developer on a separate
branch. The DB already has this stamp applied. This file exists purely so Alembic
can traverse the migration chain — the upgrade/downgrade bodies are intentionally
empty because the schema changes are already present in the database.

If you need to reproduce the downgrade path, recover the original migration file
from the branch where this was authored.

Revision ID: 97efacd72d79
Revises:     fa009_annual_lock_tier
Create Date: 2026-05-06
"""
from alembic import op

revision = '97efacd72d79'
down_revision = 'fa009_annual_lock_tier'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass  # already applied to DB


def downgrade() -> None:
    pass  # no-op — recover from original branch if needed
