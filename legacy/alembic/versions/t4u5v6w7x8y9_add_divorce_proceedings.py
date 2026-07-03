"""add_divorce_to_legal_proceedings

Revision ID: t4u5v6w7x8y9
Revises: p7q8r9s0t1u2
Create Date: 2026-04-27

Extends legal_proceedings.record_type check constraint to allow 'Divorce'
in addition to the existing Probate / Eviction / Bankruptcy values.
"""
from alembic import op

# revision identifiers
revision = 't4u5v6w7x8y9'
down_revision = 'p7q8r9s0t1u2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE legal_proceedings DROP CONSTRAINT IF EXISTS check_proceeding_record_type")
    op.execute(
        "ALTER TABLE legal_proceedings ADD CONSTRAINT check_proceeding_record_type "
        "CHECK (record_type IN ('Probate', 'Eviction', 'Bankruptcy', 'Divorce'))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE legal_proceedings DROP CONSTRAINT IF EXISTS check_proceeding_record_type")
    op.execute(
        "ALTER TABLE legal_proceedings ADD CONSTRAINT check_proceeding_record_type "
        "CHECK (record_type IN ('Probate', 'Eviction', 'Bankruptcy'))"
    )
