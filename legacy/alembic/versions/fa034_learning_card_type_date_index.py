"""add composite index on learning_cards(card_type, card_date DESC)

Supports the get_learning_card() hot path:
    SELECT … WHERE card_type = ? ORDER BY card_date DESC LIMIT 1

Existing idx_learning_card_date stays — used by date-range scans.

Revision ID: fa034_learning_card_type_date_index
Revises: o5p6q7r8s9t0
Create Date: 2026-05-25
"""

from alembic import op


revision = 'fa034_learning_card_type_date_index'
down_revision = 'o5p6q7r8s9t0'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_learning_card_type_date "
        "ON learning_cards (card_type, card_date DESC)"
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS idx_learning_card_type_date")
