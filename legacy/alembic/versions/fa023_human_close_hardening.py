"""fa023 — human_close hardening (no-op: columns applied out-of-band)

posted_at, post_attempts, last_post_error, target_tier_price_cents, vertical,
and idx_hce_retry were applied directly to prod before Alembic tracked this
branch. This revision just anchors the history record.

Revision ID: fa023_human_close
Revises:     fa022
Create Date: 2026-05-18
"""

from alembic import op

revision = "fa023_human_close"
down_revision = "fa022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # All DDL already applied out-of-band; no-op to anchor the revision.
    pass


def downgrade() -> None:
    pass
