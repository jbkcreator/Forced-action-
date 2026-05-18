"""add_referral_core_loop (no-op: DDL applied out-of-band)

referral_milestone_awards, referral_forward_copy, subscribers.bonus_zip_slots,
and the referral_events.status constraint were all applied directly to prod
before this revision was tracked by Alembic.

Revision ID: fa004_referral_core_loop
Revises:     fa003_phone_metadata
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op

revision = "fa004_referral_core_loop"
down_revision = "fa003_phone_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
