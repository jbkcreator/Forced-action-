"""fa009_annual_lock_tier

Add 'annual_lock' to the check_subscriber_tier constraint so that
/api/annual/accept can flip a subscriber's tier without hitting a
CheckViolation on commit.

Revision ID: fa009_annual_lock_tier
Revises:     fa008_enrichment_usage_logs
Create Date: 2026-05-05
"""
from alembic import op

revision = 'fa009_annual_lock_tier'
down_revision = 'fa008_enrichment_usage_logs'
branch_labels = None
depends_on = None


_OLD = "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner')"
_NEW = "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner', 'annual_lock')"


def upgrade() -> None:
    op.drop_constraint('check_subscriber_tier', 'subscribers', type_='check')
    op.create_check_constraint('check_subscriber_tier', 'subscribers', _NEW)


def downgrade() -> None:
    op.drop_constraint('check_subscriber_tier', 'subscribers', type_='check')
    op.create_check_constraint('check_subscriber_tier', 'subscribers', _OLD)
