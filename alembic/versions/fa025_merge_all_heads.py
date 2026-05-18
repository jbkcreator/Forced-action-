"""fa025 — merge all outstanding heads into a single linear tip

Merges:
  - fa024_county_catchup   (county schema catch-up)
  - fa022_sms_opt_out_source_default (sms_opt_outs.source DEFAULT)
  - fa018_phone_normalize_backfill   (phone E.164 backfill)
  - fa004_referral_core_loop         (no-op anchor for referral tables)

Revision ID: fa025_merge
Revises:     fa024_county_catchup,
             fa022_sms_opt_out_source_default,
             fa018_phone_normalize_backfill,
             fa004_referral_core_loop
Create Date: 2026-05-18
"""

from alembic import op

revision = "fa025_merge"
down_revision = (
    "fa024_county_catchup",
    "fa022_sms_opt_out_source_default",
    "fa018_phone_normalize_backfill",
    "fa004_referral_core_loop",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
