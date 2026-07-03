"""fa063_campaign_instantly_settings

Add email_campaigns.instantly_settings JSONB — holds Instantly-only campaign
knobs (daily_limit, daily_max_leads, email_list, stop_on_reply, open_tracking,
link_tracking) that are not modeled as typed columns. Edited via the campaign
PATCH endpoint and pushed to Instantly.

Purely additive (one nullable-with-default JSONB column). Safe during normal hours.

Revision ID: fa063
Revises:     fa062
Create Date: 2026-06-02
"""

from typing import Sequence, Union

import sqlalchemy as sa

revision: str = "fa063"
down_revision: Union[str, None] = "fa062"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade(conn) -> None:
    conn.execute(sa.text("""
        ALTER TABLE email_campaigns
            ADD COLUMN IF NOT EXISTS instantly_settings JSONB NOT NULL DEFAULT '{}'::jsonb
    """))


def downgrade(conn) -> None:
    conn.execute(sa.text("""
        ALTER TABLE email_campaigns
            DROP COLUMN IF EXISTS instantly_settings
    """))
