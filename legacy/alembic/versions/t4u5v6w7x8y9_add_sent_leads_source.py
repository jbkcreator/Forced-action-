"""add_sent_leads_source

Adds the `source` column to `sent_leads` so the lead-unlock webhook can
record how a lead reached a subscriber (daily_email vs lead_unlock_payment
vs lead_pack_payment). Without it the webhook's `SentLead(source=...)`
call raises and gets swallowed, leaving paid unlocks invisible to the feed.

Revision ID: fa001_sentleads_source
Revises:     s3t4u5v6w7x8
Create Date: 2026-04-28
"""

import sqlalchemy as sa
from alembic import op

revision = "fa001_sentleads_source"
down_revision = "s3t4u5v6w7x8"
branch_labels = None
depends_on = None


def upgrade() -> None:
	op.add_column(
		"sent_leads",
		sa.Column("source", sa.String(40), nullable=True),
	)
	op.create_index("idx_sent_leads_source", "sent_leads", ["source"])


def downgrade() -> None:
	op.drop_index("idx_sent_leads_source", table_name="sent_leads")
	op.drop_column("sent_leads", "source")
