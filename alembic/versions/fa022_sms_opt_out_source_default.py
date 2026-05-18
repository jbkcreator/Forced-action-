"""fa022_sms_opt_out_source_default — flip SmsOptOut.source server default

Compliance baseline V7 paperwork. Switches the server-side DEFAULT on
sms_opt_outs.source from the vendor-tagged "twilio_inbound" to the
vendor-neutral "inbound_sms". Historical rows are not modified — they
keep "twilio_inbound" as a valid legacy value.

Revision ID: fa022_sms_opt_out_source_default
Revises:     fa021
Create Date: 2026-05-18
"""

from alembic import op


revision = "fa022_sms_opt_out_source_default"
down_revision = "fa021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE sms_opt_outs ALTER COLUMN source SET DEFAULT 'inbound_sms'"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE sms_opt_outs ALTER COLUMN source SET DEFAULT 'twilio_inbound'"
    )
