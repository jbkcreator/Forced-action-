"""fa057_synthflow_inbound

Adds capture_complete to subscribers for the Synthflow inbound signup flow.
A row is marked capture_complete=False when the Synthflow webhook arrived
without a ZIP or vertical (Incomplete Capture); first-login corrects it.

Also adds a partial unique index on webhook_events(source, source_event_id)
for source='synthflow_inbound' to enforce call_id idempotency.

subscribers
  capture_complete  BOOLEAN  NOT NULL DEFAULT true

webhook_events
  uq_synthflow_inbound_event_id  UNIQUE (source, source_event_id)
                                 WHERE source = 'synthflow_inbound'
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa057_synthflow_inbound"
down_revision: Union[str, Sequence[str], None] = "fa056_deal_outcomes_county_vertical"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "subscribers",
        sa.Column("capture_complete", sa.Boolean(), server_default="true", nullable=False),
    )
    op.create_index(
        "uq_synthflow_inbound_event_id",
        "webhook_events",
        ["source", "source_event_id"],
        unique=True,
        postgresql_where=sa.text("source = 'synthflow_inbound'"),
    )


def downgrade() -> None:
    op.drop_index("uq_synthflow_inbound_event_id", table_name="webhook_events")
    op.drop_column("subscribers", "capture_complete")
