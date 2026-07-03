"""fa051 — merge open heads into a single linear tip

Collapses the outstanding heads that accumulated after fa049:
  - a1b2c3_timeline_idx              (agent_decisions timeline index)
  - fa040_waitlist_entries           (waitlist_entries table)
  - fa045_operator_crm               (operator CRM tables)
  - fa048_cora_suppressions          (cora_suppressions table)
  - fa050_cora_incident_root_cause   (cora_incident.root_cause column)

No schema operations — topology only — so `alembic upgrade head` resolves to
a single tip and applies every branch (including fa050) cleanly.

Revision ID: fa051_merge_open_heads
Create Date: 2026-05-29
"""
from typing import Sequence, Union

from alembic import op  # noqa: F401  (imported for parity with sibling migrations)


revision: str = "fa051_merge_open_heads"
down_revision: Union[str, Sequence[str], None] = (
    "a1b2c3_timeline_idx",
    "fa040_waitlist_entries",
    "fa045_operator_crm",
    "fa048_cora_suppressions",
    "fa050_cora_incident_root_cause",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
