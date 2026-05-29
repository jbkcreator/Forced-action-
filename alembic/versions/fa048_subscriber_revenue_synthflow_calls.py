"""fa048_subscriber_revenue_synthflow_calls

Adds revenue-tracking columns to subscribers and creates the synthflow_calls
table so the dashboard can show real MRR, churn rate, trial metrics, and
Synthflow booking rate.

subscribers
  plan_price     NUMERIC(10,2)  nullable  — monthly recurring charge (USD)
  churned_at     TIMESTAMPTZ    nullable  — when status transitioned to 'churned'
  is_trial       BOOLEAN        NOT NULL DEFAULT false
  trial_ends_at  TIMESTAMPTZ    nullable

synthflow_calls
  id             SERIAL PK
  prospect_phone VARCHAR(20)    NOT NULL
  outcome        VARCHAR(50)    nullable  — sample_requested|demo_requested|not_interested|voicemail|no_answer|completed
  vertical       VARCHAR(50)    nullable
  zip_code       VARCHAR(10)    nullable
  contact_id     VARCHAR(100)   nullable  — GHL contact id
  call_date      DATE           NOT NULL
  created_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fa048_subscriber_revenue_synthflow"
down_revision: Union[str, Sequence[str], None] = "fa047_cora_message_hold_review"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── subscribers columns ───────────────────────────────────────────────────
    op.add_column("subscribers", sa.Column("plan_price", sa.Numeric(10, 2), nullable=True))
    op.add_column("subscribers", sa.Column("churned_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("subscribers", sa.Column("is_trial", sa.Boolean(), server_default="false", nullable=False))
    op.add_column("subscribers", sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True))

    op.create_index("idx_subscribers_churned_at", "subscribers", ["churned_at"])
    op.create_index("idx_subscribers_is_trial", "subscribers", ["is_trial"])

    # ── synthflow_calls table ─────────────────────────────────────────────────
    op.create_table(
        "synthflow_calls",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("prospect_phone", sa.String(length=20), nullable=False),
        sa.Column("outcome", sa.String(length=50), nullable=True),
        sa.Column("vertical", sa.String(length=50), nullable=True),
        sa.Column("zip_code", sa.String(length=10), nullable=True),
        sa.Column("contact_id", sa.String(length=100), nullable=True),
        sa.Column("call_date", sa.Date(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_synthflow_calls_call_date", "synthflow_calls", ["call_date"])
    op.create_index("idx_synthflow_calls_outcome", "synthflow_calls", ["outcome"])
    op.create_index("idx_synthflow_calls_prospect_phone", "synthflow_calls", ["prospect_phone"])


def downgrade() -> None:
    op.drop_table("synthflow_calls")
    op.drop_index("idx_subscribers_is_trial", table_name="subscribers")
    op.drop_index("idx_subscribers_churned_at", table_name="subscribers")
    op.drop_column("subscribers", "trial_ends_at")
    op.drop_column("subscribers", "is_trial")
    op.drop_column("subscribers", "churned_at")
    op.drop_column("subscribers", "plan_price")
