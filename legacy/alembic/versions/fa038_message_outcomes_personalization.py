"""fa038_message_outcomes_personalization

Adds 8 personalization columns to message_outcomes so Cora SMS variant
performance can be attributed by trade vertical, county, behavioral segment,
revenue signal score band, and last-action recency.

Columns added to message_outcomes:
  trade_vertical            VARCHAR(50)  — subscriber's trade (roofing, attorneys, …)
  county_id                 VARCHAR(50)  — county_id from the subscriber profile
  behavioral_segment        VARCHAR(30)  — 8-bucket segment at send time
  revenue_signal_score      INTEGER      — 0-100 score at send time
  revenue_signal_score_band VARCHAR(20)  — low/medium/high/very_high
  last_action_recency_band  VARCHAR(30)  — same_day/recent_1_3_days/cooling_4_7_days/stale_8_plus_days/unknown
  prompt_version            VARCHAR(20)  — template version tag (e.g. fomo_v2)
  context_snapshot          JSONB        — full render context at send time

All columns are nullable — existing rows remain valid.
Purely additive. Safe during business hours.

Revision ID: fa038_message_outcomes_personalization
Revises:     fa037_revenue_signal_audit
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa038_message_outcomes_personalization"
down_revision: Union[str, Sequence[str], None] = "fa037_revenue_signal_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("message_outcomes", sa.Column("trade_vertical", sa.String(50), nullable=True))
    op.add_column("message_outcomes", sa.Column("county_id", sa.String(50), nullable=True))
    op.add_column("message_outcomes", sa.Column("behavioral_segment", sa.String(30), nullable=True))
    op.add_column("message_outcomes", sa.Column("revenue_signal_score", sa.Integer, nullable=True))
    op.add_column("message_outcomes", sa.Column("revenue_signal_score_band", sa.String(20), nullable=True))
    op.add_column("message_outcomes", sa.Column("last_action_recency_band", sa.String(30), nullable=True))
    op.add_column("message_outcomes", sa.Column("prompt_version", sa.String(20), nullable=True))
    op.add_column("message_outcomes", sa.Column("context_snapshot", JSONB, nullable=True))
    op.create_index(
        "idx_msg_outcome_vertical_segment",
        "message_outcomes",
        ["trade_vertical", "behavioral_segment"],
    )


def downgrade() -> None:
    op.drop_index("idx_msg_outcome_vertical_segment", table_name="message_outcomes")
    op.drop_column("message_outcomes", "context_snapshot")
    op.drop_column("message_outcomes", "prompt_version")
    op.drop_column("message_outcomes", "last_action_recency_band")
    op.drop_column("message_outcomes", "revenue_signal_score_band")
    op.drop_column("message_outcomes", "revenue_signal_score")
    op.drop_column("message_outcomes", "behavioral_segment")
    op.drop_column("message_outcomes", "county_id")
    op.drop_column("message_outcomes", "trade_vertical")
