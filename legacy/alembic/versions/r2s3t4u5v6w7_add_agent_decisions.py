"""add_agent_decisions

Creates the agent_decisions table — audit log for every Cora graph run.
One row per decision (not per node). Written by the log_decision tool.

Revision ID: r2s3t4u5v6w7
Revises:     q1r2s3t4u5v6
Create Date: 2026-04-23
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "r2s3t4u5v6w7"
down_revision = "q1r2s3t4u5v6"
branch_labels = None
depends_on = None


def upgrade() -> None:
	op.create_table(
		"agent_decisions",
		sa.Column("decision_id", sa.String(36), primary_key=True),
		sa.Column("graph_name", sa.String(60), nullable=False),
		sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
		sa.Column("event_type", sa.String(60), nullable=True),
		sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
		sa.Column("completed_at", sa.DateTime(), nullable=True),
		sa.Column("terminal_status", sa.String(20), nullable=True),
		sa.Column("tokens_used", sa.Integer(), nullable=False, server_default="0"),
		sa.Column("cost_usd", sa.Numeric(10, 6), nullable=False, server_default="0"),
		sa.Column("summary", JSONB(), nullable=True),
		sa.CheckConstraint(
			"terminal_status IS NULL OR terminal_status IN ('completed', 'aborted', 'escalated', 'failed')",
			name="check_agent_terminal_status",
		),
	)
	op.create_index("ix_agent_decisions_graph_name", "agent_decisions", ["graph_name"])
	op.create_index("ix_agent_decisions_subscriber_id", "agent_decisions", ["subscriber_id"])
	op.create_index("ix_agent_decisions_event_type", "agent_decisions", ["event_type"])
	op.create_index("ix_agent_decisions_started_at", "agent_decisions", ["started_at"])
	op.create_index(
		"idx_agent_decisions_graph_started",
		"agent_decisions",
		["graph_name", "started_at"],
	)


def downgrade() -> None:
	op.drop_index("idx_agent_decisions_graph_started", table_name="agent_decisions")
	op.drop_index("ix_agent_decisions_started_at", table_name="agent_decisions")
	op.drop_index("ix_agent_decisions_event_type", table_name="agent_decisions")
	op.drop_index("ix_agent_decisions_subscriber_id", table_name="agent_decisions")
	op.drop_index("ix_agent_decisions_graph_name", table_name="agent_decisions")
	op.drop_table("agent_decisions")
