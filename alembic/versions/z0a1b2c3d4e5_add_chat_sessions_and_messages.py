"""add_chat_sessions_and_messages

Revision ID: fa005_concierge_chat
Revises:     fa004_referral_core_loop
Create Date: 2026-05-25
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from alembic import op

revision = "fa005_concierge_chat"
down_revision = "fa004_referral_core_loop"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # chat_sessions
    exists = conn.execute(sa.text(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='chat_sessions')"
    )).scalar()
    if not exists:
        op.create_table(
            "chat_sessions",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
            sa.Column("anonymous_id", sa.String(36), nullable=True),
            sa.Column("source", sa.String(20), nullable=False, server_default="landing"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("last_seen_at", sa.DateTime(), nullable=False),
            sa.Column("linked_at", sa.DateTime(), nullable=True),
            sa.CheckConstraint(
                "source IN ('landing', 'dashboard', 'lead_feed')",
                name="check_chat_session_source",
            ),
        )
        op.create_index("idx_chat_session_subscriber", "chat_sessions", ["subscriber_id", "created_at"])
        op.create_index("idx_chat_session_anon", "chat_sessions", ["anonymous_id", "created_at"])

    # chat_messages
    exists = conn.execute(sa.text(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='chat_messages')"
    )).scalar()
    if not exists:
        op.create_table(
            "chat_messages",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("session_id", sa.String(36), sa.ForeignKey("chat_sessions.id"), nullable=False),
            sa.Column("role", sa.String(10), nullable=False),
            sa.Column("content", sa.Text(), nullable=True),
            sa.Column("intent_label", sa.String(40), nullable=True),
            sa.Column("intent_confidence", sa.Numeric(4, 3), nullable=True),
            sa.Column("tool_calls_json", postgresql.JSONB(), nullable=True),
            sa.Column("payment_trigger_json", postgresql.JSONB(), nullable=True),
            sa.Column("claude_model", sa.String(20), nullable=True),
            sa.Column("tokens_in", sa.Integer(), nullable=True),
            sa.Column("tokens_out", sa.Integer(), nullable=True),
            sa.Column("latency_ms", sa.Integer(), nullable=True),
            sa.Column("error", sa.String(200), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.CheckConstraint(
                "role IN ('user', 'assistant', 'system', 'tool')",
                name="check_chat_message_role",
            ),
        )
        op.create_index("idx_chat_message_session_created", "chat_messages", ["session_id", "created_at"])
        op.create_index("idx_chat_message_intent", "chat_messages", ["intent_label", "created_at"])


def downgrade() -> None:
    op.drop_table("chat_messages")
    op.drop_table("chat_sessions")
