"""fa068_icp_launch_window_and_killswitch

Adds 4-week launch window tracking and kill-switch decision support to
expansion_icp_channels:

  launch_started_at     — set when channel transitions to 'live'
  launch_ends_at        — launch_started_at + 28 days (4-week window)
  killswitch_decision   — final decision: keep | adjust | kill | NULL (pending)
  killswitch_reason     — free-text reason recorded with decision
  killswitch_decided_at — timestamp of decision
  killswitch_decided_by — actor who made the decision

Also extends icp_channel_launch_audit.event_type constraint to include
killswitch decision events.

Revision ID: fa068
Revises:     fa067
Create Date: 2026-06-02
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa068"
down_revision: Union[str, Sequence[str], None] = "fa067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Launch window columns on expansion_icp_channels ──────────────────
    op.add_column("expansion_icp_channels",
        sa.Column("launch_started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("expansion_icp_channels",
        sa.Column("launch_ends_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("expansion_icp_channels",
        sa.Column("killswitch_decision", sa.String(10), nullable=True))
    op.add_column("expansion_icp_channels",
        sa.Column("killswitch_reason", sa.Text, nullable=True))
    op.add_column("expansion_icp_channels",
        sa.Column("killswitch_decided_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("expansion_icp_channels",
        sa.Column("killswitch_decided_by", sa.String(100), nullable=True))

    op.create_check_constraint(
        "ck_expansion_icp_killswitch_decision",
        "expansion_icp_channels",
        "killswitch_decision IS NULL OR killswitch_decision IN ('keep','adjust','kill')",
    )

    # ── Extend audit event_type to include killswitch events ──────────────
    # Drop old constraint and recreate with extended list
    op.drop_constraint("ck_icp_audit_event_type", "icp_channel_launch_audit", type_="check")
    op.create_check_constraint(
        "ck_icp_audit_event_type",
        "icp_channel_launch_audit",
        "event_type IN ("
        "'activated','paused','killed','force_activated',"
        "'config_updated','gate_evaluated','created',"
        "'killswitch_keep','killswitch_adjust','killswitch_kill'"
        ")",
    )


def downgrade() -> None:
    op.drop_constraint("ck_icp_audit_event_type", "icp_channel_launch_audit", type_="check")
    op.create_check_constraint(
        "ck_icp_audit_event_type",
        "icp_channel_launch_audit",
        "event_type IN ("
        "'activated','paused','killed','force_activated',"
        "'config_updated','gate_evaluated','created'"
        ")",
    )

    op.drop_constraint("ck_expansion_icp_killswitch_decision", "expansion_icp_channels", type_="check")
    op.drop_column("expansion_icp_channels", "killswitch_decided_by")
    op.drop_column("expansion_icp_channels", "killswitch_decided_at")
    op.drop_column("expansion_icp_channels", "killswitch_reason")
    op.drop_column("expansion_icp_channels", "killswitch_decision")
    op.drop_column("expansion_icp_channels", "launch_ends_at")
    op.drop_column("expansion_icp_channels", "launch_started_at")
