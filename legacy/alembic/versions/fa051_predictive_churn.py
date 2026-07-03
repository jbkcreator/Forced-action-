"""fa051 — Predictive Churn: churn_predictions table + user_segments columns.

Revision ID: fa051_predictive_churn
Revises: fa050_expansion_icp_channels
Create Date: 2026-05-30

DDL applied out-of-band via scripts/apply_fa051_ddl.py.
This file anchors the Alembic revision history only.
"""

revision = "fa051_predictive_churn"
down_revision = "fa050_expansion_icp_channels"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
