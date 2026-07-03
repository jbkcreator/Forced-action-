"""Add sms_send_logs audit table (no-op: table applied out-of-band).

sms_send_logs was created directly on prod before Alembic tracked this branch.
This revision anchors the history record.

Revision ID: fa017_sms_send_logs
Revises: fa020_dlq_reason_widen
Create Date: 2026-05-14
"""

from alembic import op
import sqlalchemy as sa

revision = "fa017_sms_send_logs"
down_revision = "fa020_dlq_reason_widen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
