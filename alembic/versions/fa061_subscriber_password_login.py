"""fa061_subscriber_password_login

Adds subscriber password-login columns so the lead feed can be token-gated
instead of UUID-only.

  password_hash            — bcrypt hash (NULL until the subscriber has a password)
  password_set_at          — when the password was last set
  reset_token_hash         — sha256 hex of a forgot-password token (raw is emailed)
  reset_token_expires_at   — reset token expiry

Purely additive, all nullable — existing subscribers keep NULL and regain access
via the self-serve forgot-password flow. Safe to apply during normal hours.

Revision ID: fa061
Revises:     fa060
Create Date: 2026-06-01
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa061"
down_revision: Union[str, Sequence[str], None] = "fa060"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("subscribers", sa.Column("password_hash", sa.String(length=255), nullable=True))
    op.add_column("subscribers", sa.Column("password_set_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("subscribers", sa.Column("reset_token_hash", sa.String(length=64), nullable=True))
    op.add_column("subscribers", sa.Column("reset_token_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("idx_subscribers_reset_token_hash", "subscribers", ["reset_token_hash"])


def downgrade() -> None:
    op.drop_index("idx_subscribers_reset_token_hash", table_name="subscribers")
    op.drop_column("subscribers", "reset_token_expires_at")
    op.drop_column("subscribers", "reset_token_hash")
    op.drop_column("subscribers", "password_set_at")
    op.drop_column("subscribers", "password_hash")
