"""A3: scoring_weight_overrides table for warm-start priors & heuristic tuning.

Revision ID: fa_a3_scoring_weight_overrides
Revises: fa_a1_loss_autopsies
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa_a3_scoring_weight_overrides"
down_revision: Union[str, Sequence[str], None] = "fa_a1_loss_autopsies"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS scoring_weight_overrides (
            id                SERIAL PRIMARY KEY,
            vertical          VARCHAR(50)  NOT NULL,
            signal_type       VARCHAR(50)  NOT NULL,
            delta             NUMERIC(6,2) NOT NULL DEFAULT 0,
            source            VARCHAR(30)  NOT NULL DEFAULT 'seed',
            reason            TEXT,
            enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
            loss_sample_count INT          NOT NULL DEFAULT 0,
            win_sample_count  INT          NOT NULL DEFAULT 0,
            created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_swo_vertical_signal UNIQUE (vertical, signal_type)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_swo_enabled
            ON scoring_weight_overrides (enabled)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_swo_updated_at
            ON scoring_weight_overrides (updated_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS scoring_weight_overrides CASCADE")
