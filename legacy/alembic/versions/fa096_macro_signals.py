"""fa096 - A7 macro_signals table

Normalized macro-economic signal observations — one row per
(source, signal_key, source_series_id, observed_at, geography_scope, geography_id).

Sources: FRED, FHFA, BLS, Census ACS5.

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply operationally via:

    PYTHONPATH=. python scripts/apply_fa096_macro_signals.py
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fa096_macro_signals"
down_revision: Union[str, Sequence[str], None] = "fa095_feedback_ritual_shared_queue_refactor"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "macro_signals",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("generate_uuidv7()"),
            nullable=False,
        ),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("signal_key", sa.String(80), nullable=False),
        sa.Column("source_series_id", sa.String(120), nullable=False, server_default=sa.text("''")),
        sa.Column("value", sa.Numeric(18, 6), nullable=False),
        sa.Column("unit", sa.String(30), nullable=False),
        sa.Column("observed_at", sa.Date, nullable=False),
        sa.Column("frequency", sa.String(20), nullable=False),
        sa.Column("geography_scope", sa.String(50), nullable=False),
        sa.Column("geography_id", sa.String(30), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "source", "signal_key", "source_series_id",
            "observed_at", "geography_scope", "geography_id",
            name="uq_macro_signal_observation",
        ),
    )
    op.create_index("idx_macro_signals_source", "macro_signals", ["source"])
    op.create_index("idx_macro_signals_signal_key", "macro_signals", ["signal_key"])
    op.create_index("idx_macro_signals_observed_at", "macro_signals", ["observed_at"])
    op.create_index(
        "idx_macro_signals_source_key_date",
        "macro_signals",
        ["source", "signal_key", "observed_at"],
    )
    op.create_index(
        "idx_macro_signals_geo",
        "macro_signals",
        ["geography_scope", "geography_id"],
    )
    op.create_index(
        "idx_macro_signals_source_geo",
        "macro_signals",
        ["source", "geography_scope", "geography_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_macro_signals_source_geo", table_name="macro_signals")
    op.drop_index("idx_macro_signals_geo", table_name="macro_signals")
    op.drop_index("idx_macro_signals_source_key_date", table_name="macro_signals")
    op.drop_index("idx_macro_signals_observed_at", table_name="macro_signals")
    op.drop_index("idx_macro_signals_signal_key", table_name="macro_signals")
    op.drop_index("idx_macro_signals_source", table_name="macro_signals")
    op.drop_table("macro_signals")
