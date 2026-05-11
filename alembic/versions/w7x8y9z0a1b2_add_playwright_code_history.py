"""add_playwright_code_history

Append-only history of LLM-generated Playwright scrape functions. One row per
generation, regeneration, or cache-clear event. Lets us diagnose whether a
scraper regression was triggered by a prompt-version bump or a portal change,
and roll back to a known-good prior generation if needed.

Revision ID: w7x8y9z0a1b2_pw_history
Revises:     369c1a7319a8
Create Date: 2026-05-11
"""

import sqlalchemy as sa
from alembic import op


revision = "w7x8y9z0a1b2_pw_history"
down_revision = "369c1a7319a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "playwright_code_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_id",
            sa.Integer(),
            sa.ForeignKey("county_sources.id"),
            nullable=False,
        ),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("code", sa.Text(), nullable=True),
        sa.Column("prompt_version", sa.String(20), nullable=True),
        sa.Column("reason", sa.String(40), nullable=False),
        sa.Column(
            "is_approved",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_pwc_history_source_generated",
        "playwright_code_history",
        ["source_id", "generated_at"],
    )
    op.create_index(
        "ix_playwright_code_history_source_id",
        "playwright_code_history",
        ["source_id"],
    )
    op.create_index(
        "ix_playwright_code_history_county_id",
        "playwright_code_history",
        ["county_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_playwright_code_history_county_id", table_name="playwright_code_history")
    op.drop_index("ix_playwright_code_history_source_id", table_name="playwright_code_history")
    op.drop_index("idx_pwc_history_source_generated", table_name="playwright_code_history")
    op.drop_table("playwright_code_history")
