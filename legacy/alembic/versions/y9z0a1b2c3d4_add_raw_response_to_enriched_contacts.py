"""add_raw_response_to_enriched_contacts

Adds raw_response JSONB column to enriched_contacts so the full BatchData
(or IDI) API response is cached alongside the parsed contact fields.

This lets callers re-parse the original response without a new API call —
useful for extracting additional fields (e.g. mailing address city/state/zip
individually for Out-of-State absentee detection) after the fact.

Revision ID: y9z0a1b2c3d4_add_raw_response_to_enriched_contacts
Revises:     f0da513c3d8c
Create Date: 2026-05-22
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "y9z0a1b2c3d4_add_raw_response"
down_revision = "f0da513c3d8c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "enriched_contacts",
        sa.Column("raw_response", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("enriched_contacts", "raw_response")
