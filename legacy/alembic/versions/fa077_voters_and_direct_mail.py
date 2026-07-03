"""fa077 — voters table, tax_collector enriched source, owners.direct_mail_eligible

Task 4 (SOE + Tax Collector bulk loaders) schema:

1. New `voters` table — 1:Many spoke off properties, populated by
   VoterRegistryLoader from county SOE bulk files. Voters are matched by
   residential address; multiple rows per property are intended (household
   alternative-contact network). Contact-enrichment only — never feeds CDS.
   Phones/emails are isolated from auto-send paths (ADR 0013).

2. `check_enriched_source` on enriched_contacts gains 'tax_collector' —
   rows carrying the county tax-bill billing address when it normalizes
   differently from owners.mailing_address (ADR 0014). 'tracerfy' is included
   because the live DB constraint already carries it (model had drifted).

3. `owners.direct_mail_eligible` — flagged true when the skip-trace waterfall
   ends in a MISS but a usable mailing address exists (direct-mail fallback).

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply on the server with the idempotent companion script instead:

    PYTHONPATH=. python scripts/apply_fa077_ddl.py

This file is the schema-of-record; the script performs the same DDL.

Revision ID: fa077_voters_and_direct_mail
Revises: fa076_widen_unmatched_match_method
Create Date: 2026-06-10
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = 'fa077_voters_and_direct_mail'
down_revision: Union[str, Sequence[str], None] = 'fa076_widen_unmatched_match_method'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ENRICHED_SOURCES = "'batch_skip_tracing', 'idi', 'pdl', 'tracerfy', 'tax_collector'"
_ENRICHED_SOURCES_OLD = "'batch_skip_tracing', 'idi', 'pdl', 'tracerfy'"


def upgrade() -> None:
    op.create_table(
        "voters",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("property_id", sa.Integer, sa.ForeignKey("properties.id"), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("source_voter_id", sa.String(20), nullable=False),
        sa.Column("voter_name", sa.String(255)),
        sa.Column("first_name", sa.String(100)),
        sa.Column("middle_name", sa.String(100)),
        sa.Column("last_name", sa.String(100)),
        sa.Column("residential_address", sa.String(500)),
        sa.Column("residential_city", sa.String(100)),
        sa.Column("residential_zip", sa.String(10)),
        sa.Column("mailing_address", sa.String(500)),
        sa.Column("registration_status", sa.String(10)),
        sa.Column("registration_date", sa.Date),
        sa.Column("phones", JSONB),
        sa.Column("phone_1", sa.String(20)),
        sa.Column("email", sa.String(255)),
        sa.Column("meta_data", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("county_id", "source_voter_id", name="uq_voter_county_source_id"),
        sa.CheckConstraint(
            "registration_status IN ('ACT', 'INA') OR registration_status IS NULL",
            name="check_voter_registration_status",
        ),
    )
    op.create_index("ix_voters_property_id", "voters", ["property_id"])
    op.create_index("ix_voters_county_id", "voters", ["county_id"])
    op.create_index("ix_voters_voter_name", "voters", ["voter_name"])
    op.create_index("idx_voter_registration_status", "voters", ["registration_status"])

    op.execute(sa.text(
        "ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source"
    ))
    op.create_check_constraint(
        "check_enriched_source", "enriched_contacts",
        f"source IN ({_ENRICHED_SOURCES})",
    )

    op.add_column(
        "owners",
        sa.Column("direct_mail_eligible", sa.Boolean, nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("owners", "direct_mail_eligible")
    op.execute(sa.text(
        "ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source"
    ))
    op.create_check_constraint(
        "check_enriched_source", "enriched_contacts",
        f"source IN ({_ENRICHED_SOURCES_OLD})",
    )
    op.drop_table("voters")
