"""fa031 — Sunbiz LLC piercing + reverse-index foundation

Extends `owners` with the Sunbiz fields the scraper today does not capture
(document number, principal address, agent email, entity status, formation
date, managing members) plus an enrichment status / timestamp pair that the
daily refresh task and the one-shot backfill script both key off of.

Adds `sunbiz_snapshots` as the audit + reparse store: raw scraped HTML plus
parsed JSONB keyed by document number, so a parser improvement reprocesses
historical scrapes without re-hitting the Sunbiz portal.

Adds `consent_scope` to `sms_opt_ins`. Compliance prerequisite for any future
SMS send to managing-member-derived phone numbers — opt-in for the LLC entity
must NOT inherit to its members. Default `'subscriber'` preserves current
behaviour for all existing rows; new managing-member opt-ins will land with
`'managing_member_direct'`.

Revision ID: fa031_sunbiz_owner_extension
Revises:     fa030_merge_foreclosures_defendant
Create Date: 2026-05-25
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "fa031_sunbiz_owner_extension"
down_revision = "fa030_merge_foreclosures_defendant"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── owners: Sunbiz piercing fields ──────────────────────────────────
    op.add_column("owners", sa.Column("sunbiz_doc_number", sa.Text(), nullable=True))
    op.add_column("owners", sa.Column("principal_address", sa.Text(), nullable=True))
    op.add_column("owners", sa.Column("registered_agent_email", sa.String(255), nullable=True))
    op.add_column("owners", sa.Column("entity_status", sa.String(20), nullable=True))
    op.add_column("owners", sa.Column("formation_date", sa.Date(), nullable=True))
    op.add_column("owners", sa.Column("managing_members", postgresql.JSONB, nullable=True))
    op.add_column(
        "owners",
        sa.Column("sunbiz_enriched_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "owners",
        sa.Column(
            "sunbiz_status",
            sa.String(20),
            nullable=False,
            server_default="pending",
        ),
    )
    op.create_check_constraint(
        "check_sunbiz_status",
        "owners",
        "sunbiz_status IN ('pending','matched','not_found','ambiguous',"
        "'parser_failed','not_an_llc')",
    )

    # Non-unique lookup index on the hard dedupe key. NOT unique: in the
    # owners-only v1 design a single LLC's doc_number is denormalized across
    # every Owner row sharing that LLC name (one doc → N owner rows). Uniqueness
    # belongs on the v2 `entities` table; here we just want fast doc lookup.
    op.execute(
        "CREATE INDEX ix_owners_sunbiz_doc "
        "ON owners(sunbiz_doc_number) "
        "WHERE sunbiz_doc_number IS NOT NULL"
    )
    op.create_index("ix_owners_sunbiz_status", "owners", ["sunbiz_status"])
    # Partial index: daily refresh task queries matched rows ordered by enriched_at.
    op.execute(
        "CREATE INDEX ix_owners_sunbiz_enriched "
        "ON owners(sunbiz_enriched_at) "
        "WHERE sunbiz_status = 'matched'"
    )
    op.create_index(
        "ix_owners_managing_members",
        "owners",
        ["managing_members"],
        postgresql_using="gin",
    )

    # ── sunbiz_snapshots: audit + reparse store ─────────────────────────
    op.create_table(
        "sunbiz_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("sunbiz_doc_number", sa.Text(), nullable=False),
        sa.Column(
            "scraped_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("raw_html", sa.Text(), nullable=True),
        sa.Column("raw_jsonb", postgresql.JSONB, nullable=False),
        sa.Column("parser_version", sa.String(40), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.CheckConstraint(
            "status IN ('ok','partial','parser_failed')",
            name="check_snapshot_status",
        ),
    )
    op.execute(
        "CREATE INDEX ix_sbsnap_doc_recent "
        "ON sunbiz_snapshots(sunbiz_doc_number, scraped_at DESC)"
    )

    # ── sms_opt_ins: per-identity consent scoping ───────────────────────
    op.add_column(
        "sms_opt_ins",
        sa.Column(
            "consent_scope",
            sa.String(30),
            nullable=False,
            server_default="subscriber",
        ),
    )
    op.create_check_constraint(
        "check_opt_in_consent_scope",
        "sms_opt_ins",
        "consent_scope IN ('subscriber','managing_member_direct','agent_direct','other')",
    )


def downgrade() -> None:
    op.drop_constraint("check_opt_in_consent_scope", "sms_opt_ins", type_="check")
    op.drop_column("sms_opt_ins", "consent_scope")

    op.execute("DROP INDEX IF EXISTS ix_sbsnap_doc_recent")
    op.drop_table("sunbiz_snapshots")

    op.drop_index("ix_owners_managing_members", table_name="owners")
    op.execute("DROP INDEX IF EXISTS ix_owners_sunbiz_enriched")
    op.drop_index("ix_owners_sunbiz_status", table_name="owners")
    op.execute("DROP INDEX IF EXISTS ix_owners_sunbiz_doc")

    op.drop_constraint("check_sunbiz_status", "owners", type_="check")
    op.drop_column("owners", "sunbiz_status")
    op.drop_column("owners", "sunbiz_enriched_at")
    op.drop_column("owners", "managing_members")
    op.drop_column("owners", "formation_date")
    op.drop_column("owners", "entity_status")
    op.drop_column("owners", "registered_agent_email")
    op.drop_column("owners", "principal_address")
    op.drop_column("owners", "sunbiz_doc_number")
