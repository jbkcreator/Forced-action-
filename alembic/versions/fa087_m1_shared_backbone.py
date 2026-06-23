"""M1 Shared Backbone — prospects, enrichment_provenance, events, processed_events

Revision ID: fa087_m1_shared_backbone
Revises: fa083_quora_topics
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from alembic import op

revision: str = "fa087_m1_shared_backbone"
down_revision: Union[str, Sequence[str]] = "fa086_learning_cards_conversion_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # UUIDv7 generator — time-ordered, sequential inserts, cache-friendly
    # ------------------------------------------------------------------
    op.execute("""
        CREATE OR REPLACE FUNCTION generate_uuidv7() RETURNS UUID
        LANGUAGE plpgsql AS $$
        DECLARE
            ts_ms  BIGINT := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
            ts_hex TEXT   := lpad(to_hex(ts_ms), 12, '0');
            r1     TEXT   := lpad(to_hex((random() * 4095)::INT), 3, '0');
            r2     TEXT   := lpad(to_hex((random() * 63)::INT | 128), 2, '0');
            r3     TEXT   := lpad(to_hex((random() * 281474976710655)::BIGINT), 14, '0');
        BEGIN
            RETURN (
                substring(ts_hex, 1, 8) || '-' ||
                substring(ts_hex, 9, 4) || '-' ||
                '7' || r1 || '-' ||
                r2 || substring(r3, 1, 2) || '-' ||
                substring(r3, 3, 12)
            )::UUID;
        END;
        $$
    """)

    # ------------------------------------------------------------------
    # prospects — thin UUID bridge over existing properties
    # ------------------------------------------------------------------
    op.create_table(
        "prospects",
        sa.Column("prospect_id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("contactability_state", sa.String(), nullable=False,
                  server_default="unknown"),
        sa.Column("channel_consent", JSONB(), nullable=False,
                  server_default=sa.text("'{}'::jsonb")),
        sa.Column("contact_attempts", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("successful_contacts", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column(
            "contactability_rate",
            sa.Numeric(5, 4),
            sa.Computed(
                "CASE WHEN contact_attempts >= 5 "
                "THEN successful_contacts::numeric / contact_attempts "
                "ELSE NULL END",
                persisted=True,
            ),
        ),
        sa.Column("cohort_key", sa.String(), nullable=True),
        sa.Column("last_touch_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["property_id"], ["properties.id"],
                                name="fk_prospects_property_id"),
        sa.UniqueConstraint("property_id", name="uq_prospects_property_id"),
        sa.CheckConstraint(
            "contactability_state IN "
            "('unknown','enriching','contactable','invalid','exhausted')",
            name="ck_prospects_contactability_state",
        ),
    )
    op.create_index("idx_prospects_property_id", "prospects", ["property_id"])
    op.create_index(
        "idx_prospects_contactable", "prospects", ["prospect_id"],
        postgresql_where=sa.text("contactability_state = 'contactable'"),
    )
    op.create_index(
        "idx_prospects_cohort", "prospects", ["cohort_key"],
        postgresql_where=sa.text("cohort_key IS NOT NULL"),
    )

    # ------------------------------------------------------------------
    # enrichment_provenance — one row per field per source (1NF, not JSONB array)
    # ------------------------------------------------------------------
    op.create_table(
        "enrichment_provenance",
        sa.Column("id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("prospect_id", PG_UUID(as_uuid=True), nullable=False),
        sa.Column("field_name", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("cost_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(
            ["prospect_id"], ["prospects.prospect_id"],
            name="fk_enrichment_provenance_prospect_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "source IN ('voter','appraiser','tracerfy','batchdata','idi')",
            name="ck_enrichment_provenance_source",
        ),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_enrichment_provenance_confidence",
        ),
    )
    op.create_index(
        "idx_enrichment_provenance_prospect_id",
        "enrichment_provenance", ["prospect_id"],
    )

    # ------------------------------------------------------------------
    # events — universal audit log / async work queue (transactional outbox)
    # ------------------------------------------------------------------
    op.create_table(
        "events",
        sa.Column("event_id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("prospect_id", PG_UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("actor", sa.String(), nullable=False),
        sa.Column("payload", JSONB(), nullable=False,
                  server_default=sa.text("'{}'::jsonb")),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("source_component", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["prospect_id"], ["prospects.prospect_id"],
            name="fk_events_prospect_id",
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'prospect.created','enrichment.completed','enrichment.failed',"
            "'cds.scored','truth.verdict',"
            "'lane.entry','lane.advance','lane.stall','lane.close',"
            "'broker.transition',"
            "'sms.sent','sms.reply',"
            "'commission.posted',"
            "'delivery.sent'"
            ")",
            name="ck_events_event_type",
        ),
    )
    op.create_index("idx_events_prospect_id", "events", ["prospect_id"])
    op.create_index("idx_events_occurred_at", "events", ["occurred_at"],
                    postgresql_using="btree")
    op.create_index("idx_events_type", "events", ["event_type"])

    # ------------------------------------------------------------------
    # processed_events — idempotency guard per (event, consumer)
    # ------------------------------------------------------------------
    op.create_table(
        "processed_events",
        sa.Column("event_id", PG_UUID(as_uuid=True), nullable=False),
        sa.Column("consumer", sa.String(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(
            ["event_id"], ["events.event_id"],
            name="fk_processed_events_event_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id", "consumer",
                                name="pk_processed_events"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
    op.drop_index("idx_events_type", table_name="events")
    op.drop_index("idx_events_occurred_at", table_name="events")
    op.drop_index("idx_events_prospect_id", table_name="events")
    op.drop_table("events")
    op.drop_index("idx_enrichment_provenance_prospect_id",
                  table_name="enrichment_provenance")
    op.drop_table("enrichment_provenance")
    op.drop_index("idx_prospects_cohort", table_name="prospects")
    op.drop_index("idx_prospects_contactable", table_name="prospects")
    op.drop_index("idx_prospects_property_id", table_name="prospects")
    op.drop_table("prospects")
    op.execute("DROP FUNCTION IF EXISTS generate_uuidv7()")
