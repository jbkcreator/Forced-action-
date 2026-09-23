"""FA Max WP-T2-3 — Enablement Boundary & Backflip Campaign Suppression: schema.

Idempotent. Safe to re-run (ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT
EXISTS). Apply after WP-T2-2 migrations (apply_fa_max_wp_t2_2_agent_infra.py
and its follow-ons) — this extends fa_max_opportunities and relay_approval_queue
and creates the suppression decision audit table.

What this does:
1. Adds opportunity_id (nullable FK → fa_max_opportunities) on
   relay_approval_queue — links a queue row to the specific opportunity that
   triggered the draft, so mark_sent() can write attribution without guessing
   from person_id alone.
2. Adds backflip_attribution_owner + backflip_attribution_set_at on
   fa_max_opportunities — write-once record of which channel (forced_action |
   backflip) is credited with the relationship at the time of first real send.
   NULL = no send has landed yet. Written by mark_sent() under a
   WHERE backflip_attribution_owner IS NULL guard so concurrent calls are safe.
3. Creates fa_max_backflip_suppression_decisions — durable audit log for every
   suppression decision made at the draft gate or send gate, written in a
   SEPARATE committed transaction so it survives the draft gate's GovernanceBlocked
   rollback. Recipient is stored as a masked value plus a deterministic SHA-256
   digest, allowing audit correlation without storing the raw address or number.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "ADD opportunity_id to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue
            ADD COLUMN IF NOT EXISTS opportunity_id UUID
                REFERENCES fa_max_opportunities(opportunity_id)
                ON DELETE SET NULL;
        ALTER TABLE relay_approval_queue
            ADD COLUMN IF NOT EXISTS channel_split_source VARCHAR(60);
        CREATE INDEX IF NOT EXISTS ix_relay_queue_opportunity_id
            ON relay_approval_queue (opportunity_id)
            WHERE opportunity_id IS NOT NULL;
        """,
    ),
    (
        "ADD backflip_attribution_owner + backflip_attribution_set_at to fa_max_opportunities",
        """
        ALTER TABLE fa_max_opportunities
            ADD COLUMN IF NOT EXISTS backflip_attribution_owner VARCHAR(30),
            ADD COLUMN IF NOT EXISTS backflip_attribution_set_at TIMESTAMPTZ;
        ALTER TABLE fa_max_opportunities
            DROP CONSTRAINT IF EXISTS ck_fa_max_opp_attribution_owner;
        ALTER TABLE fa_max_opportunities
            ADD CONSTRAINT ck_fa_max_opp_attribution_owner
                CHECK (
                    backflip_attribution_owner IS NULL
                    OR backflip_attribution_owner IN ('forced_action', 'backflip')
                );
        CREATE INDEX IF NOT EXISTS ix_fa_max_opp_attribution_owner
            ON fa_max_opportunities (backflip_attribution_owner)
            WHERE backflip_attribution_owner IS NOT NULL;
        """,
    ),
    (
        "CREATE fa_max_backflip_suppression_decisions audit table",
        """
        CREATE TABLE IF NOT EXISTS fa_max_backflip_suppression_decisions (
            id BIGSERIAL PRIMARY KEY,
            gate VARCHAR(10) NOT NULL,
            recipient_masked VARCHAR(20) NOT NULL,
            recipient_sha256 VARCHAR(64),
            opportunity_id UUID,
            suppressed BOOLEAN NOT NULL,
            reason TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ck_fa_max_bsd_gate CHECK (gate IN ('draft', 'send'))
        );
        ALTER TABLE fa_max_backflip_suppression_decisions
            ADD COLUMN IF NOT EXISTS recipient_sha256 VARCHAR(64);
        ALTER TABLE fa_max_backflip_suppression_decisions
            ALTER COLUMN recipient_sha256 TYPE VARCHAR(64);
        ALTER TABLE fa_max_backflip_suppression_decisions
            ALTER COLUMN reason TYPE TEXT;
        CREATE INDEX IF NOT EXISTS ix_fa_max_bsd_opportunity_id
            ON fa_max_backflip_suppression_decisions (opportunity_id)
            WHERE opportunity_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS ix_fa_max_bsd_created_at
            ON fa_max_backflip_suppression_decisions (created_at DESC);
        """,
    ),
    (
        "Protect write-once Backflip attribution",
        """
        CREATE OR REPLACE FUNCTION fa_max_attribution_write_once() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.backflip_attribution_owner IS NOT NULL AND
               (NEW.backflip_attribution_owner IS DISTINCT FROM OLD.backflip_attribution_owner
                OR NEW.backflip_attribution_set_at IS DISTINCT FROM OLD.backflip_attribution_set_at) THEN
                RAISE EXCEPTION 'backflip attribution is write-once';
            END IF;
            RETURN NEW;
        END;
        $$;
        DROP TRIGGER IF EXISTS trg_fa_max_attribution_write_once ON fa_max_opportunities;
        CREATE TRIGGER trg_fa_max_attribution_write_once
            BEFORE UPDATE ON fa_max_opportunities
            FOR EACH ROW EXECUTE FUNCTION fa_max_attribution_write_once();
        """,
    ),
    (
        "Create verified FA Max contact identifiers",
        """
        CREATE TABLE IF NOT EXISTS fa_max_person_contact_identifiers (
            identifier_kind VARCHAR(10) NOT NULL,
            identifier_value TEXT NOT NULL,
            person_id UUID NOT NULL REFERENCES fa_max_persons(person_id),
            source VARCHAR(60) NOT NULL,
            verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (identifier_kind, identifier_value),
            CONSTRAINT ck_fa_max_person_identifier_kind
                CHECK (identifier_kind IN ('email', 'phone'))
        );
        CREATE INDEX IF NOT EXISTS ix_fa_max_person_contact_identifiers_person
            ON fa_max_person_contact_identifiers (person_id);
        """,
    ),
    (
        "Create first clean FA touch claim",
        """
        CREATE TABLE IF NOT EXISTS fa_max_person_first_touch (
            person_id UUID PRIMARY KEY REFERENCES fa_max_persons(person_id),
            relay_item_id BIGINT NOT NULL REFERENCES relay_approval_queue(id),
            channel_split_source VARCHAR(60) NOT NULL,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ),
]


def run() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  apply: {label}")
            session.execute(text(sql))
    print("apply_fa_max_wp_t2_3: done")


if __name__ == "__main__":
    run()
