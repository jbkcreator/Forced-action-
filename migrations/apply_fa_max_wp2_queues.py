"""FA Max WP-2 — Slack Operating Queues & Send Governance: schema migration.

Idempotent. Safe to re-run (ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT
EXISTS / ON CONFLICT DO NOTHING). Apply after both WP-1 migrations because
consent and audit references depend on WP-1 person and interaction tables.

What this does:
1. Extends relay_approval_queue with lane, person, agent, autonomy evidence,
   and immutable interaction references used by the send audit.
2. Creates fa_max_person_consent (consent per FA Max person per channel).
3. Seeds the fa_max_lending venture row (venture_key is the FK that scopes
   every relay item — FA Max items use this venture, not hillsborough_distress).
"""
from __future__ import annotations

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    # ── relay_approval_queue: operating lane ─────────────────────────────────
    (
        "ADD lane to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue
            ADD COLUMN IF NOT EXISTS lane VARCHAR(20)
                CHECK (lane IS NULL OR lane IN ('MONEY', 'EXCEPTIONS', 'RELATIONSHIPS'));
        """,
    ),
    (
        "ADD agent_name to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue
            ADD COLUMN IF NOT EXISTS agent_name VARCHAR(120);
        """,
    ),
    (
        "ADD autonomy_tier_at_send to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue
            ADD COLUMN IF NOT EXISTS autonomy_tier_at_send VARCHAR(1)
                CHECK (autonomy_tier_at_send IS NULL OR autonomy_tier_at_send IN ('A', 'B', 'C'));
        """,
    ),
    (
        "ADD FA Max governance audit fields to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS person_id UUID;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS autonomy_gate_reason TEXT;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS decision_interaction_id UUID;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS send_interaction_id UUID;
        CREATE INDEX IF NOT EXISTS ix_relay_approval_queue_person_id
            ON relay_approval_queue (person_id);
        """,
    ),
    (
        "ADD durable Slack post lease to relay_approval_queue",
        """
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS slack_post_attempted_at TIMESTAMPTZ;
        ALTER TABLE relay_approval_queue ADD COLUMN IF NOT EXISTS slack_post_lease_until TIMESTAMPTZ;
        CREATE INDEX IF NOT EXISTS ix_relay_fa_max_unposted
            ON relay_approval_queue (created_at)
            WHERE venture_key = 'fa_max_lending' AND status = 'pending' AND slack_message_ts IS NULL;
        """,
    ),
    (
        "ALLOW uncertain provider outcome on relay_approval_queue",
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_relay_approval_queue_status'
                  AND conrelid = 'relay_approval_queue'::regclass
                  AND pg_get_constraintdef(oid) LIKE '%uncertain%'
            ) THEN
                ALTER TABLE relay_approval_queue DROP CONSTRAINT IF EXISTS ck_relay_approval_queue_status;
                ALTER TABLE relay_approval_queue ADD CONSTRAINT ck_relay_approval_queue_status
                    CHECK (status IN ('pending', 'approved', 'rejected', 'sent', 'failed', 'skipped', 'uncertain'));
            END IF;
        END $$;
        """,
    ),
    # ── fa_max_person_consent ─────────────────────────────────────────────────
    (
        "CREATE fa_max_person_consent",
        """
        CREATE TABLE IF NOT EXISTS fa_max_person_consent (
            id               BIGSERIAL PRIMARY KEY,
            person_id        UUID NOT NULL REFERENCES fa_max_persons(person_id) ON DELETE CASCADE,
            channel          VARCHAR(20) NOT NULL
                                 CHECK (channel IN ('email', 'sms', 'voice')),
            consented        BOOLEAN NOT NULL,
            source           VARCHAR(120) NOT NULL,
            consented_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_fa_max_person_consent_person_channel
                UNIQUE (person_id, channel)
        );
        """,
    ),
    (
        "INDEX fa_max_person_consent person_id",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_person_consent_person_id
            ON fa_max_person_consent (person_id);
        """,
    ),
    (
        "CREATE active Backflip campaign suppression store",
        """
        CREATE TABLE IF NOT EXISTS fa_max_backflip_campaign_contacts (
            identifier_kind VARCHAR(10) NOT NULL,
            identifier_value TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT true,
            imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (identifier_kind, identifier_value),
            CONSTRAINT ck_fa_max_backflip_identifier_kind
                CHECK (identifier_kind IN ('email', 'phone'))
        );
        CREATE INDEX IF NOT EXISTS ix_fa_max_backflip_active_contact
            ON fa_max_backflip_campaign_contacts (identifier_kind, identifier_value)
            WHERE active;
        CREATE TABLE IF NOT EXISTS fa_max_backflip_campaign_feed (
            id INTEGER PRIMARY KEY,
            last_success_at TIMESTAMPTZ NOT NULL,
            CONSTRAINT ck_fa_max_backflip_feed_singleton CHECK (id = 1)
        );
        """,
    ),
    (
        "ADD FA Max queue governance constraints",
        """
        DO $$ BEGIN
            ALTER TABLE relay_approval_queue ADD CONSTRAINT fk_relay_fa_max_person
                FOREIGN KEY (person_id) REFERENCES fa_max_persons(person_id);
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        DO $$ BEGIN
            ALTER TABLE relay_approval_queue ADD CONSTRAINT fk_relay_fa_max_decision_interaction
                FOREIGN KEY (decision_interaction_id) REFERENCES fa_max_interactions(interaction_id);
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        DO $$ BEGIN
            ALTER TABLE relay_approval_queue ADD CONSTRAINT fk_relay_fa_max_send_interaction
                FOREIGN KEY (send_interaction_id) REFERENCES fa_max_interactions(interaction_id);
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        DO $$ BEGIN
            ALTER TABLE relay_approval_queue ADD CONSTRAINT ck_relay_fa_max_governance_fields
                CHECK (venture_key <> 'fa_max_lending' OR
                       (lane IS NOT NULL AND agent_name IS NOT NULL AND
                        autonomy_tier_at_send IS NOT NULL AND person_id IS NOT NULL))
                NOT VALID;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        DO $$ BEGIN
            ALTER TABLE relay_approval_queue ADD CONSTRAINT ck_relay_fa_max_no_financial_payload
                CHECK (venture_key <> 'fa_max_lending' OR
                       payload::text !~* '(ssn|social.security|credit.score|fico|income|bank.statement|tax.return|debt.to.income|interest.rate|loan.rate|loan.term|commitment)')
                NOT VALID;
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    ),
    # ── fa_max_lending venture row ────────────────────────────────────────────
    # Registers FA Max as a named venture so relay items can be scoped to it
    # (venture_key='fa_max_lending') — reusing relay's channel/ceiling/kill-
    # switch plumbing without changing the hillsborough_distress venture.
    # relay_slack_channel is intentionally empty here; the three lane-specific
    # channels are controlled by settings fa_max_slack_channel_money/
    # exceptions/relationships instead.
    (
        "SEED fa_max_lending venture",
        """
        INSERT INTO ventures (
            venture_key, display_name, brand_name, state,
            bankruptcy_court_code, default_bankruptcy_division,
            relay_send_window_start, relay_send_window_end,
            relay_send_window_timezone, relay_daily_ceiling,
            kill_switch_feature, is_active
        ) VALUES (
            'fa_max_lending', 'Forced Action MAX Lending', 'Backflip', 'FL',
            'flmb', '8:',
            11, 18, 'America/New_York',
            50,
            'relay_global', true
        )
        ON CONFLICT (venture_key) DO NOTHING;
        """,
    ),
]


def main() -> None:
    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(__import__("sqlalchemy").text(sql))
        session.commit()

    # Verification
    with get_db_context() as session:
        consent_count = session.execute(
            __import__("sqlalchemy").text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'fa_max_person_consent'"
            )
        ).scalar()
        venture_exists = session.execute(
            __import__("sqlalchemy").text(
                "SELECT COUNT(*) FROM ventures WHERE venture_key = 'fa_max_lending'"
            )
        ).scalar()
        lane_col = session.execute(
            __import__("sqlalchemy").text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name = 'relay_approval_queue' AND column_name = 'lane'"
            )
        ).scalar()

    print(f"\nVerification:")
    print(f"  fa_max_person_consent table present: {bool(consent_count)}")
    print(f"  fa_max_lending venture row present:  {bool(venture_exists)}")
    print(f"  relay_approval_queue.lane column:    {bool(lane_col)}")


if __name__ == "__main__":
    main()
