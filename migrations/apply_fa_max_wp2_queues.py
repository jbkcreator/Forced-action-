"""FA Max WP-2 — Slack Operating Queues & Send Governance: schema migration.

Idempotent. Safe to re-run (ADD COLUMN IF NOT EXISTS / CREATE TABLE IF NOT
EXISTS / ON CONFLICT DO NOTHING). Apply AFTER apply_fa_max_state_engine.py
(WP-1) because fa_max_person_consent has a FK to fa_max_persons.

What this does:
1. Extends relay_approval_queue with three nullable audit columns:
   lane (MONEY|EXCEPTIONS|RELATIONSHIPS), agent_name, autonomy_tier_at_send.
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
