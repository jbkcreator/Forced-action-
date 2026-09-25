"""FA Max WP-T3-4 — Campaign Selection Agent: schema migration.

Idempotent. Safe to re-run (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT
EXISTS). Apply after WP-1/WP-2 (fa_max_persons, fa_max_partners,
fa_max_opportunities must already exist).

Creates:
1. fa_max_campaign_sequence_steps — the writer's loaded message content,
   versioned per campaign.
2. fa_max_campaign_enrollments — one row per person's current or past
   campaign membership. A partial unique index on person_id enforces "one
   active-or-paused campaign per person" at the database layer.
3. fa_max_campaign_enrollment_events — append-only history of every
   enrollment state change (client Part A Q2: "a full timestamped and
   attributed history").
4. fa_max_campaign_touches — one row per scheduled step, shaped after the
   existing abandonment_sequences table.

See tasks/FA_Max_build/WP-T3-4_Campaign_Selection_Agent_Implementation_Plan.md
Section 7 for the full design.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

STATEMENTS: list[tuple[str, str]] = [
    (
        "CREATE fa_max_campaign_sequence_steps",
        """
        CREATE TABLE IF NOT EXISTS fa_max_campaign_sequence_steps (
            id                   BIGSERIAL PRIMARY KEY,
            campaign_key         VARCHAR(30) NOT NULL
                                     CHECK (campaign_key IN
                                         ('capital_desk_loop', 'exit_desk', 'rescue_circuit')),
            sequence_version     INT NOT NULL,
            step                 INT NOT NULL CHECK (step >= 1),
            days_after_previous  INT NOT NULL CHECK (days_after_previous >= 0),
            channel              VARCHAR(10) NOT NULL CHECK (channel IN ('email', 'sms')),
            subject              TEXT,
            body_template        TEXT NOT NULL,
            loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            loaded_by            VARCHAR(120) NOT NULL,
            CONSTRAINT uq_fa_max_campaign_step UNIQUE (campaign_key, sequence_version, step)
        );
        """,
    ),
    (
        "CREATE fa_max_campaign_enrollments",
        """
        CREATE TABLE IF NOT EXISTS fa_max_campaign_enrollments (
            enrollment_id     UUID PRIMARY KEY DEFAULT generate_uuidv7(),
            person_id         UUID NOT NULL REFERENCES fa_max_persons(person_id) ON DELETE CASCADE,
            campaign_key      VARCHAR(30) NOT NULL
                                  CHECK (campaign_key IN
                                      ('capital_desk_loop', 'exit_desk', 'rescue_circuit')),
            audience          VARCHAR(20) NOT NULL CHECK (audience IN ('investor', 'partner')),
            sequence_version  INT NOT NULL,
            status            VARCHAR(20) NOT NULL
                                  CHECK (status IN
                                      ('active', 'paused', 'completed', 'cancelled', 'preempted')),
            trigger_type      VARCHAR(60) NOT NULL,
            trigger_reason    TEXT,
            source            VARCHAR(60) NOT NULL,
            property_id       INTEGER REFERENCES properties(id),
            county_id         VARCHAR(50),
            state             VARCHAR(2),
            trigger_context   JSONB NOT NULL DEFAULT '{}'::jsonb,
            also_matched      JSONB NOT NULL DEFAULT '[]'::jsonb,
            enrolled_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ended_at          TIMESTAMPTZ,
            end_reason        VARCHAR(60),
            last_touch_sent_at TIMESTAMPTZ,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    ),
    (
        "INDEX one active-or-paused enrollment per person",
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_fa_max_campaign_enrollment_active_person
            ON fa_max_campaign_enrollments (person_id)
            WHERE status IN ('active', 'paused');
        """,
    ),
    (
        "INDEX fa_max_campaign_enrollments campaign/status",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_campaign_enrollments_campaign_status
            ON fa_max_campaign_enrollments (campaign_key, status);
        """,
    ),
    (
        "INDEX fa_max_campaign_enrollments person",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_campaign_enrollments_person
            ON fa_max_campaign_enrollments (person_id);
        """,
    ),
    (
        "CREATE fa_max_campaign_enrollment_events",
        """
        CREATE TABLE IF NOT EXISTS fa_max_campaign_enrollment_events (
            id             BIGSERIAL PRIMARY KEY,
            enrollment_id  UUID NOT NULL REFERENCES fa_max_campaign_enrollments(enrollment_id)
                               ON DELETE CASCADE,
            event          VARCHAR(30) NOT NULL
                               CHECK (event IN
                                   ('enrolled', 'paused', 'resumed', 'preempted',
                                    'cancelled', 'completed', 'touch_held', 'touch_released')),
            reason         TEXT,
            actor          VARCHAR(60) NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    ),
    (
        "INDEX fa_max_campaign_enrollment_events enrollment",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_campaign_enrollment_events_enrollment
            ON fa_max_campaign_enrollment_events (enrollment_id, created_at);
        """,
    ),
    (
        "CREATE fa_max_campaign_touches",
        """
        CREATE TABLE IF NOT EXISTS fa_max_campaign_touches (
            touch_id         BIGSERIAL PRIMARY KEY,
            enrollment_id    UUID NOT NULL REFERENCES fa_max_campaign_enrollments(enrollment_id)
                                 ON DELETE CASCADE,
            step             INT NOT NULL CHECK (step >= 1),
            channel          VARCHAR(10) NOT NULL CHECK (channel IN ('email', 'sms')),
            due_at           TIMESTAMPTZ NOT NULL,
            status           VARCHAR(20) NOT NULL DEFAULT 'scheduled'
                                 CHECK (status IN
                                     ('scheduled', 'held', 'handed_off', 'sent', 'skipped', 'cancelled')),
            status_reason    TEXT,
            work_item_id     UUID,
            relay_item_id    BIGINT,
            sent_at          TIMESTAMPTZ,
            idempotency_key  TEXT NOT NULL,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_fa_max_campaign_touch_idempotency UNIQUE (idempotency_key)
        );
        """,
    ),
    (
        "INDEX fa_max_campaign_touches due",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_campaign_touches_due
            ON fa_max_campaign_touches (due_at)
            WHERE status IN ('scheduled', 'held');
        """,
    ),
    (
        "INDEX fa_max_campaign_touches enrollment",
        """
        CREATE INDEX IF NOT EXISTS ix_fa_max_campaign_touches_enrollment
            ON fa_max_campaign_touches (enrollment_id);
        """,
    ),
]


def apply(engine=None) -> None:
    if engine is not None:
        with engine.begin() as conn:
            for label, sql in STATEMENTS:
                print(f"  -> {label}")
                conn.execute(text(sql))
        return

    with get_db_context() as session:
        for label, sql in STATEMENTS:
            print(f"  -> {label}")
            session.execute(text(sql))
        session.commit()


def main() -> None:
    apply()

    with get_db_context() as session:
        tables = session.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name IN ("
                "'fa_max_campaign_sequence_steps', 'fa_max_campaign_enrollments', "
                "'fa_max_campaign_enrollment_events', 'fa_max_campaign_touches')"
            )
        ).scalars().all()

    print("\nVerification:")
    for name in (
        "fa_max_campaign_sequence_steps",
        "fa_max_campaign_enrollments",
        "fa_max_campaign_enrollment_events",
        "fa_max_campaign_touches",
    ):
        print(f"  {name} present: {name in tables}")


if __name__ == "__main__":
    main()
