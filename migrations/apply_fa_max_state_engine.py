"""
FA Max Durable State Engine (WP-1) — schema migration.

Creates eight tables:
  fa_max_entity_registry
  fa_max_person_lifecycle_stage_config
  fa_max_persons
  fa_max_opportunity_stage_config
  fa_max_opportunities
  fa_max_opportunity_properties
  fa_max_state_transition_events

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS / INSERT ... ON
CONFLICT DO NOTHING). Safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_state_engine.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    # ------------------------------------------------------------------
    # 1. Entity registry — canonical UUID for every tracked object
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_entity_registry (
        entity_uuid UUID PRIMARY KEY DEFAULT generate_uuidv7(),
        entity_type VARCHAR(30) NOT NULL,
        native_id   VARCHAR(255) NOT NULL,
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_entity_registry_type
            CHECK (entity_type IN ('person','property','opportunity','partner','interaction')),
        CONSTRAINT uq_fa_max_entity_registry_type_native
            UNIQUE (entity_type, native_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_entity_registry_type ON fa_max_entity_registry (entity_type)",

    # ------------------------------------------------------------------
    # 2. Person lifecycle stage config
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_person_lifecycle_stage_config (
        stage_key    VARCHAR(50)  PRIMARY KEY,
        display_name VARCHAR(100) NOT NULL,
        order_index  INTEGER      NOT NULL,
        allowed_next JSONB        NOT NULL DEFAULT '[]'::jsonb,
        is_terminal  BOOLEAN      NOT NULL DEFAULT FALSE,
        is_active    BOOLEAN      NOT NULL DEFAULT TRUE
    )
    """,

    # ------------------------------------------------------------------
    # 3. Seed person lifecycle stages (SOT.md 12-stage progression)
    #    Insert only if table is empty to support re-runs.
    # ------------------------------------------------------------------
    """
    INSERT INTO fa_max_person_lifecycle_stage_config
        (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
    VALUES
        ('identified',     'Identified',          1,  '["qualifying","suppressed","dead"]'::jsonb,         FALSE, TRUE),
        ('qualifying',     'Qualifying',          2,  '["warm","cold","suppressed","dead"]'::jsonb,        FALSE, TRUE),
        ('warm',           'Warm',                3,  '["active","cold","suppressed","dead"]'::jsonb,      FALSE, TRUE),
        ('cold',           'Cold',                4,  '["warm","active","suppressed","dead"]'::jsonb,      FALSE, TRUE),
        ('active',         'Active — Opportunity',5,  '["submitted","warm","cold","suppressed","dead"]'::jsonb, FALSE, TRUE),
        ('submitted',      'Submitted to Backflip',6, '["funded","declined","active","suppressed"]'::jsonb,FALSE, TRUE),
        ('funded',         'Funded',              7,  '["repeat","suppressed"]'::jsonb,                   TRUE,  TRUE),
        ('declined',       'Declined',            8,  '["active","suppressed","dead"]'::jsonb,             FALSE, TRUE),
        ('repeat',         'Repeat Borrower',     9,  '["active","suppressed","dead"]'::jsonb,             FALSE, TRUE),
        ('suppressed',     'Suppressed',          10, '[]'::jsonb,                                         TRUE,  TRUE),
        ('dead',           'Dead',                11, '[]'::jsonb,                                         TRUE,  TRUE),
        ('do_not_contact', 'Do Not Contact',      12, '[]'::jsonb,                                         TRUE,  TRUE)
    ON CONFLICT (stage_key) DO NOTHING
    """,

    # ------------------------------------------------------------------
    # 4. Persons — canonical borrower identity
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_persons (
        person_id       UUID        PRIMARY KEY DEFAULT generate_uuidv7(),
        lifecycle_state VARCHAR(50) NOT NULL DEFAULT 'identified'
            REFERENCES fa_max_person_lifecycle_stage_config(stage_key)
            DEFERRABLE INITIALLY DEFERRED,
        merged_into_id  UUID        REFERENCES fa_max_persons(person_id),
        source          VARCHAR(60) NOT NULL,
        source_reference VARCHAR(255),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_persons_no_self_merge
            CHECK (merged_into_id IS NULL OR merged_into_id <> person_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_persons_lifecycle_state ON fa_max_persons (lifecycle_state)",
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_persons_not_merged
        ON fa_max_persons (person_id)
        WHERE merged_into_id IS NULL
    """,

    # ------------------------------------------------------------------
    # 5. Opportunity stage config
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_opportunity_stage_config (
        stage_key    VARCHAR(50)  PRIMARY KEY,
        display_name VARCHAR(100) NOT NULL,
        order_index  INTEGER      NOT NULL,
        allowed_next JSONB        NOT NULL DEFAULT '[]'::jsonb,
        is_terminal  BOOLEAN      NOT NULL DEFAULT FALSE,
        is_active    BOOLEAN      NOT NULL DEFAULT TRUE
    )
    """,

    # Seed opportunity stages
    """
    INSERT INTO fa_max_opportunity_stage_config
        (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
    VALUES
        ('new',         'New',                1,  '["qualifying","dead"]'::jsonb,                               FALSE, TRUE),
        ('qualifying',  'Qualifying',         2,  '["scoping","warm_hold","dead"]'::jsonb,                     FALSE, TRUE),
        ('scoping',     'Scoping Deal',       3,  '["ready_to_submit","warm_hold","dead"]'::jsonb,              FALSE, TRUE),
        ('warm_hold',   'Warm Hold',          4,  '["scoping","dead"]'::jsonb,                                  FALSE, TRUE),
        ('ready_to_submit','Ready to Submit', 5,  '["submitted","scoping","dead"]'::jsonb,                     FALSE, TRUE),
        ('submitted',   'Submitted',          6,  '["term_sheet","declined","dead"]'::jsonb,                   FALSE, TRUE),
        ('term_sheet',  'Term Sheet',         7,  '["closing","declined","dead"]'::jsonb,                      FALSE, TRUE),
        ('closing',     'Closing',            8,  '["funded","dead"]'::jsonb,                                  FALSE, TRUE),
        ('funded',      'Funded',             9,  '[]'::jsonb,                                                  TRUE,  TRUE),
        ('declined',    'Declined',           10, '[]'::jsonb,                                                  TRUE,  TRUE),
        ('dead',        'Dead',               11, '[]'::jsonb,                                                  TRUE,  TRUE)
    ON CONFLICT (stage_key) DO NOTHING
    """,

    # ------------------------------------------------------------------
    # 6. Opportunities
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_opportunities (
        opportunity_id   UUID        PRIMARY KEY DEFAULT generate_uuidv7(),
        person_id        UUID        NOT NULL REFERENCES fa_max_persons(person_id),
        opportunity_type VARCHAR(30) NOT NULL,
        current_stage    VARCHAR(50) NOT NULL DEFAULT 'new'
            REFERENCES fa_max_opportunity_stage_config(stage_key)
            DEFERRABLE INITIALLY DEFERRED,
        outcome          VARCHAR(20) NOT NULL DEFAULT 'open',
        source           VARCHAR(60) NOT NULL,
        source_reference VARCHAR(255),
        idempotency_key  VARCHAR(255) UNIQUE,
        expected_need_date TIMESTAMPTZ,
        actual_funded_at   TIMESTAMPTZ,
        loan_amount_cents  BIGINT,
        maturity_months    SMALLINT,
        backflip_ref     VARCHAR(255),
        assigned_to      VARCHAR(120),
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_opp_type CHECK (
            opportunity_type IN (
                'acquisition','rehab','construction','extension',
                'refinance','dscr_takeout','repeat'
            )
        ),
        CONSTRAINT ck_fa_max_opp_outcome CHECK (
            outcome IN ('open','funded','dead','recycled','referred')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_opp_person_id ON fa_max_opportunities (person_id)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_opp_stage ON fa_max_opportunities (current_stage)",
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_opp_open
        ON fa_max_opportunities (outcome)
        WHERE outcome = 'open'
    """,

    # ------------------------------------------------------------------
    # 7. Opportunity ↔ Property link table
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_opportunity_properties (
        id             BIGSERIAL   PRIMARY KEY,
        opportunity_id UUID        NOT NULL REFERENCES fa_max_opportunities(opportunity_id),
        property_id    INTEGER     NOT NULL REFERENCES properties(id),
        role           VARCHAR(30) NOT NULL DEFAULT 'subject',
        source         VARCHAR(60),
        linked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_opp_prop_role CHECK (
            role IN ('subject','collateral','current_project','exit_property')
        ),
        CONSTRAINT uq_fa_max_opp_prop_role UNIQUE (opportunity_id, property_id, role)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_opp_prop_opp_id ON fa_max_opportunity_properties (opportunity_id)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_opp_prop_property_id ON fa_max_opportunity_properties (property_id)",

    # ------------------------------------------------------------------
    # 8. State transition events — the append-only event spine
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_state_transition_events (
        event_id         UUID         PRIMARY KEY DEFAULT generate_uuidv7(),
        entity_uuid      UUID         NOT NULL REFERENCES fa_max_entity_registry(entity_uuid),
        person_id        UUID         REFERENCES fa_max_persons(person_id),
        entity_type      VARCHAR(30)  NOT NULL,
        from_state       VARCHAR(50)  NOT NULL,
        to_state         VARCHAR(50)  NOT NULL,
        actor            VARCHAR(120) NOT NULL,
        source_component VARCHAR(120) NOT NULL,
        decision_id      VARCHAR(36)  REFERENCES agent_decisions(decision_id),
        context          JSONB        NOT NULL DEFAULT '{}'::jsonb,
        idempotency_key  VARCHAR(255) NOT NULL UNIQUE,
        occurred_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_ste_entity_type CHECK (
            entity_type IN ('person','property','opportunity','partner','interaction')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_ste_entity_uuid_occurred ON fa_max_state_transition_events (entity_uuid, occurred_at)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_ste_person_id_occurred ON fa_max_state_transition_events (person_id, occurred_at)",
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_ste_decision_id
        ON fa_max_state_transition_events (decision_id)
        WHERE decision_id IS NOT NULL
    """,
    # GIN index on context JSONB for flexible audit queries
    "CREATE INDEX IF NOT EXISTS ix_fa_max_ste_context_gin ON fa_max_state_transition_events USING GIN (context)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()

        # Verification: count rows in each table
        tables = [
            "fa_max_entity_registry",
            "fa_max_person_lifecycle_stage_config",
            "fa_max_persons",
            "fa_max_opportunity_stage_config",
            "fa_max_opportunities",
            "fa_max_opportunity_properties",
            "fa_max_state_transition_events",
        ]
        for table in tables:
            count = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {count} rows")

    print("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
