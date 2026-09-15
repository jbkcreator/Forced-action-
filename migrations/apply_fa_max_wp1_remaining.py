"""
FA Max WP-1 remaining schema — migration.

Adds to the schema laid by apply_fa_max_state_engine.py:

  1. Reseeds fa_max_person_lifecycle_stage_config with the 12 SOT stages
     (identified → enriched → contacted → engaged → qualified →
      portal_started → application_submitted → term_sheet_issued →
      locked → funded → matured → repeat) + 3 terminal states
     (suppressed, dead, do_not_contact). Old stage rows not in this set
     are deleted.

  2. Adds state_version (INTEGER DEFAULT 0) to fa_max_persons and
     fa_max_opportunities.

  3. Creates fa_max_partners.

  4. Creates fa_max_interactions (write-once).

  5. Creates fa_max_property_associations (temporal, integer FK → properties.id).

  6. Installs DB immutability trigger on fa_max_state_transition_events and
     fa_max_interactions — BEFORE UPDATE OR DELETE raises unless session-local
     GUC fa_max.allow_state_write = 'on'.

  7. Creates fa_max_work_queue with indexes for claim_next() / reclaim_expired().

Idempotent: safe to re-run (ADD COLUMN IF NOT EXISTS, CREATE TABLE IF NOT EXISTS,
CREATE INDEX IF NOT EXISTS, INSERT ... ON CONFLICT DO NOTHING).

Run:
  PYTHONPATH=. python migrations/apply_fa_max_wp1_remaining.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

# SOT 12-stage forward chain + 3 terminal states
# allowed_next: forward-one-step + terminal jumps (any-active -> terminal) +
# explicit backward edge (repeat -> engaged only).
_SOT_STAGES = [
    # (stage_key, display_name, order_index, allowed_next, is_terminal)
    ("identified",            "Identified",              1,
     '["enriched","suppressed","dead","do_not_contact"]', False),
    ("enriched",              "Enriched",                2,
     '["contacted","suppressed","dead","do_not_contact"]', False),
    ("contacted",             "Contacted",               3,
     '["engaged","suppressed","dead","do_not_contact"]', False),
    ("engaged",               "Engaged",                 4,
     '["qualified","suppressed","dead","do_not_contact"]', False),
    ("qualified",             "Qualified",               5,
     '["portal_started","suppressed","dead","do_not_contact"]', False),
    ("portal_started",        "Portal Started",          6,
     '["application_submitted","suppressed","dead","do_not_contact"]', False),
    ("application_submitted", "Application Submitted",   7,
     '["term_sheet_issued","suppressed","dead","do_not_contact"]', False),
    ("term_sheet_issued",     "Term Sheet Issued",       8,
     '["locked","suppressed","dead","do_not_contact"]', False),
    ("locked",                "Locked",                  9,
     '["funded","suppressed","dead","do_not_contact"]', False),
    ("funded",                "Funded",                  10,
     '["matured","suppressed","dead","do_not_contact"]', False),
    ("matured",               "Matured",                 11,
     '["repeat","suppressed","dead","do_not_contact"]', False),
    ("repeat",                "Repeat Borrower",         12,
     '["engaged","suppressed","dead","do_not_contact"]', False),
    ("suppressed",            "Suppressed",              13, '[]', True),
    ("dead",                  "Dead",                    14, '[]', True),
    ("do_not_contact",        "Do Not Contact",          15, '[]', True),
]

_SOT_STAGE_KEYS = tuple(s[0] for s in _SOT_STAGES)

STATEMENTS = [
    # Insert/update the destination stages before remapping live records.
] + [
    f"""
    INSERT INTO fa_max_person_lifecycle_stage_config
        (stage_key, display_name, order_index, allowed_next, is_terminal, is_active)
    VALUES
        ('{sk}', '{dn}', {oi}, '{an}'::jsonb, {str(it).upper()}, TRUE)
    ON CONFLICT (stage_key) DO UPDATE
        SET display_name = EXCLUDED.display_name,
            order_index  = EXCLUDED.order_index,
            allowed_next = EXCLUDED.allowed_next,
            is_terminal  = EXCLUDED.is_terminal,
            is_active    = EXCLUDED.is_active
    """
    for sk, dn, oi, an, it in _SOT_STAGES
] + [
    # Preserve live rows created under the original WP-1 vocabulary.
    "SELECT set_config('fa_max.allow_state_write', 'on', true)",
    """
    UPDATE fa_max_persons
    SET lifecycle_state = CASE lifecycle_state
        WHEN 'qualifying' THEN 'qualified'
        WHEN 'warm' THEN 'engaged'
        WHEN 'cold' THEN 'contacted'
        WHEN 'active' THEN 'engaged'
        WHEN 'submitted' THEN 'application_submitted'
        WHEN 'declined' THEN 'dead'
        ELSE lifecycle_state
    END
    WHERE lifecycle_state IN ('qualifying','warm','cold','active','submitted','declined')
    """,
    f"""
    DELETE FROM fa_max_person_lifecycle_stage_config
    WHERE stage_key NOT IN {str(_SOT_STAGE_KEYS).replace('[','(').replace(']',')')}
    """,
    "SELECT set_config('fa_max.allow_state_write', 'off', true)",
    # ------------------------------------------------------------------ #
    # 2. state_version on persons and opportunities                       #
    # ------------------------------------------------------------------ #
    "ALTER TABLE fa_max_persons ADD COLUMN IF NOT EXISTS state_version INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE fa_max_opportunities ADD COLUMN IF NOT EXISTS state_version INTEGER NOT NULL DEFAULT 0",

    # ------------------------------------------------------------------ #
    # 3. fa_max_partners                                                  #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fa_max_partners (
        partner_id    UUID        PRIMARY KEY DEFAULT generate_uuidv7(),
        person_id     UUID        NOT NULL REFERENCES fa_max_persons(person_id),
        partner_class VARCHAR(60) NOT NULL,
        status        VARCHAR(20) NOT NULL DEFAULT 'identified',
        rank          INTEGER,
        source        VARCHAR(60) NOT NULL,
        state_version INTEGER     NOT NULL DEFAULT 0,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_partner_status
            CHECK (status IN ('identified', 'active', 'inactive'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_partner_person_id ON fa_max_partners (person_id)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_partner_status ON fa_max_partners (status)",

    # ------------------------------------------------------------------ #
    # 4. fa_max_interactions (write-once)                                 #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fa_max_interactions (
        interaction_id        UUID        PRIMARY KEY DEFAULT generate_uuidv7(),
        person_id             UUID        NOT NULL REFERENCES fa_max_persons(person_id),
        channel               VARCHAR(20) NOT NULL,
        direction             VARCHAR(10) NOT NULL,
        actor                 VARCHAR(120) NOT NULL,
        approved_bool         BOOLEAN,
        autonomy_tier_at_time VARCHAR(5),
        body_redacted         TEXT,
        occurred_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        seq                   BIGSERIAL   NOT NULL UNIQUE,
        created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_interaction_channel
            CHECK (channel IN ('email','sms','voice','slack','linkedin')),
        CONSTRAINT ck_fa_max_interaction_direction
            CHECK (direction IN ('inbound','outbound')),
        CONSTRAINT ck_fa_max_interaction_tier
            CHECK (autonomy_tier_at_time IS NULL
                   OR autonomy_tier_at_time IN ('A','B','C'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_interaction_person_id ON fa_max_interactions (person_id)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_interaction_person_seq ON fa_max_interactions (person_id, seq)",

    # ------------------------------------------------------------------ #
    # 5. fa_max_property_associations (temporal, integer FK -> properties) #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fa_max_property_associations (
        id             BIGSERIAL   PRIMARY KEY,
        person_id      UUID        NOT NULL REFERENCES fa_max_persons(person_id),
        property_id    INTEGER     NOT NULL REFERENCES properties(id),
        opportunity_id UUID        REFERENCES fa_max_opportunities(opportunity_id),
        role           VARCHAR(30) NOT NULL DEFAULT 'subject',
        valid_from     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        valid_to       TIMESTAMPTZ,
        source         VARCHAR(60),
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_prop_assoc_role
            CHECK (role IN ('subject','collateral','current_project','exit_property','owned'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_prop_assoc_person_id ON fa_max_property_associations (person_id)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_prop_assoc_property_id ON fa_max_property_associations (property_id)",
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_prop_assoc_current
        ON fa_max_property_associations (person_id, property_id)
        WHERE valid_to IS NULL
    """,

    # One sequence shared by every source in the unified borrower timeline.
    # Per-table BIGSERIAL values cannot safely serve as a union cursor because
    # identical values occur in each independent sequence.
    "CREATE SEQUENCE IF NOT EXISTS fa_max_timeline_seq",
    "ALTER TABLE fa_max_state_transition_events ADD COLUMN IF NOT EXISTS timeline_seq BIGINT",
    "ALTER TABLE fa_max_interactions ADD COLUMN IF NOT EXISTS timeline_seq BIGINT",
    "ALTER TABLE fa_max_property_associations ADD COLUMN IF NOT EXISTS timeline_seq BIGINT",
    "ALTER TABLE fa_max_state_transition_events ALTER COLUMN timeline_seq SET DEFAULT nextval('fa_max_timeline_seq')",
    "ALTER TABLE fa_max_interactions ALTER COLUMN timeline_seq SET DEFAULT nextval('fa_max_timeline_seq')",
    "ALTER TABLE fa_max_property_associations ALTER COLUMN timeline_seq SET DEFAULT nextval('fa_max_timeline_seq')",
    # A prior migration run may already have installed immutability triggers.
    # Remove them during the controlled backfill; they are recreated below.
    "DROP TRIGGER IF EXISTS trg_fa_max_ste_immutable ON fa_max_state_transition_events",
    "DROP TRIGGER IF EXISTS trg_fa_max_interactions_immutable ON fa_max_interactions",
    "SELECT set_config('fa_max.allow_state_write', 'on', true)",
    "UPDATE fa_max_state_transition_events SET timeline_seq = nextval('fa_max_timeline_seq') WHERE timeline_seq IS NULL",
    "UPDATE fa_max_interactions SET timeline_seq = nextval('fa_max_timeline_seq') WHERE timeline_seq IS NULL",
    "UPDATE fa_max_property_associations SET timeline_seq = nextval('fa_max_timeline_seq') WHERE timeline_seq IS NULL",
    "SELECT set_config('fa_max.allow_state_write', 'off', true)",
    "ALTER TABLE fa_max_state_transition_events ALTER COLUMN timeline_seq SET NOT NULL",
    "ALTER TABLE fa_max_interactions ALTER COLUMN timeline_seq SET NOT NULL",
    "ALTER TABLE fa_max_property_associations ALTER COLUMN timeline_seq SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_ste_person_timeline_seq ON fa_max_state_transition_events (person_id, timeline_seq)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_interaction_person_timeline_seq ON fa_max_interactions (person_id, timeline_seq)",
    "CREATE INDEX IF NOT EXISTS ix_fa_max_prop_assoc_person_timeline_seq ON fa_max_property_associations (person_id, timeline_seq)",

    # ------------------------------------------------------------------ #
    # 6. DB-enforced immutability trigger on event spine + interactions   #
    #    The trigger checks the session-local GUC fa_max.allow_state_write.#
    #    Only transition() sets this GUC (via SET LOCAL); everything else  #
    #    gets a clean block.                                               #
    # ------------------------------------------------------------------ #
    """
    CREATE OR REPLACE FUNCTION fa_max_guard_immutable_row()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        IF current_setting('fa_max.allow_state_write', true) <> 'on' THEN
            RAISE EXCEPTION
                'fa_max: direct UPDATE/DELETE on % is forbidden. '
                'State changes must go through transition(). '
                '(fa_max.allow_state_write is not set for this session.)',
                TG_TABLE_NAME;
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END;
    $$
    """,

    # State columns may change only while transition() owns the write gate.
    """
    CREATE OR REPLACE FUNCTION fa_max_guard_state_write()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        IF current_setting('fa_max.allow_state_write', true) <> 'on' THEN
            RAISE EXCEPTION
                'fa_max: direct state update on % is forbidden; use transition()',
                TG_TABLE_NAME;
        END IF;
        RETURN NEW;
    END;
    $$
    """,
    "DROP TRIGGER IF EXISTS trg_fa_max_person_state_guard ON fa_max_persons",
    "CREATE TRIGGER trg_fa_max_person_state_guard BEFORE UPDATE OF lifecycle_state, state_version ON fa_max_persons FOR EACH ROW EXECUTE FUNCTION fa_max_guard_state_write()",
    "DROP TRIGGER IF EXISTS trg_fa_max_opportunity_state_guard ON fa_max_opportunities",
    "CREATE TRIGGER trg_fa_max_opportunity_state_guard BEFORE UPDATE OF current_stage, state_version ON fa_max_opportunities FOR EACH ROW EXECUTE FUNCTION fa_max_guard_state_write()",
    "DROP TRIGGER IF EXISTS trg_fa_max_partner_state_guard ON fa_max_partners",
    "CREATE TRIGGER trg_fa_max_partner_state_guard BEFORE UPDATE OF status, state_version ON fa_max_partners FOR EACH ROW EXECUTE FUNCTION fa_max_guard_state_write()",

    # Install on the event spine.
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_fa_max_ste_immutable'
              AND tgrelid = 'fa_max_state_transition_events'::regclass
        ) THEN
            CREATE TRIGGER trg_fa_max_ste_immutable
            BEFORE UPDATE OR DELETE ON fa_max_state_transition_events
            FOR EACH ROW EXECUTE FUNCTION fa_max_guard_immutable_row();
        END IF;
    END;
    $$
    """,

    # Install on interactions.
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_fa_max_interactions_immutable'
              AND tgrelid = 'fa_max_interactions'::regclass
        ) THEN
            CREATE TRIGGER trg_fa_max_interactions_immutable
            BEFORE UPDATE OR DELETE ON fa_max_interactions
            FOR EACH ROW EXECUTE FUNCTION fa_max_guard_immutable_row();
        END IF;
    END;
    $$
    """,

    # ------------------------------------------------------------------ #
    # 7. fa_max_work_queue                                                #
    # ------------------------------------------------------------------ #
    """
    CREATE TABLE IF NOT EXISTS fa_max_work_queue (
        work_item_id    UUID        PRIMARY KEY DEFAULT generate_uuidv7(),
        person_id       UUID        REFERENCES fa_max_persons(person_id),
        queue_name      VARCHAR(60) NOT NULL,
        payload         JSONB       NOT NULL DEFAULT '{}',
        status          VARCHAR(20) NOT NULL DEFAULT 'available',
        idempotency_key VARCHAR(255),
        available_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        claimed_at      TIMESTAMPTZ,
        lease_expires_at TIMESTAMPTZ,
        done_at         TIMESTAMPTZ,
        attempt_count   INTEGER     NOT NULL DEFAULT 0,
        worker_id       VARCHAR(120),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fa_max_work_queue_status
            CHECK (status IN ('available','claimed','done','failed'))
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_fa_max_work_queue_idempotency
        ON fa_max_work_queue (idempotency_key)
        WHERE idempotency_key IS NOT NULL
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_work_queue_queue_name ON fa_max_work_queue (queue_name)",
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_work_queue_claimable
        ON fa_max_work_queue (queue_name, available_at)
        WHERE status = 'available'
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_work_queue_expired_leases
        ON fa_max_work_queue (lease_expires_at)
        WHERE status = 'claimed'
    """,
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()

        tables = [
            "fa_max_person_lifecycle_stage_config",
            "fa_max_persons",
            "fa_max_opportunities",
            "fa_max_partners",
            "fa_max_interactions",
            "fa_max_property_associations",
            "fa_max_work_queue",
        ]
        for table in tables:
            count = db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {count} rows")

        stage_count = db.execute(
            text("SELECT COUNT(*) FROM fa_max_person_lifecycle_stage_config")
        ).scalar()
        print(f"  SOT stages loaded: {stage_count} (expected 15)")

    print("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
