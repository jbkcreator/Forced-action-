"""R-1 Overflow Router: idempotent schema migration.

Tables added:
  lending.lender_routing_rules   - one row per lender, Josh edits via admin API
  lending.lender_product_boxes   - per-lender eligibility rules per product group
  lending.file_routing           - 1:1 with lending.lendingflow_leads; routing state
  lending.file_routing_events    - append-only audit log (DELETE/UPDATE blocked by trigger)
  lending.decline_reason_codes   - Josh's decline reason enum, stored as data

Run:  PYTHONPATH=. python migrations/apply_lending_lender_routing.py
Run it after apply_lending_lendingflow.py and apply_lending_partner_door.py (a partner file is routed through
``loan_file_id``).  If it runs before the Partner Door migration the tables are still created, without the
foreign keys to lending.loan_files; run it again afterwards to add them.

Idempotent: every statement uses IF NOT EXISTS or IF column/constraint/trigger
does not already exist.  Safe to re-run against a live database.
"""
import logging
import sys

from sqlalchemy import create_engine, text

from config.lending_routing import LEGACY_PLACEHOLDER_REASONS, SEED_DECLINE_REASONS
from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

DDL = [
    # ── decline reason codes ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lending.decline_reason_codes (
        code        VARCHAR(50)  PRIMARY KEY,
        label       VARCHAR(200) NOT NULL,
        active      BOOLEAN      NOT NULL DEFAULT TRUE
    )
    """,
    # ── lender routing rules ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lending.lender_routing_rules (
        id                     SERIAL       PRIMARY KEY,
        name                   VARCHAR(100) NOT NULL,
        active                 BOOLEAN      NOT NULL DEFAULT TRUE,
        is_primary             BOOLEAN      NOT NULL DEFAULT FALSE,
        agreement_status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
        priority               INTEGER      NOT NULL,
        monthly_cap            INTEGER,
        construction_min_amount BIGINT,
        points_paid_to_broker  NUMERIC(5,4),
        takes_ground_up        BOOLEAN      NOT NULL DEFAULT FALSE,
        created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_lrr_agreement CHECK (
            agreement_status IN ('signed','pending','none','terminated')
        )
    )
    """,
    # Unique priority index (ties are broken by id, so priority must be unique)
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'lending'
              AND tablename  = 'lender_routing_rules'
              AND indexname  = 'uq_lender_routing_rules_priority'
        ) THEN
            CREATE UNIQUE INDEX uq_lender_routing_rules_priority
                ON lending.lender_routing_rules (priority)
                WHERE active;
        END IF;
    END $$
    """,
    # At most one primary lender at any time
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'lending'
              AND tablename  = 'lender_routing_rules'
              AND indexname  = 'uq_lender_routing_rules_primary'
        ) THEN
            CREATE UNIQUE INDEX uq_lender_routing_rules_primary
                ON lending.lender_routing_rules (is_primary)
                WHERE is_primary;
        END IF;
    END $$
    """,
    # ── lender product boxes ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lending.lender_product_boxes (
        id                        SERIAL      PRIMARY KEY,
        lender_id                 INTEGER     NOT NULL
                                  REFERENCES lending.lender_routing_rules(id)
                                  ON DELETE CASCADE,
        loan_types                TEXT[]      NOT NULL,
        min_loan_amount           BIGINT,
        max_loan_amount           BIGINT,
        approved_states           TEXT[],
        credit_floor              INTEGER,
        max_ltv                   NUMERIC(5,4),
        max_ltc                   NUMERIC(5,4),
        min_completed_projects    INTEGER     NOT NULL DEFAULT 0,
        requires_ground_up_build  BOOLEAN     NOT NULL DEFAULT FALSE,
        created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'lending'
              AND tablename  = 'lender_product_boxes'
              AND indexname  = 'idx_lpb_lender'
        ) THEN
            CREATE INDEX idx_lpb_lender ON lending.lender_product_boxes (lender_id);
        END IF;
    END $$
    """,
    # ── file routing state ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lending.file_routing (
        id                  SERIAL       PRIMARY KEY,
        lead_id             INTEGER      NOT NULL UNIQUE
                            REFERENCES lending.lendingflow_leads(id),
        status              VARCHAR(20)  NOT NULL DEFAULT 'unrouted',
        current_lender_id   INTEGER
                            REFERENCES lending.lender_routing_rules(id),
        current_lender_name VARCHAR(100),
        route_position      INTEGER,
        lenders_tried       INTEGER[]    NOT NULL DEFAULT '{}',
        hold_reason         VARCHAR(30),
        is_test             BOOLEAN      NOT NULL DEFAULT false,
        created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_file_routing_status CHECK (
            status IN ('unrouted','routed','held','exhausted')
        )
    )
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'lending'
              AND tablename  = 'file_routing'
              AND indexname  = 'idx_file_routing_status'
        ) THEN
            CREATE INDEX idx_file_routing_status
                ON lending.file_routing (status, lead_id)
                WHERE status IN ('unrouted','routed');
        END IF;
    END $$
    """,
    # ── file routing events (append-only) ────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lending.file_routing_events (
        id            BIGSERIAL    PRIMARY KEY,
        lead_id       INTEGER      NOT NULL
                      REFERENCES lending.lendingflow_leads(id),
        lender_id     INTEGER
                      REFERENCES lending.lender_routing_rules(id),
        lender_name   VARCHAR(100),
        action        VARCHAR(20)  NOT NULL,
        reason_code   VARCHAR(50),
        free_text     TEXT,
        actor         VARCHAR(100) NOT NULL DEFAULT 'system',
        slack_ts      VARCHAR(40),
        slack_posted_at TIMESTAMPTZ,
        created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fre_action CHECK (
            action IN ('routed','declined','approved','funded','held')
        )
    )
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'lending'
              AND tablename  = 'file_routing_events'
              AND indexname  = 'idx_fre_lead'
        ) THEN
            CREATE INDEX idx_fre_lead
                ON lending.file_routing_events (lead_id, created_at);
        END IF;
    END $$
    """,
    # Chunk 1 Addendum 1.2.1: why a file skipped the primary, e.g. {"overflow": true, "primary_bypass_reason": "SEAT_CAP"}.
    "ALTER TABLE lending.file_routing_events ADD COLUMN IF NOT EXISTS metadata JSONB",
    # DELETE and non-Slack UPDATE on file_routing_events are forbidden.
    """
    CREATE OR REPLACE FUNCTION lending.fre_immutable()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'file_routing_events is append-only: DELETE not permitted';
        END IF;
        -- Only slack bookkeeping columns may be updated.
        IF NEW.id            IS DISTINCT FROM OLD.id
        OR NEW.lead_id       IS DISTINCT FROM OLD.lead_id
        OR NEW.loan_file_id  IS DISTINCT FROM OLD.loan_file_id
        OR NEW.lender_id     IS DISTINCT FROM OLD.lender_id
        OR NEW.lender_name   IS DISTINCT FROM OLD.lender_name
        OR NEW.action        IS DISTINCT FROM OLD.action
        OR NEW.reason_code   IS DISTINCT FROM OLD.reason_code
        OR NEW.free_text     IS DISTINCT FROM OLD.free_text
        OR NEW.actor         IS DISTINCT FROM OLD.actor
        OR NEW.metadata      IS DISTINCT FROM OLD.metadata
        OR NEW.created_at    IS DISTINCT FROM OLD.created_at
        THEN
            RAISE EXCEPTION 'file_routing_events is append-only: only slack_ts/slack_posted_at may be updated';
        END IF;
        RETURN NEW;
    END
    $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            JOIN pg_class ON pg_class.oid = pg_trigger.tgrelid
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_namespace.nspname = 'lending'
              AND pg_class.relname = 'file_routing_events'
              AND pg_trigger.tgname = 'trg_fre_immutable'
        ) THEN
            CREATE TRIGGER trg_fre_immutable
            BEFORE DELETE OR UPDATE ON lending.file_routing_events
            FOR EACH ROW EXECUTE FUNCTION lending.fre_immutable();
        END IF;
    END $$
    """,
    # Link to the T-05 lenders whose rule values define this lender's eligibility (NULL = own boxes).
    "ALTER TABLE lending.lender_routing_rules ADD COLUMN IF NOT EXISTS rule_keys TEXT[]",
    # A file is a LendingFlow lead OR a partner file (lending.loan_files): exactly one key per row.
    "ALTER TABLE lending.file_routing ALTER COLUMN lead_id DROP NOT NULL",
    "ALTER TABLE lending.file_routing ADD COLUMN IF NOT EXISTS loan_file_id BIGINT",
    "ALTER TABLE lending.file_routing_events ALTER COLUMN lead_id DROP NOT NULL",
    "ALTER TABLE lending.file_routing_events ADD COLUMN IF NOT EXISTS loan_file_id BIGINT",
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_file_routing_one_file') THEN
            ALTER TABLE lending.file_routing
                ADD CONSTRAINT ck_file_routing_one_file CHECK ((lead_id IS NULL) <> (loan_file_id IS NULL));
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_fre_one_file') THEN
            ALTER TABLE lending.file_routing_events
                ADD CONSTRAINT ck_fre_one_file CHECK ((lead_id IS NULL) <> (loan_file_id IS NULL));
        END IF;
    END $$
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_file_routing_loan_file ON lending.file_routing (loan_file_id) WHERE loan_file_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_fre_loan_file ON lending.file_routing_events (loan_file_id, created_at)",
    # The foreign keys need the Partner Door table; where it is not installed yet they are added by a later run.
    """
    DO $$ BEGIN
        IF to_regclass('lending.loan_files') IS NOT NULL THEN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_file_routing_loan_file') THEN
                ALTER TABLE lending.file_routing ADD CONSTRAINT fk_file_routing_loan_file
                    FOREIGN KEY (loan_file_id) REFERENCES lending.loan_files (id);
            END IF;
            -- NOT VALID: the append-only log keeps events for loan files since deleted; new rows are still checked.
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fre_loan_file') THEN
                ALTER TABLE lending.file_routing_events ADD CONSTRAINT fk_fre_loan_file
                    FOREIGN KEY (loan_file_id) REFERENCES lending.loan_files (id) NOT VALID;
            END IF;
        END IF;
    END $$
    """,
]

SEED_REASONS = SEED_DECLINE_REASONS  # Josh's enum, one source: config/lending_routing.py


def apply_to(conn) -> None:
    """Execute all DDL + seed data against an existing connection/transaction.

    Called by tests that wrap everything in a rolled-back transaction.
    Does NOT commit: the caller is responsible.
    """
    for stmt in DDL:
        conn.execute(text(stmt))
    for code, label in SEED_REASONS:
        conn.execute(
            text(
                "INSERT INTO lending.decline_reason_codes (code, label)"
                " VALUES (:code, :label)"
                " ON CONFLICT (code) DO NOTHING"
            ),
            {"code": code, "label": label},
        )
    # Switch off a placeholder seeded before Josh's enum arrived, but only while it still has its placeholder
    # label: a code someone has since re-labelled or re-added is theirs, and a re-run never undoes that.
    for code, label in LEGACY_PLACEHOLDER_REASONS:
        conn.execute(
            text("UPDATE lending.decline_reason_codes SET active = FALSE"
                 " WHERE code = :code AND label = :label AND active"),
            {"code": code, "label": label},
        )
    if conn.execute(text("SELECT to_regclass('lending.loan_files')")).scalar() is None:
        log.warning(
            "lending.loan_files does not exist yet, so the routing tables have no foreign key to it. "
            "Run apply_lending_partner_door.py, then run this script again to add them."
        )


def main() -> None:
    engine = create_engine(str(get_settings().database_url), pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    log.info("apply_lending_lender_routing: done")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Migration failed: %s", exc)
        sys.exit(1)
