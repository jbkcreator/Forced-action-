"""R-1 Overflow Router: idempotent schema migration.

Tables added:
  lending.lender_routing_rules   - one row per lender, Josh edits via admin API
  lending.lender_product_boxes   - per-lender eligibility rules per product group
  lending.file_routing           - 1:1 with lending.lendingflow_leads; routing state
  lending.file_routing_events    - append-only audit log (DELETE/UPDATE blocked by trigger)
  lending.decline_reason_codes   - Josh's decline reason enum, stored as data

Run:  PYTHONPATH=. python migrations/apply_lending_lender_routing.py

Idempotent: every statement uses IF NOT EXISTS or IF column/constraint/trigger
does not already exist.  Safe to re-run against a live database.
"""
import logging
import sys

from sqlalchemy import create_engine, text

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
        OR NEW.lender_id     IS DISTINCT FROM OLD.lender_id
        OR NEW.lender_name   IS DISTINCT FROM OLD.lender_name
        OR NEW.action        IS DISTINCT FROM OLD.action
        OR NEW.reason_code   IS DISTINCT FROM OLD.reason_code
        OR NEW.free_text     IS DISTINCT FROM OLD.free_text
        OR NEW.actor         IS DISTINCT FROM OLD.actor
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
]

SEED_REASONS = [
    ("unspecified", "Not specified"),
    ("ltv", "LTV out of range"),
    ("credit", "Credit too low"),
    ("experience", "Insufficient borrower experience"),
    ("product", "Product mismatch"),
    ("geography", "Property outside footprint"),
    ("amount", "Loan amount out of range"),
    ("cap_exceeded", "Monthly cap reached"),
    ("other", "Other (see free text)"),
]


def apply_to(conn) -> None:
    """Execute all DDL + seed data against an existing connection/transaction.

    Called by tests that wrap everything in a rolled-back transaction.
    Does NOT commit — the caller is responsible.
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


def main() -> None:
    engine = create_engine(str(get_settings().database_url), pool_pre_ping=True)
    with engine.connect() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
            conn.commit()
        log.info("DDL applied")

        # Seed decline reason codes (idempotent: ON CONFLICT DO NOTHING)
        for code, label in SEED_REASONS:
            conn.execute(
                text(
                    "INSERT INTO lending.decline_reason_codes (code, label)"
                    " VALUES (:code, :label)"
                    " ON CONFLICT (code) DO NOTHING"
                ),
                {"code": code, "label": label},
            )
        conn.commit()
        log.info("Seed data applied")

    log.info("apply_lending_lender_routing: done")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Migration failed: %s", exc)
        sys.exit(1)
