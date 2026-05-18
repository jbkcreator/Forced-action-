"""
One-off idempotent schema setup for fa022_county_launch.

Creates expansion_candidates and county_launch_audit tables + indexes
if they do not already exist. Safe to re-run — all DDL is guarded by
IF NOT EXISTS / conditional checks.

Usage:
    python scripts/setup_county_launch_schema.py [--dry-run]
"""
import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text
from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL statements — each is idempotent
# ---------------------------------------------------------------------------

_STEPS: list[tuple[str, str]] = [
    # --- expansion_candidates table -----------------------------------------
    (
        "Create expansion_candidates table",
        """
        CREATE TABLE IF NOT EXISTS expansion_candidates (
            id                      SERIAL PRIMARY KEY,
            county_id               VARCHAR(64)  NOT NULL UNIQUE,
            priority                INTEGER      NOT NULL DEFAULT 100,
            status                  VARCHAR(16)  NOT NULL DEFAULT 'queued',
            created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            last_slack_posted_at    TIMESTAMPTZ,
            last_slack_message_ts   VARCHAR(32),
            approved_at             TIMESTAMPTZ,
            approved_by_slack_user  VARCHAR(32),
            launched_at             TIMESTAMPTZ
        )
        """,
    ),
    (
        "Add check constraint ck_expansion_candidates_status (if missing)",
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_expansion_candidates_status'
            ) THEN
                ALTER TABLE expansion_candidates
                ADD CONSTRAINT ck_expansion_candidates_status
                CHECK (status IN ('queued','approved','launching','launched','aborted','skipped'));
            END IF;
        END$$
        """,
    ),
    (
        "Create index ix_expansion_candidates_status_priority (if missing)",
        """
        CREATE INDEX IF NOT EXISTS ix_expansion_candidates_status_priority
            ON expansion_candidates (status, priority)
        """,
    ),
    # --- county_launch_audit table ------------------------------------------
    (
        "Create county_launch_audit table",
        """
        CREATE TABLE IF NOT EXISTS county_launch_audit (
            id            BIGSERIAL PRIMARY KEY,
            county_id     VARCHAR(64) NOT NULL,
            event_type    VARCHAR(32) NOT NULL,
            actor         VARCHAR(64) NOT NULL,
            gate_snapshot JSONB,
            detail        JSONB,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    ),
    (
        "Add check constraint ck_county_launch_audit_event (if missing)",
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ck_county_launch_audit_event'
            ) THEN
                ALTER TABLE county_launch_audit
                ADD CONSTRAINT ck_county_launch_audit_event
                CHECK (event_type IN (
                    'evaluated','posted','approved','rejected',
                    'launch_started','launch_aborted_gate_red',
                    'launched','cooldown_skipped'
                ));
            END IF;
        END$$
        """,
    ),
    (
        "Create index ix_county_launch_audit_county_time (if missing)",
        """
        CREATE INDEX IF NOT EXISTS ix_county_launch_audit_county_time
            ON county_launch_audit (county_id, created_at)
        """,
    ),
]


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = 'public' AND table_name = :t"
            ")"
        ),
        {"t": table_name},
    ).scalar()
    return bool(row)


def run(dry_run: bool = False) -> None:
    db = Database()
    results: list[tuple[str, str]] = []

    with db.session_scope() as session:
        conn = session.connection()

        for label, ddl in _STEPS:
            if dry_run:
                log.info("[DRY-RUN] Would execute: %s", label)
                results.append((label, "skipped (dry-run)"))
                continue
            try:
                conn.execute(text(ddl))
                log.info("OK   %s", label)
                results.append((label, "ok"))
            except Exception as exc:
                log.error("FAIL %s — %s", label, exc)
                results.append((label, f"FAILED: {exc}"))
                raise

        # Verify tables exist after DDL (skip in dry-run)
        if not dry_run:
            for tbl in ("expansion_candidates", "county_launch_audit"):
                exists = _table_exists(conn, tbl)
                status = "present" if exists else "MISSING"
                log.info("Table %-35s %s", tbl, status)
                if not exists:
                    raise RuntimeError(f"Table {tbl} not found after DDL — aborting.")

    # Summary
    print("\n--- Summary ---")
    for label, status in results:
        print(f"  {'[DRY]' if dry_run else '[OK] ':6} {label}")
    if not dry_run:
        print("\nSchema setup complete.")
        print("Tables created/verified: expansion_candidates, county_launch_audit")
        print("Indexes: ix_expansion_candidates_status_priority, ix_county_launch_audit_county_time")
        print("Constraints: ck_expansion_candidates_status, ck_county_launch_audit_event")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply fa022 county-launch schema changes.")
    parser.add_argument("--dry-run", action="store_true", help="Print DDL steps without executing.")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
