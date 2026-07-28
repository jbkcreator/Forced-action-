"""Rename every remaining "cora"-named database object to "lifecycle".

Companion to the repo-wide Cora -> Lifecycle text/identifier rename
(chore/rename-cora-to-lifecycle). The application code, ORM models, and
migration scripts under scripts/ and legacy/alembic/ have already been
updated to say "lifecycle" (models.py's __tablename__/Index/Constraint
`name=` values are the intended target state). This script brings the
LIVE database schema in line with that target state, and backfills the
handful of literal 'cora' attribution values written by rows that
predate the rename.

Idempotent: every rename is guarded by an existence check against
pg_catalog, so re-running this script after it has already applied is a
no-op. Safe to run against a database that has already been partially
or fully renamed by hand.

Usage:
    PYTHONPATH=. python migrations/apply_rename_cora_to_lifecycle.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Dynamically rename any table/column/index/constraint whose name still
# contains a "cora" token, rather than hand-listing every object — safer
# against drift between what the original migration scripts created and
# what's actually live (constraint/index names have drifted from the
# ORM's declared names before in this codebase; see idx_cora_event_queue_
# status_created vs. the model's declared idx_lifecycle_event_queue_status).
RENAME_SQL = r"""
DO $$
DECLARE
    r RECORD;
    new_name TEXT;
BEGIN
    -- Tables: cora_x -> lifecycle_x
    FOR r IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename ~ '(^|_)cora(_|$)'
    LOOP
        new_name := regexp_replace(r.tablename, '(^|_)cora(_|$)', '\1lifecycle\2');
        IF new_name <> r.tablename THEN
            EXECUTE format('ALTER TABLE %I RENAME TO %I', r.tablename, new_name);
            RAISE NOTICE 'renamed table % -> %', r.tablename, new_name;
        END IF;
    END LOOP;

    -- Columns: cora_x -> lifecycle_x, on every table
    FOR r IN
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND column_name ~ '(^|_)cora(_|$)'
    LOOP
        new_name := regexp_replace(r.column_name, '(^|_)cora(_|$)', '\1lifecycle\2');
        IF new_name <> r.column_name THEN
            EXECUTE format('ALTER TABLE %I RENAME COLUMN %I TO %I', r.table_name, r.column_name, new_name);
            RAISE NOTICE 'renamed column %.% -> %', r.table_name, r.column_name, new_name;
        END IF;
    END LOOP;

    -- Indexes: idx_cora_x / uq_cora_x -> idx_lifecycle_x / uq_lifecycle_x
    FOR r IN
        SELECT indexname FROM pg_indexes
        WHERE schemaname = 'public' AND indexname ~ '(^|_)cora(_|$)'
    LOOP
        new_name := regexp_replace(r.indexname, '(^|_)cora(_|$)', '\1lifecycle\2');
        IF new_name <> r.indexname THEN
            EXECUTE format('ALTER INDEX %I RENAME TO %I', r.indexname, new_name);
            RAISE NOTICE 'renamed index % -> %', r.indexname, new_name;
        END IF;
    END LOOP;

    -- Constraints (check/unique/foreign key): ck_cora_x, uq_cora_x, fk_cora_x, check_cora_x
    FOR r IN
        SELECT conname, conrelid::regclass::text AS tbl
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace AND conname ~ '(^|_)cora(_|$)'
    LOOP
        new_name := regexp_replace(r.conname, '(^|_)cora(_|$)', '\1lifecycle\2');
        IF new_name <> r.conname THEN
            EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I', r.tbl, r.conname, new_name);
            RAISE NOTICE 'renamed constraint % on % -> %', r.conname, r.tbl, new_name;
        END IF;
    END LOOP;
END $$;
"""

# Literal stored VALUES that predate the rename — renaming the schema above
# does nothing for existing row data. Every write path in the application
# now writes 'lifecycle' (already updated in code); these backfills bring
# historical rows in line so a query filtering on the new value doesn't
# silently miss old ones.
BACKFILL_STATEMENTS = [
    ("lifecycle_playbook", "authored_by",
     "UPDATE lifecycle_playbook SET authored_by = 'lifecycle' WHERE authored_by = 'cora'"),
    ("dfy_lite_orders", "generated_by",
     "UPDATE dfy_lite_orders SET generated_by = 'lifecycle' WHERE generated_by = 'cora'"),
    ("subscribers", "signup_source",
     "UPDATE subscribers SET signup_source = 'lifecycle_sms' WHERE signup_source = 'cora_sms'"),
]

# check_subscriber_signup_source's name carries no "cora" token, so the
# generic constraint-rename loop above never touches it — but its CHECK body
# still hard-codes the old 'cora_sms' literal (from scripts/apply_fa017_signup_source.py
# and scripts/apply_affiliate_signup_source.py, both already applied against
# prod and never re-run). Every write path now writes 'lifecycle_sms'
# (src/services/signup_engine.py, src/tasks/lifecycle_sms_sender.py), so the
# live constraint must be recreated here or those writes fail outright.
#
# Added NOT VALID: a plain ADD CONSTRAINT validates every existing row
# immediately, and existing rows still say 'cora_sms' (not in this new list)
# until the backfill below runs — that would make the DDL itself fail on any
# database with real cora_sms history. NOT VALID skips that upfront scan but
# still enforces the check on every row touched from here on (including the
# backfill UPDATE, which writes the now-allowed 'lifecycle_sms'). The
# VALIDATE CONSTRAINT step below then scans once the backfill has run, under
# a SHARE UPDATE EXCLUSIVE lock that — unlike the immediate-validate path —
# doesn't block concurrent reads/writes on subscribers.
#
# DROP and ADD are two separate statements executed one per call (not one
# ";"-joined string) — matching scripts/apply_affiliate_signup_source.py's
# existing pattern, since whether a driver accepts multiple statements in a
# single execute() depends on the DATABASE_URL scheme (postgresql+psycopg2
# vs. postgresql+psycopg, both supported per src/agents/checkpoint.py) and
# isn't worth relying on.
SIGNUP_SOURCE_CONSTRAINT_DROP_SQL = (
    "ALTER TABLE subscribers DROP CONSTRAINT IF EXISTS check_subscriber_signup_source"
)

SIGNUP_SOURCE_CONSTRAINT_ADD_SQL = r"""
ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_signup_source
    CHECK (signup_source IN (
        'direct', 'landing_page', 'dbpr_email', 'lifecycle_sms',
        'missed_call', 'referral', 'admin', 'unknown', 'affiliate'
    )) NOT VALID
"""

SIGNUP_SOURCE_CONSTRAINT_VALIDATE_SQL = (
    "ALTER TABLE subscribers VALIDATE CONSTRAINT check_subscriber_signup_source"
)


def _table_exists(engine, table_name: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT to_regclass(:t) IS NOT NULL"),
            {"t": f"public.{table_name}"},
        ).scalar()
        return bool(row)


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)

    with engine.begin() as conn:
        logger.info("Renaming cora_* schema objects to lifecycle_* ...")
        conn.execute(text(RENAME_SQL))

    if _table_exists(engine, "subscribers"):
        with engine.begin() as conn:
            # NOT VALID — must run before the signup_source backfill below.
            # See the comment on SIGNUP_SOURCE_CONSTRAINT_ADD_SQL for why.
            logger.info("Recreating check_subscriber_signup_source (NOT VALID) to allow 'lifecycle_sms' ...")
            conn.execute(text(SIGNUP_SOURCE_CONSTRAINT_DROP_SQL))
            conn.execute(text(SIGNUP_SOURCE_CONSTRAINT_ADD_SQL))

    with engine.begin() as conn:
        for table_name, column_name, sql in BACKFILL_STATEMENTS:
            if not _table_exists(engine, table_name):
                logger.info("Skipping backfill on %s.%s — table not found (nothing to do)", table_name, column_name)
                continue
            result = conn.execute(text(sql))
            logger.info("Backfilled %s.%s ('cora' -> 'lifecycle'): %d row(s)", table_name, column_name, result.rowcount)

    if _table_exists(engine, "subscribers"):
        with engine.begin() as conn:
            # Full-table scan now that legacy rows are backfilled — safe to
            # run after the fact since the constraint has been enforcing on
            # new writes since NOT VALID was added above.
            logger.info("Validating check_subscriber_signup_source now that legacy rows are backfilled ...")
            conn.execute(text(SIGNUP_SOURCE_CONSTRAINT_VALIDATE_SQL))

    logger.info("Done.")


if __name__ == "__main__":
    main()
