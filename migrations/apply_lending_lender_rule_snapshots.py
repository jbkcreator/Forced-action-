"""R1 item 0.7 — versioned lender rule snapshots.

Creates ``lending.lender_rule_snapshots`` (append-only: a trigger refuses UPDATE and DELETE) and a statement-level
trigger on ``lending.lender_rule_values`` that writes the full rule set as a new snapshot after every INSERT, UPDATE
or DELETE, so a hand-edited SQL change (the runbook's way) is versioned like any other.  An edit that leaves the
rules unchanged writes nothing.  Who and why come from the session settings ``lending.rule_actor`` and
``lending.rule_reason`` (``SET LOCAL``), else the database user and "table edit".

Seeds version 1 from the table as it is now when no snapshot exists.

Idempotent; run after apply_lending_lender_rule_values.py.  Safe any time: it only adds a table and a trigger.

Usage:
    PYTHONPATH=. python migrations/apply_lending_lender_rule_snapshots.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateIndex

from config.settings import get_settings
from src.lending.lender_rules_store import SNAPSHOT_LOCK_KEY
from src.lending.models import LENDING_SCHEMA, LendingLenderRuleSnapshot

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_WRITE_FUNCTION = """
CREATE OR REPLACE FUNCTION "{s}".write_lender_rule_snapshot(p_actor text, p_reason text) RETURNS integer AS $fn$
DECLARE
    snap jsonb;
    last_rules jsonb;
    last_no integer;
BEGIN
    PERFORM pg_advisory_xact_lock({key});
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'lender_key', lender_key, 'field', field, 'value', value, 'status', status, 'note', note,
               'updated_by', updated_by, 'effective_from', effective_from) ORDER BY lender_key, field), '[]'::jsonb)
      INTO snap FROM "{s}".lender_rule_values;
    SELECT version_no, rules INTO last_no, last_rules FROM "{s}".lender_rule_snapshots ORDER BY version_no DESC LIMIT 1;
    IF last_no IS NOT NULL AND last_rules IS NOT DISTINCT FROM snap THEN
        RETURN last_no;
    END IF;
    last_no := COALESCE(last_no, 0) + 1;
    INSERT INTO "{s}".lender_rule_snapshots (version_no, rules, created_by, reason)
    VALUES (last_no, snap, left(p_actor, 80), left(p_reason, 200));
    RETURN last_no;
END
$fn$ LANGUAGE plpgsql
"""

_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION "{s}".snapshot_lender_rule_values() RETURNS trigger AS $fn$
BEGIN
    IF current_setting('lending.rule_snapshot_skip', true) = 'on' THEN
        RETURN NULL;
    END IF;
    PERFORM "{s}".write_lender_rule_snapshot(
        COALESCE(NULLIF(current_setting('lending.rule_actor', true), ''), session_user),
        COALESCE(NULLIF(current_setting('lending.rule_reason', true), ''), 'table edit'));
    RETURN NULL;
END
$fn$ LANGUAGE plpgsql
"""

_LOCK_FUNCTION = """
CREATE OR REPLACE FUNCTION "{s}".lock_lender_rule_values() RETURNS trigger AS $fn$
BEGIN
    PERFORM pg_advisory_xact_lock({key});
    RETURN NULL;
END
$fn$ LANGUAGE plpgsql
"""

_APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION "{s}".lender_rule_snapshots_append_only() RETURNS trigger AS $fn$
BEGIN
    RAISE EXCEPTION 'lender_rule_snapshots is append-only';
END
$fn$ LANGUAGE plpgsql
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    table = LendingLenderRuleSnapshot.__table__
    table.create(translated, checkfirst=True)
    for index in table.indexes:
        translated.execute(CreateIndex(index, if_not_exists=True))

    for ddl in (_WRITE_FUNCTION, _TRIGGER_FUNCTION, _LOCK_FUNCTION, _APPEND_ONLY_FUNCTION):
        conn.exec_driver_sql(ddl.format(s=schema, key=SNAPSHOT_LOCK_KEY))
    conn.exec_driver_sql(f'DROP TRIGGER IF EXISTS lender_rule_snapshots_append_only ON "{schema}".lender_rule_snapshots')
    conn.exec_driver_sql(
        f'CREATE TRIGGER lender_rule_snapshots_append_only BEFORE UPDATE OR DELETE ON "{schema}".lender_rule_snapshots '
        f'FOR EACH ROW EXECUTE FUNCTION "{schema}".lender_rule_snapshots_append_only()')
    conn.exec_driver_sql(f'DROP TRIGGER IF EXISTS lender_rule_values_lock ON "{schema}".lender_rule_values')
    conn.exec_driver_sql(
        f'CREATE TRIGGER lender_rule_values_lock BEFORE INSERT OR UPDATE OR DELETE ON "{schema}".lender_rule_values '
        f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".lock_lender_rule_values()')
    conn.exec_driver_sql(
        f'DROP TRIGGER IF EXISTS lender_rule_values_snapshot ON "{schema}".lender_rule_values')
    conn.exec_driver_sql(
        f'CREATE TRIGGER lender_rule_values_snapshot AFTER INSERT OR UPDATE OR DELETE ON "{schema}".lender_rule_values '
        f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".snapshot_lender_rule_values()')

    # Version 1 = the rules as they are now (a no-op when a snapshot already exists).
    conn.execute(text(f'SELECT "{schema}".write_lender_rule_snapshot(:actor, :reason) '
                      f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}".lender_rule_snapshots)'),
                 {"actor": "migration", "reason": "baseline when snapshots were introduced"})


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        apply_to(conn, schema)
    logger.info("apply_lending_lender_rule_snapshots complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
