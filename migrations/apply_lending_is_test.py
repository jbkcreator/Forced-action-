"""Global is_test flag (Q35): ``is_test BOOLEAN NOT NULL DEFAULT false`` on the tables test traffic lands in, and
``lending.test_data_cleanup_log`` (one row per row tagged, so every cleanup can be reviewed and undone) and
``lending.test_data_deleted_rows`` (a JSON copy of every test row the cleanup deletes, so a deletion can be undone).

Flagged: lending.contacts, lending.web_leads, lending.lendingflow_leads, lending.file_routing, lending.loan_files,
lending.partners, lending.suppression_list, public.checkout_provisioning_failures, public.consent_acceptances,
public.non_buyer_nurture_sequences, public.checkout_recovery, public.email_opt_outs. (public.subscribers already has it.) The routing event log
has no column: its immutability trigger blocks updates, so events are excluded through their file_routing row.

A table that does not exist yet (loan_files / partners need the P-1 migration) is skipped. Idempotent. A short lock
timeout makes the migration give up rather than queue live jobs behind it; re-run it if it times out.

Usage:
    PYTHONPATH=. python migrations/apply_lending_is_test.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingTestDataCleanupLog, LendingTestDataDeletedRow

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FLAGGED_TABLES: tuple[str, ...] = (
    f"{LENDING_SCHEMA}.contacts",
    f"{LENDING_SCHEMA}.web_leads",
    f"{LENDING_SCHEMA}.lendingflow_leads",
    f"{LENDING_SCHEMA}.file_routing",
    f"{LENDING_SCHEMA}.loan_files",
    f"{LENDING_SCHEMA}.partners",
    "public.checkout_provisioning_failures",
    "public.consent_acceptances",
    "public.non_buyer_nurture_sequences",
    "public.checkout_recovery",
    "public.email_opt_outs",
    f"{LENDING_SCHEMA}.suppression_list",
)


def apply_to(conn: Connection) -> list[str]:
    """Run on ``conn`` (the caller owns the transaction, so tests can roll it back). Returns the tables flagged."""
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingTestDataCleanupLog.__table__.create(bind=conn, checkfirst=True)
    LendingTestDataDeletedRow.__table__.create(bind=conn, checkfirst=True)
    flagged = []
    for table in FLAGGED_TABLES:
        if conn.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": table}).scalar() is not True:
            logger.info("%s does not exist yet: skipped", table)
            continue
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS is_test BOOLEAN NOT NULL DEFAULT false"))
        flagged.append(table)
    return flagged


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        flagged = apply_to(conn)
    logger.info("apply_lending_is_test complete: %s", ", ".join(flagged))


if __name__ == "__main__":
    apply()
