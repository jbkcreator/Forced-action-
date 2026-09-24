"""Apply WP-8B manual ARV override with audit trail.

WP-8B's scope requires "allow reviewed manual override with audit trail"
(SOT.md's general architecture principle: "every state transition written,
timestamped, and attributed... every merge reversible and logged"). Neither
the schema nor the code had any override mechanism — status CHECK only
allowed 'computed'/'superseded' (see apply_wp8b_arv_persistence.py).

Adds, alongside the original computed low/point/high/confidence/
selected_comps (never modified by an override — the audit trail sits next
to the original computation, it does not erase it):

  override_low / override_point / override_high  -- human-supplied
                                                      replacement figures
  override_reason                                 -- required whenever an
                                                      override is set
                                                      (enforced in
                                                      arv_persistence.py,
                                                      not the DB)
  overridden_by / overridden_at                   -- who and when

Widens ck_fa_max_arv_status to allow 'overridden'.

Idempotent -- IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_wp8b_arv_override.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS override_low NUMERIC(14, 2);",
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS override_point NUMERIC(14, 2);",
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS override_high NUMERIC(14, 2);",
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS override_reason TEXT;",
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS overridden_by TEXT;",
    "ALTER TABLE fa_max_arv_results ADD COLUMN IF NOT EXISTS overridden_at TIMESTAMPTZ;",
]


def run(conn) -> None:
    for i, stmt in enumerate(DDL, 1):
        logger.info("DDL step %d/%d", i, len(DDL))
        conn.execute(text(stmt))

    existing_name = conn.execute(
        text(
            "SELECT con.conname "
            "FROM pg_constraint con "
            "JOIN pg_class rel ON rel.oid = con.conrelid "
            "WHERE rel.relname = 'fa_max_arv_results' AND con.contype = 'c' "
            "AND pg_get_constraintdef(con.oid) LIKE '%%status%%'"
        )
    ).scalar()
    if existing_name:
        logger.info("dropping existing status CHECK constraint %r", existing_name)
        conn.execute(text(f'ALTER TABLE fa_max_arv_results DROP CONSTRAINT "{existing_name}"'))
    conn.execute(
        text(
            "ALTER TABLE fa_max_arv_results ADD CONSTRAINT ck_fa_max_arv_status "
            "CHECK (status IN ('computed', 'superseded', 'overridden'));"
        )
    )
    logger.info("wp8b_arv_override migration complete.")


if __name__ == "__main__":
    engine = create_engine(str(get_settings().database_url))
    with engine.begin() as conn:
        run(conn)
