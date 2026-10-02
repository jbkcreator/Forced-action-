"""Remove the dialer-load audit columns from the FA Max Backflip audit table.

Idempotent. Safe to re-run (DROP COLUMN IF EXISTS; the constraint is dropped
and re-created).

An earlier revision of apply_backflip_conflict_identifiers.py added a
'dialer' gate plus subject_ref and matched_criteria to
fa_max_backflip_suppression_decisions. Conflict-check decisions are lending
data and belong in the isolated lending schema (lending.load_exclusions), so
this restores the FA Max table to its draft/send-only shape.

Refuses to run while any 'dialer' row exists: those rows would violate the
restored constraint, and removing them would destroy audit records.
"""
from __future__ import annotations

from sqlalchemy import text

from src.core.database import get_db_context

DIALER_ROW_COUNT_SQL = """
    SELECT count(*) FROM fa_max_backflip_suppression_decisions WHERE gate = 'dialer'
"""

STATEMENTS: list[tuple[str, str]] = [
    (
        "RESTORE draft/send gate + DROP subject_ref, matched_criteria on fa_max_backflip_suppression_decisions",
        """
        ALTER TABLE fa_max_backflip_suppression_decisions
            DROP CONSTRAINT IF EXISTS ck_fa_max_bsd_gate;
        ALTER TABLE fa_max_backflip_suppression_decisions
            ADD CONSTRAINT ck_fa_max_bsd_gate
                CHECK (gate IN ('draft', 'send'));
        ALTER TABLE fa_max_backflip_suppression_decisions
            DROP COLUMN IF EXISTS subject_ref,
            DROP COLUMN IF EXISTS matched_criteria;
        """,
    ),
]


def run() -> None:
    with get_db_context() as session:
        dialer_rows = session.execute(text(DIALER_ROW_COUNT_SQL)).scalar_one()
        if dialer_rows:
            raise RuntimeError(
                f"{dialer_rows} 'dialer' audit row(s) exist; refusing to drop the columns that hold them"
            )
        for label, sql in STATEMENTS:
            print(f"  apply: {label}")
            session.execute(text(sql))
    print("apply_backflip_dialer_audit_removal: done")


if __name__ == "__main__":
    run()
