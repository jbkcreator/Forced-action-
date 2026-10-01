"""
lending.lead_call_confirmations: facts a borrower confirmed on a call.

One row per property holding the latest borrower-confirmed maturity date and
whether the decision maker was on the call (src/lending/call_confirmations.py).
Lead scoring reads these as known inputs; without a confirmation the
maturity stays an estimate and the decision maker stays missing.

Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_lending_lead_call_confirmations.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "CREATE SCHEMA IF NOT EXISTS lending;",
    """
    CREATE TABLE IF NOT EXISTS lending.lead_call_confirmations (
        property_id                 BIGINT       PRIMARY KEY,
        maturity_date               DATE,
        maturity_confirmed_at       TIMESTAMPTZ,
        decision_maker_on_call      BOOLEAN,
        decision_maker_confirmed_at TIMESTAMPTZ,
        caller_seat                 TEXT,
        source_call_ref             TEXT,
        updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_lending_lead_call_confirmations_something_confirmed
            CHECK (maturity_date IS NOT NULL OR decision_maker_on_call IS NOT NULL)
    );
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_lending_lead_call_confirmations: done")


if __name__ == "__main__":
    main()
