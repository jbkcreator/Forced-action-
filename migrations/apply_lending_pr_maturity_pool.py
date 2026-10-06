"""Widen the lending.calling_pool_staging pool_name CHECK to allow 'pr_maturity'.

The PropertyRadar maturity bridge (src/lending/pr_maturity_bridge.py) writes a
sixth pool into the same staging table the dialer reads. The table's pool_name
CHECK rejects any value outside the Wave 0 set, so it has to be widened before
the bridge can insert a row.

Idempotent: re-running drops and recreates the named constraint with the full
set. Guarded on the table existing — this migration stacks on
apply_lending_calling_pool_staging.py (WP-W0-1); it is a no-op until that ran.
"""
from sqlalchemy import text

from src.core.database import get_db_context

_CONSTRAINT = "lending_calling_pool_staging_pool_name_check"
_ALLOWED = (
    "wholesaler_flipper",
    "active_builder",
    "mortgage_broker",
    "auction_winner",
    "permit_owner",
    "pr_maturity",
)


def run() -> None:
    values = ", ".join(f"'{v}'" for v in _ALLOWED)
    with get_db_context() as session:
        exists = session.execute(
            text("SELECT to_regclass('lending.calling_pool_staging')")
        ).scalar()
        if exists is None:
            print(
                "apply_lending_pr_maturity_pool: lending.calling_pool_staging not found "
                "(run apply_lending_calling_pool_staging.py first) — skipped"
            )
            return
        # The base table creates an inline (auto-named) CHECK; the model names it.
        # Drop whichever is present, then add the canonical named one.
        session.execute(
            text(f"ALTER TABLE lending.calling_pool_staging DROP CONSTRAINT IF EXISTS {_CONSTRAINT}")
        )
        session.execute(
            text(
                "ALTER TABLE lending.calling_pool_staging DROP CONSTRAINT IF EXISTS "
                "calling_pool_staging_pool_name_check"
            )
        )
        session.execute(
            text(
                f"ALTER TABLE lending.calling_pool_staging ADD CONSTRAINT {_CONSTRAINT} "
                f"CHECK (pool_name IN ({values}))"
            )
        )
        session.commit()
    print(f"apply_lending_pr_maturity_pool: {_CONSTRAINT} now allows {_ALLOWED}")


if __name__ == "__main__":
    run()
