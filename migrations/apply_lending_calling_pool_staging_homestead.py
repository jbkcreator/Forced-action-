"""
F8 (Josh, Oct 4 answers §2): "Owner is an LLC, LP or corporation, or a non owner
occupied investor. Homestead is out."

Adds lending.calling_pool_staging.homestead_exempt, sourced from the existing
properties.homestead_exempt column by the investor-owner pools (wholesaler/
flipper, active builder, permit owner, auction winner). Not populated for the
mortgage_broker pool (List 4, brokers and LOs) — that pool is a professional
referral list, never screened as a property owner.

Idempotent: safe to re-run (ADD COLUMN IF NOT EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_lending_calling_pool_staging_homestead.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "ALTER TABLE lending.calling_pool_staging ADD COLUMN IF NOT EXISTS homestead_exempt BOOLEAN",
]


def run() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_lending_calling_pool_staging_homestead: done")


if __name__ == "__main__":
    run()
