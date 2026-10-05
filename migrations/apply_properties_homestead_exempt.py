"""
properties.homestead_exempt has been declared in src/core/models.py (line 357)
with no corresponding apply script anywhere in migrations/ or scripts/ — i.e. it
may never have actually been applied to the shared DB, despite the model
declaring it. F8 (Josh, Oct 4 answers §2, "homestead is out") now reads this
column in the lending pool extractors, so it must exist for real, not just in
the ORM model.

Idempotent: safe to re-run (ADD COLUMN IF NOT EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_properties_homestead_exempt.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    "ALTER TABLE properties ADD COLUMN IF NOT EXISTS homestead_exempt BOOLEAN DEFAULT false",
]


def run() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_properties_homestead_exempt: done")


if __name__ == "__main__":
    run()
