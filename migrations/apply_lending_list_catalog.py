"""
lending.list_catalog: a human-readable reference table for Josh's List 1-9
taxonomy, so anyone reading lending.calling_pool_staging.source_tag directly
(a BI tool, a one-off SQL query) doesn't need to read pool_extraction.py's
source_tag_for() to know what "list_4" means.

config/lending_list_catalog.py (LIST_CATALOG) is the single source of truth;
this script only materializes it into the DB and keeps it in sync on re-run
(UPSERT — not a one-time INSERT). pool_extraction.py's source_tag_for() is
still the only place that *assigns* a list_key to a staged row; this table
is documentation, not a gate.

Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS + UPSERT).

Run:
  PYTHONPATH=. python migrations/apply_lending_list_catalog.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context
from config.lending_list_catalog import LIST_CATALOG

_DDL = """
CREATE TABLE IF NOT EXISTS lending.list_catalog (
    list_key     VARCHAR(10) PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    pool_name    VARCHAR(50),
    queue        VARCHAR(50) NOT NULL
)
"""

_UPSERT = """
INSERT INTO lending.list_catalog (list_key, display_name, pool_name, queue)
VALUES (:list_key, :display_name, :pool_name, :queue)
ON CONFLICT (list_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    pool_name = EXCLUDED.pool_name,
    queue = EXCLUDED.queue
"""


def run() -> None:
    with get_db_context() as session:
        session.execute(text(_DDL))
        session.execute(
            text(_UPSERT),
            [
                {"list_key": key, "display_name": entry.display_name,
                 "pool_name": entry.pool_name, "queue": entry.queue}
                for key, entry in LIST_CATALOG.items()
            ],
        )
        session.commit()
    print(f"apply_lending_list_catalog: {len(LIST_CATALOG)} row(s) upserted")


if __name__ == "__main__":
    run()
