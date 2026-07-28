"""
Backfill vertical='solar' for existing CVC (solar contractor) DBPR rows.

CVC was added to _LICENSE_TO_VERTICAL in src/scrappers/dbpr/dbpr_engine.py
after these rows were already ingested with vertical=NULL. This is a
one-time catch-up for rows loaded before the mapping existed; the scraper
maps CVC -> solar for every row ingested from here on.

    PYTHONPATH=. python migrations/apply_solar_cvc_vertical_backfill.py

Idempotent: the WHERE vertical IS NULL clause means re-running after the
first successful run updates 0 rows.
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context


def main() -> int:
    with get_db_context() as db:
        result = db.execute(text("""
            UPDATE dbpr_contacts
            SET vertical = 'solar'
            WHERE license_type_code = 'CVC' AND vertical IS NULL
        """))
        db.commit()
        print(f"solar_cvc_vertical_backfill: updated {result.rowcount} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
