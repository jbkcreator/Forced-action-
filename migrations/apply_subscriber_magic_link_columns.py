"""
Apply scripts/sql/add_subscriber_magic_link_columns.sql to the shared DB.

The magic-link feature (PR #112) shipped a raw idempotent .sql with no Python
runner, so it was never applied — leaving dev's Subscriber model referencing
columns the shared DB lacks (magic_link_hash/expires_at/used_at). This wraps
that SQL so it runs through the standard migrations/ path.

Idempotent (ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).

    PYTHONPATH=. python migrations/apply_subscriber_magic_link_columns.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

SQL_FILE = "scripts/sql/add_subscriber_magic_link_columns.sql"


def main() -> int:
    with open(SQL_FILE, encoding="utf-8") as fh:
        raw = fh.read()
    # Drop comment lines first — otherwise the leading -- header stays glued to
    # the first ALTER and a naive "startswith('--')" filter would skip it.
    code = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--"))
    statements = [s.strip() for s in code.split(";") if s.strip()]
    with get_db_context() as db:
        for stmt in statements:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='subscribers' AND column_name LIKE 'magic_link%'
            ORDER BY column_name
        """)).fetchall()
    print(f"applied {len(statements)} statements")
    print("magic_link columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
