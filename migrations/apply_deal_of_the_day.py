"""
T-B12-07 — Deal-of-the-day table.

Lightweight daily-pick table: one row per calendar date, the top-CDS
qualified lead not yet delivered, with a 24h exclusive-unlock window.
Backs src.services.deal_of_the_day (selection) and the
/api/deal-of-the-day endpoint (surface).

NOT applied by this agent — flagged for manual review since this is a
shared Postgres used by dev/test/prod (single-shared-db). Run once:

    PYTHONPATH=. python migrations/apply_deal_of_the_day.py

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS deal_of_the_day (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        lead_id INTEGER NOT NULL REFERENCES properties(id),
        window_start TIMESTAMPTZ NOT NULL,
        window_end TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_deal_of_the_day_date UNIQUE (date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_deal_of_the_day_window ON deal_of_the_day (window_start, window_end)",
    "CREATE INDEX IF NOT EXISTS ix_deal_of_the_day_lead_id ON deal_of_the_day (lead_id)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'deal_of_the_day'
            ORDER BY ordinal_position
        """)).fetchall()
    print("deal_of_the_day columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
