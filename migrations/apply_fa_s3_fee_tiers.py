"""
Task 3.2 — deal-size fee tiers on commission_splits (ADR 0031).

Adds min_gross_cents / max_gross_cents to commission_splits so a split can be
scoped to a gross-amount band. resolve_split_config() picks the highest-floor
active tier where min_gross_cents <= gross < max_gross_cents (max NULL =
unbounded). A new tier is a new row — no further schema change.

The existing placeholder split (platform_50_broker_50) is set to the catch-all
band [0, +inf) so it remains the default until real tiers are seeded — behavior
is unchanged (every deal still resolves to 50/50) until a product-owner inserts
narrower tiers with their signed-off percentages.

Idempotent: ADD COLUMN IF NOT EXISTS; the catch-all UPDATE only touches the
placeholder row and is safe to re-run.

    PYTHONPATH=. python migrations/apply_fa_s3_fee_tiers.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    "ALTER TABLE commission_splits ADD COLUMN IF NOT EXISTS min_gross_cents BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE commission_splits ADD COLUMN IF NOT EXISTS max_gross_cents BIGINT",
    # Speeds up resolve_split_config's band lookup on active tiers.
    "CREATE INDEX IF NOT EXISTS ix_commission_splits_tier "
    "ON commission_splits (is_active, min_gross_cents)",
    # Existing placeholder becomes the catch-all default band [0, +inf).
    "UPDATE commission_splits "
    "SET min_gross_cents = 0, max_gross_cents = NULL "
    "WHERE split_config_id = 'platform_50_broker_50'",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'commission_splits' "
            "AND column_name IN ('min_gross_cents','max_gross_cents') "
            "ORDER BY column_name"
        )).fetchall()
    print("commission_splits tier columns:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
