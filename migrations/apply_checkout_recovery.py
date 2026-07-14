"""
Task 7 — abandoned-checkout recovery.

Creates the checkout_recovery table and widens the non_buyer_nurture_sequences
status CHECK to allow 'in_recovery' (the held-out-of-nurture state used while an
active recovery sequence owns a contact).

Idempotent: CREATE TABLE / INDEX IF NOT EXISTS; the CHECK is dropped then
re-added (drop-first makes the add re-runnable).

    PYTHONPATH=. python migrations/apply_checkout_recovery.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS checkout_recovery (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        subscriber_id INTEGER REFERENCES subscribers(id) ON DELETE SET NULL,
        phone VARCHAR(20),
        source VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        touches_sent INTEGER NOT NULL DEFAULT 0,
        resume_context JSONB,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        first_touch_at TIMESTAMPTZ,
        last_touch_at TIMESTAMPTZ,
        closed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_checkout_recovery_status CHECK (status IN ('active','recovered','failed')),
        CONSTRAINT ck_checkout_recovery_source CHECK (source IN ('session_expired','pre_payment'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_checkout_recovery_email ON checkout_recovery (email)",
    "CREATE INDEX IF NOT EXISTS ix_checkout_recovery_subscriber_id ON checkout_recovery (subscriber_id)",
    "CREATE INDEX IF NOT EXISTS ix_checkout_recovery_status ON checkout_recovery (status)",
    "CREATE INDEX IF NOT EXISTS ix_checkout_recovery_last_touch_at ON checkout_recovery (last_touch_at)",
    # Widen the nurture status CHECK to allow 'in_recovery'. Drop-first so the
    # add always applies cleanly on re-run.
    "ALTER TABLE non_buyer_nurture_sequences DROP CONSTRAINT IF EXISTS ck_non_buyer_nurture_status",
    """
    ALTER TABLE non_buyer_nurture_sequences ADD CONSTRAINT ck_non_buyer_nurture_status
    CHECK (status IN ('eligible','in_recovery','enrolled','converted','unsubscribed','bounced','removed'))
    """,
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        exists = db.execute(text(
            "SELECT to_regclass('public.checkout_recovery')"
        )).scalar()
    print("checkout_recovery table:", exists)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
