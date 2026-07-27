"""
T-B12-07 — Win-back offer redemption table.

Backs the actual redemption mechanism for the tier3_winback outreach
(PR #172 review): a token is minted when the reactivation message is SENT,
embedded in the outbound link, and only redeemed server-side when the
subscriber comes back through checkout — so the promised 50%-off (zip_held)
or 5-free-credits (zip_released) benefit is tied to a real reactivation,
not merely a message having been dispatched.

    PYTHONPATH=. python migrations/apply_winback_offers.py

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS winback_offers (
        id SERIAL PRIMARY KEY,
        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
        branch VARCHAR(20) NOT NULL,
        token VARCHAR(43) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        expires_at TIMESTAMPTZ NOT NULL,
        redeemed_at TIMESTAMPTZ,
        CONSTRAINT uq_winback_offers_token UNIQUE (token)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_winback_offers_token ON winback_offers (token)",
    "CREATE INDEX IF NOT EXISTS idx_winback_offers_subscriber_branch ON winback_offers (subscriber_id, branch)",
    # PR #172 follow-up review: separate from redeemed_at so a failed credit
    # grant doesn't look "done" — see WinbackOffer's docstring in models.py.
    "ALTER TABLE winback_offers ADD COLUMN IF NOT EXISTS credits_granted_at TIMESTAMPTZ",
    # Reconciliation sweep needs to cheaply find redeemed-but-not-yet-credited
    # zip_released rows.
    "CREATE INDEX IF NOT EXISTS idx_winback_offers_pending_credit ON winback_offers "
    "(branch, redeemed_at) WHERE credits_granted_at IS NULL",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'winback_offers'
            ORDER BY ordinal_position
        """)).fetchall()
    print("winback_offers columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
