"""
QUALITY-v2.2 Q1 — fleet event-trigger dispatcher schema.

Three tables, modeled on the existing transactional-outbox pair
(`events`/`processed_events`/`event_failures` in src/core/models.py) but
deliberately separate:
  - `events` has a CHECK constraint enumerating a closed list of
    prospect-lifecycle event types and a NOT NULL prospect_id FK — neither
    fits a fleet-wide event (a Stripe cancellation or a Dev-Shop finding
    has no prospect_id). Reusing it would mean altering a constraint relied
    on by ~12 existing callers (skip-trace, wallet, prospect intake, etc.).
  - `fleet_events` adds `priority` (lower = more urgent), the mechanism
    behind spec §9.5's "deadline-aware preemption" — the dispatch query
    orders by (priority, occurred_at) instead of pure FIFO.

    PYTHONPATH=. python migrations/apply_fleet_events.py

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS fleet_events (
        id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(40) NOT NULL,
        priority SMALLINT NOT NULL DEFAULT 100,
        source_component VARCHAR(60) NOT NULL,
        subscriber_id INTEGER REFERENCES subscribers(id),
        opportunity_thread_id VARCHAR(20),
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_fleet_events_event_type CHECK (event_type IN (
            'filing.new', 'payment.received', 'reply.received',
            'booking.created', 'subscription.cancelled', 'source.failure'
        )),
        CONSTRAINT ck_fleet_events_priority CHECK (priority >= 0)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fleet_events_type ON fleet_events (event_type)",
    "CREATE INDEX IF NOT EXISTS idx_fleet_events_priority_occurred ON fleet_events (priority, occurred_at)",
    "CREATE INDEX IF NOT EXISTS idx_fleet_events_subscriber ON fleet_events (subscriber_id)",
    """
    CREATE TABLE IF NOT EXISTS fleet_processed_events (
        event_id BIGINT NOT NULL REFERENCES fleet_events(id) ON DELETE CASCADE,
        consumer VARCHAR(100) NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (event_id, consumer)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fleet_event_failures (
        event_id BIGINT NOT NULL REFERENCES fleet_events(id) ON DELETE CASCADE,
        consumer VARCHAR(100) NOT NULL,
        retry_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        failed_permanently BOOLEAN NOT NULL DEFAULT FALSE,
        last_attempt_at TIMESTAMPTZ,
        PRIMARY KEY (event_id, consumer)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fleet_event_failures_consumer_permanent ON fleet_event_failures (consumer, failed_permanently)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'fleet_events'
            ORDER BY ordinal_position
        """)).fetchall()
    print("fleet_events columns present:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
