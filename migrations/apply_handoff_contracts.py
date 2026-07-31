"""
QUALITY-v2.2 Q3 — formal handoff contracts schema.

Two tables:
  1. handoff_rejections — an audit trail for every "incomplete handoff"
     across all four boundaries (Hunter->Cora, Cora->Relay, Vera->Dev,
     Dev->Vera). Per decision D2, an auto-reject also posts to Slack
     (src.agents.contracts.base.notify_slack_rejection) -- this table is
     the durable record so a rejection is diagnosable after the fact, not
     just a Slack message that scrolls away.

  2. handoff_quality_ratings — the §1.4 1-5 handoff-quality rating
     ("Cora rates Hunter's enrichment 1-5; Vera rates findings packets").
     Read literally: Cora rates the Hunter->Cora boundary; "Vera rates
     findings packets" is read as Vera rating the packets she receives
     BACK from Dev (the Dev->Vera boundary) -- the only boundary where
     Vera is a receiver rather than the Vera->Dev sender. Generic
     rater_seat/ratee_seat columns (not hardcoded to those two pairs)
     so a future boundary can reuse this table without a new migration.

    PYTHONPATH=. python migrations/apply_handoff_contracts.py

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS handoff_rejections (
        id BIGSERIAL PRIMARY KEY,
        boundary VARCHAR(20) NOT NULL,
        reference_id VARCHAR(120),
        missing_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
        payload_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        slack_notified BOOLEAN NOT NULL DEFAULT FALSE,
        rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_handoff_rejections_boundary CHECK (boundary IN (
            'hunter_to_cora', 'cora_to_relay', 'vera_to_dev', 'dev_to_vera'
        ))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_handoff_rejections_boundary_time ON handoff_rejections (boundary, rejected_at)",
    "CREATE INDEX IF NOT EXISTS idx_handoff_rejections_reference ON handoff_rejections (reference_id)",
    """
    CREATE TABLE IF NOT EXISTS handoff_quality_ratings (
        id BIGSERIAL PRIMARY KEY,
        boundary VARCHAR(20) NOT NULL,
        rater_seat VARCHAR(20) NOT NULL,
        ratee_seat VARCHAR(20) NOT NULL,
        reference_id VARCHAR(120) NOT NULL,
        score SMALLINT NOT NULL,
        notes TEXT,
        rated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_handoff_quality_boundary CHECK (boundary IN (
            'hunter_to_cora', 'dev_to_vera'
        )),
        CONSTRAINT ck_handoff_quality_score CHECK (score BETWEEN 1 AND 5)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_handoff_quality_ratee_time ON handoff_quality_ratings (ratee_seat, rated_at)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'handoff_rejections'
            ORDER BY ordinal_position
        """)).fetchall()
    print("handoff_rejections columns:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
