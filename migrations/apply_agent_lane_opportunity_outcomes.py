"""
LEARN-v2.2 T-LEARN-03 — agent_lane_opportunity_outcomes table.

Creates:
  - agent_lane_opportunity_outcomes  (one terminal outcome per opportunity thread)

The missing "this opportunity is over, and here is why" home for a bare
opportunity_thread_id string. One row per thread (UNIQUE). outcome='won'
needs no reason; outcome='lost' carries one of the spec's eight loss codes
(DB CHECK enforces the closed set).

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).

Usage:
    PYTHONPATH=. python migrations/apply_agent_lane_opportunity_outcomes.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS agent_lane_opportunity_outcomes (
        id BIGSERIAL PRIMARY KEY,
        opportunity_thread_id VARCHAR(20) NOT NULL,
        outcome VARCHAR(10) NOT NULL,
        reason_code VARCHAR(20),
        coded_by VARCHAR(60) NOT NULL,
        source_ref VARCHAR(120),
        coded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_agent_lane_opportunity_outcome UNIQUE (opportunity_thread_id),
        CONSTRAINT ck_alo_outcome CHECK (outcome IN ('won','lost')),
        CONSTRAINT ck_alo_reason_code CHECK (
            (outcome = 'won' AND reason_code IS NULL) OR
            (outcome = 'lost' AND reason_code IN
                ('timing','price','trust','fit','no_urgency','wrong_contact','competitor','no_response'))
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_alo_outcome_reason ON agent_lane_opportunity_outcomes (outcome, reason_code)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'agent_lane_opportunity_outcomes'
            ORDER BY ordinal_position
        """)).fetchall()
    print("agent_lane_opportunity_outcomes columns:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
