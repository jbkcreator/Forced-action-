"""
LEARN-v2.2 L2 — experiment attribution table + supporting index.

Creates:
  - experiment_attributions     (attribution rows per fleet event / arm)
  - idx_outcome_on_assignments  (speeds the unattributed-assignment query)

Idempotent (CREATE TABLE IF NOT EXISTS / index IF NOT EXISTS).

Usage:
    PYTHONPATH=. python migrations/apply_experiment_attribution.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS experiment_attributions (
        id BIGSERIAL PRIMARY KEY,
        fleet_event_id BIGINT NOT NULL REFERENCES fleet_events(id),
        assignment_id INTEGER NOT NULL REFERENCES agent_lane_experiment_assignments(id),
        test_id INTEGER NOT NULL REFERENCES agent_lane_experiments(id),
        opportunity_thread_id VARCHAR(20) NOT NULL,
        variant VARCHAR(10) NOT NULL,
        event_type VARCHAR(40) NOT NULL,
        attribution_method VARCHAR(20) NOT NULL,
        window_days SMALLINT NOT NULL,
        attributed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_experiment_attribution UNIQUE (fleet_event_id, assignment_id),
        CONSTRAINT ck_experiment_attribution_method CHECK (
            attribution_method IN ('draft_match', 'last_touch')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_experiment_attribution_test ON experiment_attributions (test_id, event_type, attributed_at)",
    "CREATE INDEX IF NOT EXISTS idx_experiment_attribution_thread ON experiment_attributions (opportunity_thread_id)",
    # Speeds the sweep's "find assignments without an attribution yet" filter.
    "CREATE INDEX IF NOT EXISTS idx_alea_outcome ON agent_lane_experiment_assignments (outcome)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in STATEMENTS:
            db.execute(text(stmt))
        db.commit()
        cols = db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'experiment_attributions'
            ORDER BY ordinal_position
        """)).fetchall()
    print("experiment_attributions columns:", [c.column_name for c in cols])
    return 0


if __name__ == "__main__":
    sys.exit(main())
