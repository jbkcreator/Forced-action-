"""
PropertyRadar lead handoff decisions.

Creates property_radar_handoff_decisions: one row per handoff decision made
for a staged PropertyRadar record (handed off to FA Max, suppressed, or
skipped), with the reason. A partial unique index guarantees a record is
handed off at most once, so re-running the handoff can never create a
second FA Max person or opportunity for the same PropertyRadar record.

Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT
EXISTS).

Run:
  PYTHONPATH=. python migrations/apply_property_radar_handoff_decisions.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_handoff_decisions (
        id              BIGSERIAL    PRIMARY KEY,
        radar_id        VARCHAR(50)  NOT NULL,
        campaign        VARCHAR(100) NOT NULL,
        county_fips     VARCHAR(5)   NOT NULL,
        outcome         VARCHAR(20)  NOT NULL
                        CHECK (outcome IN ('handed_off', 'suppressed', 'skipped')),
        reason          VARCHAR(60)  NOT NULL,
        person_id       UUID         REFERENCES fa_max_persons(person_id) ON DELETE SET NULL,
        opportunity_id  UUID         REFERENCES fa_max_opportunities(opportunity_id) ON DELETE SET NULL,
        decided_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_pr_handoff_once
        ON property_radar_handoff_decisions (radar_id)
        WHERE outcome = 'handed_off';
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_pr_handoff_radar_decided
        ON property_radar_handoff_decisions (radar_id, decided_at);
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
            session.commit()
    print("apply_property_radar_handoff_decisions: done")


if __name__ == "__main__":
    main()
