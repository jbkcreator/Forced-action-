"""
PropertyRadar excluded-records table.

Creates:
  property_radar_excluded_records — bought PropertyRadar records that the
                                    normalizer dropped (20+ year loans,
                                    unmapped county, missing APN, ...). Every
                                    export is billed, so the raw payload is
                                    kept with the reason instead of discarded.

Run after apply_property_radar_pull_runs.py (FK to property_radar_pull_runs).
Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_property_radar_excluded_records.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_excluded_records (
        id                  BIGSERIAL    PRIMARY KEY,
        radar_id            VARCHAR(50)  NOT NULL,
        state               VARCHAR(2)   NOT NULL,
        campaign            VARCHAR(80)  NOT NULL,
        reason              VARCHAR(40)  NOT NULL,
        loan_term_years     TEXT,
        raw                 JSONB        NOT NULL,
        pull_run_id         BIGINT       REFERENCES property_radar_pull_runs (id) ON DELETE SET NULL,
        created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
        CONSTRAINT uq_pr_excluded_state_campaign_radar UNIQUE (state, campaign, radar_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_pr_excluded_reason
        ON property_radar_excluded_records (reason)
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_property_radar_excluded_records: done")


if __name__ == "__main__":
    main()
