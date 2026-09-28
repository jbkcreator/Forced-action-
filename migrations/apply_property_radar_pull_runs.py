"""
PropertyRadar pull-run checkpoint table (Developer 1, PropertyRadar ingestion adapter).

Creates:
  property_radar_pull_runs — one row per completed pull run; stores the
                              radar_ids seen so the daily pull can skip
                              records already fetched (idempotency key is
                              radar_id, owned here not in a staging table).

Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_property_radar_pull_runs.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_pull_runs (
        id                  BIGSERIAL PRIMARY KEY,
        run_type            VARCHAR(20)  NOT NULL CHECK (run_type IN ('backlog', 'daily')),
        state               CHAR(2)      NOT NULL,
        campaign            VARCHAR(80)  NOT NULL,
        started_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
        finished_at         TIMESTAMPTZ,
        records_fetched     INTEGER      NOT NULL DEFAULT 0,
        exports_consumed    INTEGER      NOT NULL DEFAULT 0,
        status              VARCHAR(20)  NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'done', 'failed'))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_pr_pull_runs_state_campaign
        ON property_radar_pull_runs (state, campaign)
    """,
    # Seen radar_ids — one row per (state, campaign, radar_id). Owned by Dev 1
    # so the daily pull can do a single NOT IN / EXCEPT query rather than
    # relying on Dev 2's staging table (open question #2 deferred to Monday
    # cross-developer contract meeting — this table can be dropped in favour of
    # Dev 2's staging radar_ids if the teams agree at that meeting; the daily
    # pull logic in property_radar_maturity_pull.py has a clear boundary here).
    """
    CREATE TABLE IF NOT EXISTS property_radar_seen_ids (
        state       CHAR(2)     NOT NULL,
        campaign    VARCHAR(80) NOT NULL,
        radar_id    VARCHAR(40) NOT NULL,
        first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (state, campaign, radar_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_pr_seen_ids_state_campaign
        ON property_radar_seen_ids (state, campaign)
    """,
]


def main() -> None:
    with get_db_context() as session:
        for ddl in _DDL:
            session.execute(text(ddl))
        session.commit()
    print("apply_property_radar_pull_runs: done")


if __name__ == "__main__":
    main()
