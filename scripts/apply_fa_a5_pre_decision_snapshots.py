"""Idempotent companion migration for fa_a5_pre_decision_snapshots.

Run directly if Alembic multi-head tracking is not available:
    PYTHONPATH=. python scripts/apply_fa_a5_pre_decision_snapshots.py
"""
import logging
from src.core.database import get_db_context
from sqlalchemy import text as sa_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS pre_decision_snapshots (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id         INTEGER REFERENCES properties(id)    ON DELETE SET NULL,
    prospect_id         UUID    REFERENCES prospects(prospect_id) ON DELETE SET NULL,
    deal_outcome_id     INTEGER REFERENCES deal_outcomes(id) ON DELETE SET NULL,
    snapshot_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    selected_vertical   VARCHAR(50),
    lead_tier           VARCHAR(30),
    final_cds_score     NUMERIC(5,2),
    distress_types      JSONB,
    all_vertical_scores JSONB,
    runner_up_verticals JSONB,
    pricing_cohort_id   INTEGER,
    pricing_snapshot    JSONB,
    lifecycle_graph          VARCHAR(100),
    pitch_variant       VARCHAR(100),
    raw_context         JSONB,
    resolved_at         TIMESTAMPTZ,
    outcome_status      VARCHAR(30),
    broker_id           INTEGER,
    alternative_brokers JSONB,
    counterfactual_run    BOOLEAN NOT NULL DEFAULT FALSE,
    counterfactual_run_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_pds_deal_outcome
    ON pre_decision_snapshots(deal_outcome_id)
    WHERE deal_outcome_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pds_property_id       ON pre_decision_snapshots(property_id);
CREATE INDEX IF NOT EXISTS idx_pds_snapshot_ts       ON pre_decision_snapshots(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_pds_selected_vertical ON pre_decision_snapshots(selected_vertical);
CREATE INDEX IF NOT EXISTS idx_pds_outcome_status    ON pre_decision_snapshots(outcome_status);
CREATE INDEX IF NOT EXISTS idx_pds_pending_cf
    ON pre_decision_snapshots(id)
    WHERE counterfactual_run = FALSE AND outcome_status IS NOT NULL;
"""

if __name__ == "__main__":
    with get_db_context() as db:
        db.execute(sa_text(DDL))
    logger.info("pre_decision_snapshots table and indexes created (idempotent)")
