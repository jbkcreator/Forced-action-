"""Apply fa_a1 — loss_autopsies table (Phase 3 A1: Loss Autopsy Engine).

Idempotent: safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa_a1_loss_autopsies.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS loss_autopsies (
        id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        property_id              INT  REFERENCES properties(id)         ON DELETE SET NULL,
        prospect_id              UUID REFERENCES prospects(prospect_id)  ON DELETE SET NULL,
        deal_outcome_id          INT  REFERENCES deal_outcomes(id)       ON DELETE SET NULL,
        trigger_reason           VARCHAR(50) NOT NULL,
        primary_rejection_reason TEXT,
        competitor_rate_delta    NUMERIC(8,4),
        underwriting_blocker     TEXT,
        lifecycle_behavior_adjustment TEXT,
        raw_context              JSONB NOT NULL DEFAULT '{}'::jsonb,
        model_response           JSONB NOT NULL DEFAULT '{}'::jsonb,
        claude_cost_usd          NUMERIC(10,6),
        created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_loss_autopsy_trigger
            CHECK (trigger_reason IN ('CLOSED_LOST','DECLINED','GHOSTED_SLA'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_loss_autopsies_property_id    ON loss_autopsies (property_id)",
    "CREATE INDEX IF NOT EXISTS idx_loss_autopsies_deal_outcome_id ON loss_autopsies (deal_outcome_id)",
    "CREATE INDEX IF NOT EXISTS idx_loss_autopsies_trigger_reason ON loss_autopsies (trigger_reason)",
    "CREATE INDEX IF NOT EXISTS idx_loss_autopsies_created_at     ON loss_autopsies (created_at DESC)",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_loss_autopsies_deal_outcome_once
        ON loss_autopsies(deal_outcome_id)
        WHERE deal_outcome_id IS NOT NULL
    """,
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip()[:80])
    logger.info("fa_a1_loss_autopsies DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
