"""Apply fa051 — Predictive Churn schema changes.

Adds 5 Churn Risk columns to user_segments and creates the append-only
churn_predictions table. Idempotent: safe to run multiple times.

Usage:
    python scripts/apply_fa051_ddl.py
"""

import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    # ── user_segments: 5 new nullable columns ──────────────────────────────
    "ALTER TABLE user_segments ADD COLUMN IF NOT EXISTS churn_risk_score INT",
    "ALTER TABLE user_segments ADD COLUMN IF NOT EXISTS churn_risk_band VARCHAR(20)",
    "ALTER TABLE user_segments ADD COLUMN IF NOT EXISTS predicted_inactivity_at TIMESTAMPTZ",
    "ALTER TABLE user_segments ADD COLUMN IF NOT EXISTS churn_risk_reason VARCHAR(255)",
    "ALTER TABLE user_segments ADD COLUMN IF NOT EXISTS churn_risk_updated_at TIMESTAMPTZ",
    # CHECK constraint on user_segments.churn_risk_band (idempotent)
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_churn_risk_band'
        ) THEN
            ALTER TABLE user_segments ADD CONSTRAINT check_churn_risk_band
                CHECK (churn_risk_band IS NULL OR
                       churn_risk_band IN ('low', 'medium', 'high', 'very_high'));
        END IF;
    END $$
    """,
    # ── churn_predictions: append-only history table ───────────────────────
    """
    CREATE TABLE IF NOT EXISTS churn_predictions (
        id              BIGSERIAL PRIMARY KEY,
        subscriber_id   INT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
        predicted_at    TIMESTAMPTZ NOT NULL,
        churn_risk_score INT NOT NULL,
        churn_risk_band VARCHAR(20),
        predicted_inactivity_at TIMESTAMPTZ,
        features        JSONB,
        in_holdout      BOOLEAN NOT NULL DEFAULT FALSE,
        save_offer_sent_at TIMESTAMPTZ,
        realized_inactive_at TIMESTAMPTZ,
        was_correct     BOOLEAN,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    # Indexes
    "CREATE INDEX IF NOT EXISTS ix_churn_predictions_subscriber_id ON churn_predictions(subscriber_id)",
    "CREATE INDEX IF NOT EXISTS ix_churn_predictions_sub_predicted ON churn_predictions(subscriber_id, predicted_at DESC)",
    # CHECK constraint on churn_predictions.churn_risk_band (idempotent)
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_churn_prediction_band'
        ) THEN
            ALTER TABLE churn_predictions ADD CONSTRAINT check_churn_prediction_band
                CHECK (churn_risk_band IS NULL OR
                       churn_risk_band IN ('low', 'medium', 'high', 'very_high'));
        END IF;
    END $$
    """,
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt.strip()))
            logger.info("OK: %s", stmt.strip().splitlines()[0][:80])
    logger.info("fa051 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)
