"""Idempotent DDL script for A3: scoring_weight_overrides.

Run with:
    PYTHONPATH=. python scripts/apply_fa_a3_scoring_weight_overrides.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.database import get_db_context
from sqlalchemy import text as sa_text


DDL = """
CREATE TABLE IF NOT EXISTS scoring_weight_overrides (
    id                SERIAL PRIMARY KEY,
    vertical          VARCHAR(50)  NOT NULL,
    signal_type       VARCHAR(50)  NOT NULL,
    delta             NUMERIC(6,2) NOT NULL DEFAULT 0,
    source            VARCHAR(30)  NOT NULL DEFAULT 'seed',
    reason            TEXT,
    enabled           BOOLEAN      NOT NULL DEFAULT TRUE,
    loss_sample_count INT          NOT NULL DEFAULT 0,
    win_sample_count  INT          NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_swo_vertical_signal UNIQUE (vertical, signal_type)
);
CREATE INDEX IF NOT EXISTS idx_swo_enabled ON scoring_weight_overrides (enabled);
CREATE INDEX IF NOT EXISTS idx_swo_updated_at ON scoring_weight_overrides (updated_at DESC);
"""


def main() -> None:
    with get_db_context() as db:
        for statement in DDL.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                db.execute(sa_text(stmt))
    print("apply_fa_a3_scoring_weight_overrides: table ready")


if __name__ == "__main__":
    main()
