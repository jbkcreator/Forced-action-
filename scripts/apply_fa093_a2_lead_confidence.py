"""Apply fa093 — A2 Lead Confidence gating.

Adds distress_scores.lead_confidence + is_guess_lead and the partial sellable
index on the shared Postgres. DDL is idempotent (IF NOT EXISTS), safe to re-run.
Run directly because the alembic CLI is unusable on this repo's multi-head tree:

    python -m scripts.apply_fa093_a2_lead_confidence
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("apply_fa093")

_DDL = [
    "ALTER TABLE distress_scores ADD COLUMN IF NOT EXISTS lead_confidence NUMERIC(4, 3)",
    "ALTER TABLE distress_scores ADD COLUMN IF NOT EXISTS is_guess_lead BOOLEAN NOT NULL DEFAULT FALSE",
    "CREATE INDEX IF NOT EXISTS idx_score_sellable "
    "ON distress_scores (final_cds_score) WHERE is_guess_lead = false",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in _DDL:
            db.execute(text(stmt))
        db.commit()
    logger.info("fa093 applied: distress_scores.lead_confidence, is_guess_lead, idx_score_sellable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
