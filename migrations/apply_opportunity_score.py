"""Apply REVINT-I1 — opportunity_scores + opportunity_score_history tables.

Idempotent DDL: safe to re-run. No Alembic — scripts-only per ADR 0024.

    PYTHONPATH=. python migrations/apply_opportunity_score.py
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("apply_opportunity_score")

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS opportunity_scores (
        id                                BIGSERIAL PRIMARY KEY,
        opportunity_thread_id             TEXT NOT NULL,
        buyer_entity_id                   INTEGER NOT NULL,
        segment                           TEXT NOT NULL,
        revenue_type                      TEXT NOT NULL,
        billing_interval                  TEXT,
        expected_revenue_cents            INTEGER NOT NULL,
        expected_mrr_cents                INTEGER,
        expected_retained_gross_profit_cents INTEGER NOT NULL,
        p_reply                           NUMERIC(6,4) NOT NULL,
        p_close                           NUMERIC(6,4) NOT NULL,
        time_to_cash_days                 INTEGER NOT NULL,
        josh_minutes_required             NUMERIC(8,2) NOT NULL,
        nbra_score                        NUMERIC(12,4),
        source_action_type                TEXT,
        is_automated                      BOOLEAN NOT NULL DEFAULT FALSE,
        created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_opp_scores_segment CHECK (
            segment IN ('whale','auction_winner','lapsed_subscriber','default')
        ),
        CONSTRAINT ck_opp_scores_billing_interval CHECK (
            billing_interval IN ('monthly','annual') OR billing_interval IS NULL
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_opp_scores_thread_id    ON opportunity_scores(opportunity_thread_id)",
    "CREATE INDEX IF NOT EXISTS ix_opp_scores_buyer_entity ON opportunity_scores(buyer_entity_id)",
    "CREATE INDEX IF NOT EXISTS ix_opp_scores_segment_nbra ON opportunity_scores(segment, nbra_score)",
    """
    CREATE TABLE IF NOT EXISTS opportunity_score_history (
        id                    BIGSERIAL PRIMARY KEY,
        opportunity_score_id  BIGINT NOT NULL REFERENCES opportunity_scores(id) ON DELETE CASCADE,
        opportunity_thread_id TEXT NOT NULL,
        snapshot_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        p_reply               NUMERIC(6,4) NOT NULL,
        p_close               NUMERIC(6,4) NOT NULL,
        time_to_cash_days     INTEGER NOT NULL,
        nbra_score            NUMERIC(12,4) NOT NULL,
        reason                TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_opp_score_history_score_id  ON opportunity_score_history(opportunity_score_id)",
    "CREATE INDEX IF NOT EXISTS ix_opp_score_history_thread_id ON opportunity_score_history(opportunity_thread_id)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in _DDL:
            db.execute(text(stmt))
        db.commit()
    logger.info("REVINT-I1 applied: opportunity_scores, opportunity_score_history")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
