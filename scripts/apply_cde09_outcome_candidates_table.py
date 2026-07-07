"""Apply CDE-09 — outcome_candidates staging table.

Canonical shape the Cora Data Engine's outcome connectors (src/connectors/)
write into: a labeled event mined from an already-ingested, already-matched
public record (foreclosure auction result, tax-deed auction result, appraiser
sale, etc.), keyed back to its source row for traceability. Deliberately not
FK'd to deal_outcomes — a separate label layer promotes rows from here once
DealOutcome.subscriber_id is made nullable for pipeline-sourced outcomes
(a different task).

Idempotent — IF NOT EXISTS guards on all DDL.

Usage:
    PYTHONPATH=. python scripts/apply_cde09_outcome_candidates_table.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS outcome_candidates (
        id                 BIGSERIAL PRIMARY KEY,
        property_id        INTEGER      NOT NULL REFERENCES properties (id),
        county_id          VARCHAR(50)  NOT NULL,
        source_type        VARCHAR(50)  NOT NULL,
        source_table       VARCHAR(50)  NOT NULL,
        source_id          INTEGER      NOT NULL,
        event_type         VARCHAR(40)  NOT NULL
                           CHECK (event_type IN (
                               'auction_sold_third_party', 'auction_reverted_to_lender',
                               'auction_cancelled', 'tax_deed_sold', 'tax_deed_cancelled',
                               'tax_deed_redeemed', 'qualified_sale', 'unqualified_sale'
                           )),
        event_date         DATE         NOT NULL,
        amount             NUMERIC(14, 2),
        counterparty       VARCHAR(255),
        raw_status         VARCHAR(100),
        match_confidence   NUMERIC(4, 3),
        match_method       VARCHAR(30),
        consumed_at        TIMESTAMPTZ,
        created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at         TIMESTAMPTZ,
        CONSTRAINT uq_outcome_candidate UNIQUE (source_type, source_table, source_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_outcome_candidates_property
        ON outcome_candidates (property_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_outcome_candidates_source_type
        ON outcome_candidates (source_type);
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_outcome_candidates_unconsumed
        ON outcome_candidates (consumed_at) WHERE consumed_at IS NULL;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("cde09_outcome_candidates_table complete — outcome_candidates staging table applied.")


if __name__ == "__main__":
    main()
