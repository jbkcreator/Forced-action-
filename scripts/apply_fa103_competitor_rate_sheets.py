"""Apply fa103 - competitor_rate_sheets + forced_action_lender_terms (Task 4.8).

Idempotent — IF NOT EXISTS guards on all DDL; seed uses ON CONFLICT DO NOTHING.

Usage:
    PYTHONPATH=. python scripts/apply_fa103_competitor_rate_sheets.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS competitor_rate_sheets (
        id             BIGSERIAL PRIMARY KEY,
        lender_name    VARCHAR(128)  NOT NULL,
        product        VARCHAR(24)   NOT NULL CHECK (product IN ('dscr','private')),
        region         VARCHAR(64),
        rate_low       NUMERIC(6,3),
        rate_high      NUMERIC(6,3),
        max_ltv        NUMERIC(5,2),
        min_fico       INTEGER,
        min_dscr       NUMERIC(4,2),
        points         NUMERIC(4,2),
        prepay         VARCHAR(64),
        hq_location    VARCHAR(96),
        source_url     TEXT          NOT NULL,
        source_adapter VARCHAR(48)   NOT NULL,
        confidence     VARCHAR(16)   NOT NULL DEFAULT 'high'
                       CHECK (confidence IN ('high','low')),
        raw_text       TEXT,
        captured_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_competitor_rate_sheet
            UNIQUE (lender_name, product, region, captured_at)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_competitor_rate_sheets_product_region
        ON competitor_rate_sheets (product, region);
    """,
    """
    CREATE TABLE IF NOT EXISTS forced_action_lender_terms (
        id          BIGSERIAL PRIMARY KEY,
        product     VARCHAR(24)  NOT NULL UNIQUE
                    CHECK (product IN ('dscr','private')),
        rate        NUMERIC(6,3) NOT NULL,
        max_ltv     NUMERIC(5,2) NOT NULL,
        points      NUMERIC(4,2),
        prepay      VARCHAR(64),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );
    """,
]

# FL market-average placeholders — replace with real Forced Action terms.
SEED = """
    INSERT INTO forced_action_lender_terms (product, rate, max_ltv, points, prepay)
    VALUES
        ('dscr',    6.875, 80.00, 1.00, '3yr step-down'),
        ('private', 12.000, 70.00, 2.00, 'none')
    ON CONFLICT (product) DO NOTHING;
"""


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))
        logger.info("Seeding forced_action_lender_terms (placeholders)")
        conn.execute(text(SEED.strip()))

    logger.info("fa103 complete — competitor rate-sheet tables applied + seeded.")


if __name__ == "__main__":
    main()
