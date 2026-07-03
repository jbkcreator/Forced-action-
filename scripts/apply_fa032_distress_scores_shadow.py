"""Auto-converted from alembic migration `fa032_distress_scores_shadow` (revision fa032_distress_scores_shadow).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa032_distress_scores_shadow.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE distress_scores_shadow (
    id SERIAL NOT NULL, 
    property_id INTEGER NOT NULL, 
    vertical_scores JSONB, 
    score_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    final_cds_score NUMERIC(5, 2), 
    lead_tier VARCHAR(50), 
    distress_types JSONB, 
    urgency_level VARCHAR(20), 
    multiplier NUMERIC(4, 2), 
    factor_scores JSONB, 
    qualified BOOLEAN DEFAULT false, 
    county_id VARCHAR(50) DEFAULT 'hillsborough', 
    scoring_run_id INTEGER, 
    PRIMARY KEY (id), 
    CONSTRAINT check_urgency_level_shadow CHECK (urgency_level IN ('Immediate', 'High', 'Medium', 'Low')), 
    CONSTRAINT check_lead_tier_shadow CHECK (lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze')), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX ix_distress_scores_shadow_county_id ON distress_scores_shadow (county_id);

CREATE INDEX ix_distress_scores_shadow_property_id ON distress_scores_shadow (property_id);

CREATE INDEX idx_shadow_score_date ON distress_scores_shadow (score_date);

CREATE INDEX idx_shadow_score_final_cds ON distress_scores_shadow (final_cds_score);

CREATE INDEX idx_shadow_score_lead_tier ON distress_scores_shadow (lead_tier);

CREATE INDEX idx_shadow_score_qualified ON distress_scores_shadow (qualified);

CREATE INDEX idx_shadow_score_county_id ON distress_scores_shadow (county_id);

CREATE INDEX idx_shadow_score_distress_types ON distress_scores_shadow USING gin (distress_types);

CREATE INDEX idx_shadow_score_scoring_run_id ON distress_scores_shadow (scoring_run_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa032_distress_scores_shadow")


if __name__ == "__main__":
    main()
