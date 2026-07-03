"""Auto-converted from alembic migration `fa091_m6_truth_engine` (revision fa091_m6_truth_engine).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa091_m6_truth_engine.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE grade_thresholds (
    id SERIAL NOT NULL, 
    grade VARCHAR NOT NULL, 
    cds_min INTEGER, 
    cds_max INTEGER, 
    contactability_min NUMERIC(5, 4), 
    requires_mobile_consent BOOLEAN DEFAULT false NOT NULL, 
    notes TEXT, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_grade_thresholds_grade UNIQUE (grade), 
    CONSTRAINT ck_grade_thresholds_grade CHECK (grade IN ('Ultra','Platinum','Gold','Silver','Bronze','sub_grade'))
);

INSERT INTO grade_thresholds
            (grade, cds_min, cds_max, contactability_min, requires_mobile_consent, notes)
        VALUES
            ('Ultra',     85, NULL, 0.4000, true,  'spec §3.1a: CDS>=0.85, contactability>=40%%, validated mobile+consent'),
            ('Platinum',  70,   84, 0.2500, false, 'spec §3.1a: CDS 0.70-0.84, contactability>=25%%'),
            ('Gold',      50,   69, 0.1200, false, 'spec §3.1a: CDS 0.50-0.69, contactability>=12%% (binding floor)'),
            ('Silver',    30,   49, 0.0500, false, 'spec §3.1a: CDS 0.30-0.49, contactability>=5%%'),
            ('Bronze',    15,   29, NULL,   false, 'spec §3.1a: CDS 0.15-0.29, any contactability'),
            ('sub_grade', NULL, 14, NULL,   false, 'spec §3.1a: CDS<0.15, any — recycle/suppress');

CREATE TABLE verdicts (
    verdict_id UUID DEFAULT generate_uuidv7() NOT NULL, 
    prospect_id UUID NOT NULL, 
    grade VARCHAR NOT NULL, 
    contributing_factors JSONB DEFAULT '{}'::jsonb NOT NULL, 
    contactability_flag BOOLEAN DEFAULT false NOT NULL, 
    routed_channel VARCHAR NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (verdict_id), 
    CONSTRAINT ck_verdicts_grade CHECK (grade IN ('Ultra','Platinum','Gold','Silver','Bronze','sub_grade')), 
    CONSTRAINT ck_verdicts_routed_channel CHECK (routed_channel IN ('loan_lane','contractor_subscription','storm_retainer','data_pack_bulk','free_hand_delivered','recycle_suppress')), 
    FOREIGN KEY(prospect_id) REFERENCES prospects (prospect_id)
);

CREATE INDEX idx_verdicts_prospect_id ON verdicts (prospect_id);

CREATE INDEX idx_verdicts_created_at ON verdicts (created_at);

CREATE TABLE cohort_rates (
    cohort_key VARCHAR NOT NULL, 
    contact_attempts INTEGER DEFAULT 0 NOT NULL, 
    successful_contacts INTEGER DEFAULT 0 NOT NULL, 
    contactability_rate NUMERIC(5, 4), 
    sample_size INTEGER DEFAULT 0 NOT NULL, 
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (cohort_key)
);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa091_m6_truth_engine")


if __name__ == "__main__":
    main()
