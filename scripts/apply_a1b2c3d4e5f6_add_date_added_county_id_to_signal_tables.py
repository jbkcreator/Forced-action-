"""Auto-converted from alembic migration `a1b2c3d4e5f6_add_date_added_county_id_to_signal_tables` (revision a1b2c3d4e5f6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_a1b2c3d4e5f6_add_date_added_county_id_to_signal_tables.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE code_violations ADD COLUMN date_added DATE;

CREATE INDEX idx_code_violations_date_added ON code_violations (date_added);

ALTER TABLE code_violations ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_code_violations_county_id ON code_violations (county_id);

UPDATE code_violations SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE code_violations SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE legal_and_liens ADD COLUMN date_added DATE;

CREATE INDEX idx_legal_and_liens_date_added ON legal_and_liens (date_added);

ALTER TABLE legal_and_liens ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_legal_and_liens_county_id ON legal_and_liens (county_id);

UPDATE legal_and_liens SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE legal_and_liens SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE deeds ADD COLUMN date_added DATE;

CREATE INDEX idx_deeds_date_added ON deeds (date_added);

ALTER TABLE deeds ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_deeds_county_id ON deeds (county_id);

UPDATE deeds SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE deeds SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE legal_proceedings ADD COLUMN date_added DATE;

CREATE INDEX idx_legal_proceedings_date_added ON legal_proceedings (date_added);

ALTER TABLE legal_proceedings ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_legal_proceedings_county_id ON legal_proceedings (county_id);

UPDATE legal_proceedings SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE legal_proceedings SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE tax_delinquencies ADD COLUMN date_added DATE;

CREATE INDEX idx_tax_delinquencies_date_added ON tax_delinquencies (date_added);

ALTER TABLE tax_delinquencies ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_tax_delinquencies_county_id ON tax_delinquencies (county_id);

UPDATE tax_delinquencies SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE tax_delinquencies SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE foreclosures ADD COLUMN date_added DATE;

CREATE INDEX idx_foreclosures_date_added ON foreclosures (date_added);

ALTER TABLE foreclosures ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_foreclosures_county_id ON foreclosures (county_id);

UPDATE foreclosures SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE foreclosures SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE building_permits ADD COLUMN date_added DATE;

CREATE INDEX idx_building_permits_date_added ON building_permits (date_added);

ALTER TABLE building_permits ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_building_permits_county_id ON building_permits (county_id);

UPDATE building_permits SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE building_permits SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE incidents ADD COLUMN date_added DATE;

CREATE INDEX idx_incidents_date_added ON incidents (date_added);

ALTER TABLE incidents ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_incidents_county_id ON incidents (county_id);

UPDATE incidents SET date_added = CURRENT_DATE WHERE date_added IS NULL;

UPDATE incidents SET county_id = 'hillsborough' WHERE county_id IS NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied a1b2c3d4e5f6_add_date_added_county_id_to_signal_tables")


if __name__ == "__main__":
    main()
