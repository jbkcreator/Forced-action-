"""Apply fa_5_2b: functional index for SEO compiler city scans.

CREATE INDEX CONCURRENTLY cannot run inside a transaction, so this uses an
autocommit connection (not get_db_context). Safe on the live table — no locks.
"""
from sqlalchemy import text
from src.core.database import db

_DDL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_properties_trim_city_county
ON properties (TRIM(city), county_id)
"""

if __name__ == "__main__":
    with db.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(_DDL))
    print("fa_5_2b applied: idx_properties_trim_city_county ready.")
