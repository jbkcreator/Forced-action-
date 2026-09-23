"""
Migration: permit detail fields on building_permits.

Adds new detail-page columns; the 4 orphaned columns (contractor_name,
holder_name, job_value, completion_status) are included as no-ops if they
already exist. Safe to re-run — all ADD COLUMN IF NOT EXISTS.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from config.settings import get_settings


DDL = [
    # Orphaned columns — already exist in DB; ADD COLUMN IF NOT EXISTS is a no-op
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_name VARCHAR(200)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS holder_name VARCHAR(200)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS job_value VARCHAR(50)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS completion_status VARCHAR(100)",
    # New detail columns
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_license VARCHAR(50)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_license_type VARCHAR(100)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_phone VARCHAR(20)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS contractor_email VARCHAR(200)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS applicant_name VARCHAR(200)",
    "ALTER TABLE building_permits ADD COLUMN IF NOT EXISTS owner_name VARCHAR(200)",
    # Index contractor_name for WP-T2-8/T2-9 lookups
    """CREATE INDEX IF NOT EXISTS idx_permit_contractor_name
       ON building_permits (contractor_name)
       WHERE contractor_name IS NOT NULL""",
]


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    print("apply_permit_detail_fields: done")


if __name__ == "__main__":
    main()
