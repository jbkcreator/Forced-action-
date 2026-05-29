"""List ZIPs available for landing page testing."""
import sys
sys.path.insert(0, ".")
from sqlalchemy import text
from src.core.database import get_db_context

with get_db_context() as db:
    print("\n=== ZIPs with properties in Hillsborough (use these to test zip-check) ===")
    rows = db.execute(text("""
        SELECT p.zip, COUNT(DISTINCT p.id) AS prop_count,
               COALESCE(MAX(zt.status), 'no_territory') AS territory_status
        FROM properties p
        LEFT JOIN zip_territories zt
            ON zt.zip_code = p.zip AND zt.county_id = 'hillsborough'
        WHERE p.county_id='hillsborough' AND p.zip IS NOT NULL
        GROUP BY p.zip
        ORDER BY p.zip
        LIMIT 20
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]} — {r[1]} properties — territory: {r[2]}")

    print("\n=== Locked ZIPs (use these to test waitlist form) ===")
    rows = db.execute(text("""
        SELECT zip_code, vertical FROM zip_territories
        WHERE county_id='hillsborough' AND status='locked'
        ORDER BY zip_code LIMIT 20
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]} ({r[1]})")
