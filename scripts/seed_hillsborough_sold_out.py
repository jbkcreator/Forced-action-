"""
Seed Hillsborough taken ZIPs so /landing/hillsborough resolves to sold_out.

Creates a handful of locked ZIPs across the 6 verticals.
"""
import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from src.core.database import Database
from src.core.models import ZipTerritory

db = Database()

ZIPS = ["33601", "33602", "33603", "33604", "33605", "33606", "33607", "33609", "33610", "33611"]
VERTICALS = ["roofing", "restoration", "public_adjusters", "wholesalers", "fix_flip", "attorneys"]

with db.session_scope() as s:
    inserted = 0
    skipped = 0
    for zc in ZIPS:
        for v in VERTICALS:
            existing = (
                s.query(ZipTerritory)
                .filter_by(zip_code=zc, vertical=v, county_id="hillsborough")
                .first()
            )
            if existing:
                # Ensure at least some are locked for sold_out resolution
                if existing.status == "available":
                    existing.status = "locked"
                    existing.locked_at = datetime.now(timezone.utc)
                    inserted += 1
                else:
                    skipped += 1
            else:
                s.add(ZipTerritory(
                    zip_code=zc,
                    vertical=v,
                    county_id="hillsborough",
                    status="locked",
                    locked_at=datetime.now(timezone.utc),
                ))
                inserted += 1

print(f"Inserted/locked: {inserted}, skipped: {skipped}")
print("Now visit: http://localhost:5173/landing/hillsborough")
