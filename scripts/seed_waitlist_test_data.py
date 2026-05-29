"""
Seed test data for County Waitlist Landing Pages.

Scenarios:
  - pinellas  → coming_soon  (queued expansion candidate, no taken ZIPs)
  - hillsborough → sold_out (launched, taken ZIPs seeded via existing ZipTerritory)

Run: python scripts/seed_waitlist_test_data.py
"""
from src.core.database import Database
from src.core.models import County, ExpansionCandidate

db = Database()

# ── Ensure pinellas county row exists ────────────────────────────────────────
with db.session_scope() as s:
    if not s.query(County).filter_by(county_id="pinellas").first():
        s.add(County(
            county_id="pinellas",
            display_name="Pinellas County",
            fips="12103",
            nws_zone="FLZ050",
            parcel_id_format="strap",
            bankruptcy_division="8",
            city_filer_keywords=["PINELLAS COUNTY", "CITY OF ST. PETERSBURG"],
            code_lien_type_map={},
            is_active=True,
        ))
        print("Inserted County: pinellas")
    else:
        print("County pinellas already exists — skipped")

# ── Pinellas: queued expansion candidate → coming_soon landing ───────────────
with db.session_scope() as s:
    existing = s.query(ExpansionCandidate).filter_by(county_id="pinellas").first()
    if existing:
        if existing.status not in ("queued", "approved", "launching"):
            existing.status = "queued"
            print(f"Updated pinellas ExpansionCandidate status → queued")
        else:
            print(f"ExpansionCandidate pinellas already {existing.status} — skipped")
    else:
        s.add(ExpansionCandidate(
            county_id="pinellas",
            status="queued",
            priority=10,
        ))
        print("Inserted ExpansionCandidate: pinellas (queued)")

# ── Hillsborough: verify county exists ───────────────────────────────────────
with db.session_scope() as s:
    if not s.query(County).filter_by(county_id="hillsborough").first():
        print("WARNING: hillsborough county not found — run scripts/seed_dev_db.py first")
    else:
        taken = s.execute(
            __import__("sqlalchemy").text(
                "SELECT COUNT(*) FROM zip_territories "
                "WHERE county_id='hillsborough' AND status='taken'"
            )
        ).scalar()
        print(f"Hillsborough taken ZIPs: {taken} "
              f"({'sold_out landing will work' if taken > 0 else 'WARNING: 0 taken ZIPs — sold_out landing returns 404'})")

print("\nDone. Now visit:")
print("  http://localhost:5173/landing/pinellas      → should show coming_soon")
print("  http://localhost:5173/landing/hillsborough  → should show sold_out (if taken ZIPs exist)")
