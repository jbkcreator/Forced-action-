"""Pull 10 random current Gold+ leads with full detail for spot-check."""
import json, sys
sys.path.insert(0, ".")
from src.core.database import get_db_context
from sqlalchemy import text

SQL = """
SELECT
    p.id,
    p.parcel_id,
    p.address,
    p.city,
    p.state,
    p.zip,
    ds.lead_tier,
    ds.final_cds_score,
    ds.vertical_scores,
    ds.distress_types,
    ds.score_date,
    o.owner_name,
    o.mailing_address,
    o.owner_type,
    o.absentee_status,
    o.phone_1,
    o.email_1
FROM (
    SELECT DISTINCT ON (property_id)
        property_id, lead_tier, final_cds_score, vertical_scores, distress_types, score_date
    FROM distress_scores
    WHERE county_id = 'hillsborough'
      AND lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold')
    ORDER BY property_id, score_date DESC
) ds
JOIN properties p ON p.id = ds.property_id
LEFT JOIN owners o ON o.property_id = p.id
ORDER BY RANDOM()
LIMIT 10
"""

with get_db_context() as session:
    rows = session.execute(text(SQL)).fetchall()

for i, r in enumerate(rows, 1):
    print(f"=== LEAD {i} ===")
    print(f"Parcel ID   : {r[1]}")
    print(f"Address     : {r[2]}, {r[3]}, {r[4]} {r[5]}")
    print(f"Tier        : {r[6]}")
    print(f"CDS Score   : {r[7]}")
    vs = r[8]
    if vs:
        top_v = max(vs, key=lambda k: vs.get(k, 0))
        print(f"Top Vertical: {top_v} ({vs[top_v]})")
        print(f"All Verticals: {json.dumps(vs)}")
    dt = r[9]
    print(f"Signals     : {json.dumps(list(dt) if dt else [])}")
    print(f"Score Date  : {str(r[10])[:10]}")
    print(f"Owner       : {r[11]}")
    print(f"Mail Addr   : {r[12]}")
    print(f"Owner Type  : {r[13]}")
    print(f"Absentee    : {r[14]}")
    print(f"Phone       : {r[15]}")
    print(f"Email       : {r[16]}")
    print()
