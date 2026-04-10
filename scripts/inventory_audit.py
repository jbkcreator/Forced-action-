"""Audit the full current Gold+ inventory for systemic issues."""
import sys
sys.path.insert(0, ".")
from src.core.database import get_db_context
from sqlalchemy import text

SQL = """
WITH current_gold AS (
    SELECT DISTINCT ON (ds.property_id)
        ds.property_id,
        ds.lead_tier,
        ds.final_cds_score,
        ds.distress_types,
        ds.vertical_scores
    FROM distress_scores ds
    WHERE ds.county_id = 'hillsborough'
      AND ds.lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold')
    ORDER BY ds.property_id, ds.score_date DESC
)
SELECT
    COUNT(*)                                                        AS total_gold_plus,

    -- Issue 1: sole signal is deed_transfers (likely just-sold, not distressed)
    COUNT(*) FILTER (
        WHERE cg.distress_types = '["deed_transfers"]'::jsonb
           OR cg.distress_types = '["deed_transfers"]'
    )                                                               AS sole_signal_deed,

    -- Issue 2: In-County Individual owners (likely owner-occupied)
    COUNT(*) FILTER (
        WHERE o.absentee_status = 'In-County'
          AND o.owner_type = 'Individual'
    )                                                               AS in_county_individual,

    -- Issue 3: zero contact data (skip trace never ran or failed)
    COUNT(*) FILTER (
        WHERE (o.phone_1 IS NULL AND o.phone_2 IS NULL AND o.phone_3 IS NULL)
          AND (o.email_1 IS NULL AND o.email_2 IS NULL)
    )                                                               AS no_contact,

    -- Issue 4: owner_type = Individual but name looks corporate
    COUNT(*) FILTER (
        WHERE o.owner_type = 'Individual'
          AND (
              o.owner_name ILIKE '%LLC%'
              OR o.owner_name ILIKE '%CORP%'
              OR o.owner_name ILIKE '%INC%'
              OR o.owner_name ILIKE '%AT&T%'
              OR o.owner_name ILIKE '%TELEPHONE%'
              OR o.owner_name ILIKE '%COMPANY%'
              OR o.owner_name ILIKE '%ASSOCIATION%'
              OR o.owner_name ILIKE '%HOLDINGS%'
          )
    )                                                               AS misclassified_corporate,

    -- Issue 5: sold in last 60 days (deed transfer — dead leads still in DB)
    COUNT(*) FILTER (
        WHERE EXISTS (
            SELECT 1 FROM deeds d
            WHERE d.property_id = cg.property_id
              AND d.record_date >= CURRENT_DATE - INTERVAL '60 days'
        )
    )                                                               AS sold_last_60d,

    -- Issue 6: sold in last 30 days specifically
    COUNT(*) FILTER (
        WHERE EXISTS (
            SELECT 1 FROM deeds d
            WHERE d.property_id = cg.property_id
              AND d.record_date >= CURRENT_DATE - INTERVAL '30 days'
        )
    )                                                               AS sold_last_30d,

    -- Issue 7: skip trace attempted but failed (owner exists, no contact)
    COUNT(*) FILTER (
        WHERE o.id IS NOT NULL
          AND o.skip_trace_success = false
    )                                                               AS skip_trace_failed,

    -- Issue 8: no owner record at all
    COUNT(*) FILTER (WHERE o.id IS NULL)                           AS no_owner_record

FROM current_gold cg
LEFT JOIN properties p ON p.id = cg.property_id
LEFT JOIN owners o ON o.property_id = cg.property_id
"""

with get_db_context() as session:
    row = session.execute(text(SQL)).fetchone()

total = row[0]
print(f"\n{'='*55}")
print(f"  GOLD+ INVENTORY AUDIT  (total: {total:,})")
print(f"{'='*55}")
issues = [
    ("Sole signal = deed_transfers (just-sold?)",    row[1]),
    ("In-County Individual (likely owner-occupied)", row[2]),
    ("Zero contact data (no phone + no email)",      row[3]),
    ("Misclassified corporate as Individual",        row[4]),
    ("Sold in last 60 days (dead lead in DB)",       row[5]),
    ("Sold in last 30 days",                         row[6]),
    ("Skip trace ran but returned nothing",          row[7]),
    ("No owner record at all",                       row[8]),
]
for label, count in issues:
    pct = count / total * 100 if total else 0
    print(f"  {label}")
    print(f"    {count:>6,}  ({pct:.1f}%)")
    print()
