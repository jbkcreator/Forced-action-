"""
10 random Gold+ leads with every underlying signal record for manual verification.

Where to look:
  hcpafl.org                          -> Parcel ID (Property Appraiser)
  hillsclerk.com                      -> instrument_number, case_number (Clerk of Courts)
  Accela (hillsborough.accela.net)    -> record_number (Code Violations)
  hctc.net                            -> parcel ID (Tax Collector / Delinquencies)
  permits.hillsboroughcounty.org      -> permit_number (Building Permits)
"""
import sys
sys.path.insert(0, ".")
from src.core.database import get_db_context
from sqlalchemy import text

SAMPLE_SQL = """
SELECT p.id, p.parcel_id, p.address, p.city, p.state, p.zip,
       ds.lead_tier, ds.final_cds_score, ds.distress_types,
       ds.vertical_scores, ds.score_date,
       o.owner_name, o.owner_type, o.absentee_status, o.mailing_address
FROM (
    SELECT DISTINCT ON (property_id)
        property_id, lead_tier, final_cds_score, distress_types,
        vertical_scores, score_date
    FROM distress_scores
    WHERE county_id = 'hillsborough'
      AND lead_tier IN ('Ultra Platinum','Platinum','Gold')
    ORDER BY property_id, score_date DESC
) ds
JOIN properties p ON p.id = ds.property_id
LEFT JOIN owners o ON o.property_id = p.id
ORDER BY RANDOM()
LIMIT 10
"""

SIGNAL_QUERIES = [
    (
        "JUDGMENTS/LIENS  [hillsclerk.com -> instrument_number]",
        """SELECT record_type, instrument_number, filing_date, amount,
                  document_type, creditor, debtor
           FROM legal_and_liens WHERE property_id = :pid ORDER BY filing_date DESC"""
    ),
    (
        "CODE VIOLATIONS  [hillsborough.accela.net -> record_number]",
        """SELECT record_number, violation_type, opened_date, status,
                  severity_tier, fine_amount
           FROM code_violations WHERE property_id = :pid ORDER BY opened_date DESC"""
    ),
    (
        "FORECLOSURES  [hillsclerk.com -> case_number]",
        """SELECT case_number, plaintiff, filing_date, lis_pendens_date,
                  judgment_amount, auction_date, case_status
           FROM foreclosures WHERE property_id = :pid ORDER BY filing_date DESC"""
    ),
    (
        "DEEDS  [hillsclerk.com -> instrument_number]",
        """SELECT instrument_number, deed_type, grantor, grantee,
                  record_date, sale_price
           FROM deeds WHERE property_id = :pid ORDER BY record_date DESC LIMIT 3"""
    ),
    (
        "TAX DELINQUENCIES  [hctc.net -> parcel ID]",
        """SELECT tax_year, years_delinquent, total_amount_due,
                  certificate_data, deed_app_date
           FROM tax_delinquencies WHERE property_id = :pid ORDER BY tax_year DESC"""
    ),
    (
        "BUILDING PERMITS  [permits.hillsboroughcounty.org -> permit_number]",
        """SELECT permit_number, permit_type, issue_date, expire_date,
                  status, is_enforcement_permit
           FROM building_permits WHERE property_id = :pid ORDER BY issue_date DESC LIMIT 5"""
    ),
    (
        "LEGAL PROCEEDINGS  [hillsclerk.com -> case_number]",
        """SELECT record_type, case_number, filing_date, case_status,
                  amount, associated_party, secondary_party
           FROM legal_proceedings WHERE property_id = :pid ORDER BY filing_date DESC"""
    ),
    (
        "INCIDENTS",
        """SELECT incident_type, incident_date, arrest_count_12m, problem_prop_flag
           FROM incidents WHERE property_id = :pid ORDER BY incident_date DESC"""
    ),
]

SEP  = "=" * 72
THIN = "-" * 72

with get_db_context() as session:
    leads = session.execute(text(SAMPLE_SQL)).fetchall()

    for i, lead in enumerate(leads, 1):
        pid      = lead[0]
        parcel   = lead[1]
        addr     = f"{lead[2]}, {lead[3]}, {lead[4]} {lead[5]}"
        tier     = lead[6]
        score    = lead[7]
        sigs     = list(lead[8]) if lead[8] else []
        vs       = lead[9] or {}
        top_v    = max(vs, key=lambda k: vs.get(k, 0)) if vs else "none"
        sdate    = str(lead[10])[:10]
        owner    = lead[11] or "none"
        otype    = lead[12] or "none"
        absent   = lead[13] or "none"
        mail     = lead[14] or "none"

        print(f"\n{SEP}")
        print(f"  LEAD {i}/10")
        print(SEP)
        print(f"  Parcel ID    : {parcel}  -> hcpafl.org")
        print(f"  Address      : {addr}")
        print(f"  Tier         : {tier}  |  CDS Score: {score}")
        print(f"  Top Vertical : {top_v} ({vs.get(top_v, 0)})")
        print(f"  Signals      : {sigs}")
        print(f"  Score Date   : {sdate}")
        print(f"  Owner        : {owner}")
        print(f"  Owner Type   : {otype}  |  Absentee: {absent}")
        print(f"  Mail Addr    : {mail}")

        has_any = False
        for label, sql in SIGNAL_QUERIES:
            rows = session.execute(text(sql), {"pid": pid}).fetchall()
            if not rows:
                continue
            has_any = True
            print(f"\n  {THIN}")
            print(f"  {label}")
            print(f"  {THIN}")
            for r in rows:
                parts = []
                for c in r:
                    if c is None:
                        parts.append("--")
                    else:
                        parts.append(str(c)[:60])
                print(f"    {' | '.join(parts)}")

        if not has_any:
            print("\n  (no signal records found in DB)")
        print()
