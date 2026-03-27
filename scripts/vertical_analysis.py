"""
Vertical Analysis Script
Answers key business questions about lead verticals, contractor opportunities,
permit types, code violations, and distress score distributions.
"""

import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.database import get_db_context
from sqlalchemy import text


def section(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def run_query(session, sql, params=None):
    result = session.execute(text(sql), params or {})
    return result.fetchall(), result.keys()


def main():
    with get_db_context() as session:

        # ------------------------------------------------------------------ #
        # QUESTION 1: Gold+ leads by signal source
        # ------------------------------------------------------------------ #
        section("Q1: Gold+ Lead Counts by Signal Source")

        # Total Gold+ properties (latest score per property)
        sql_total = """
            WITH latest AS (
                SELECT DISTINCT ON (property_id) property_id, lead_tier
                FROM distress_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT lead_tier, COUNT(*) AS cnt
            FROM latest
            WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
            GROUP BY lead_tier
            ORDER BY cnt DESC
        """
        rows, keys = run_query(session, sql_total)
        print("\nGold+ properties by tier (latest score per property):")
        total_gold_plus = 0
        for r in rows:
            print(f"  {r[0]:<20} {r[1]:>8,}")
            total_gold_plus += r[1]
        print(f"  {'TOTAL':<20} {total_gold_plus:>8,}")

        # Gold+ property IDs CTE (used for all sub-queries below)
        gold_cte = """
            WITH latest AS (
                SELECT DISTINCT ON (property_id) property_id, lead_tier
                FROM distress_scores
                ORDER BY property_id, score_date DESC
            ),
            gold_props AS (
                SELECT property_id
                FROM latest
                WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
            )
        """

        signals = [
            ("Foreclosures",       "foreclosures",        None),
            ("Tax Delinquencies",  "tax_delinquencies",   None),
            ("Code Violations",    "code_violations",     None),
            ("Legal & Liens",      "legal_and_liens",     None),
            ("Building Permits",   "building_permits",    None),
            ("Legal Proceedings",  "legal_proceedings",   None),
            ("Incidents",          "incidents",           None),
        ]

        print("\nGold+ properties with at least one record in each signal table:")
        for label, tbl, _ in signals:
            sql = f"""
                {gold_cte}
                SELECT COUNT(DISTINCT gp.property_id)
                FROM gold_props gp
                INNER JOIN {tbl} t ON t.property_id = gp.property_id
            """
            rows, _ = run_query(session, sql)
            cnt = rows[0][0]
            pct = (cnt / total_gold_plus * 100) if total_gold_plus else 0
            print(f"  {label:<25} {cnt:>8,}  ({pct:.1f}%)")

        # Breakdown of legal_and_liens by record_type
        print("\nGold+ Legal & Liens — breakdown by record_type:")
        sql = f"""
            {gold_cte}
            SELECT lal.record_type, COUNT(*) AS record_cnt, COUNT(DISTINCT lal.property_id) AS prop_cnt
            FROM gold_props gp
            INNER JOIN legal_and_liens lal ON lal.property_id = gp.property_id
            GROUP BY lal.record_type
            ORDER BY record_cnt DESC
        """
        rows, _ = run_query(session, sql)
        for r in rows:
            print(f"  {r[0]:<20} {r[1]:>8,} records  {r[2]:>8,} props")

        # Breakdown of legal_and_liens by document_type
        print("\nGold+ Legal & Liens — breakdown by document_type:")
        sql = f"""
            {gold_cte}
            SELECT COALESCE(lal.document_type, '(null)') AS doc_type,
                   COUNT(*) AS record_cnt,
                   COUNT(DISTINCT lal.property_id) AS prop_cnt
            FROM gold_props gp
            INNER JOIN legal_and_liens lal ON lal.property_id = gp.property_id
            GROUP BY doc_type
            ORDER BY record_cnt DESC
        """
        rows, _ = run_query(session, sql)
        for r in rows:
            print(f"  {r[0]:<30} {r[1]:>8,} records  {r[2]:>8,} props")

        # Breakdown of legal_proceedings by record_type
        print("\nGold+ Legal Proceedings — breakdown by record_type (proceeding_type):")
        sql = f"""
            {gold_cte}
            SELECT lp.record_type, COUNT(*) AS record_cnt, COUNT(DISTINCT lp.property_id) AS prop_cnt
            FROM gold_props gp
            INNER JOIN legal_proceedings lp ON lp.property_id = gp.property_id
            GROUP BY lp.record_type
            ORDER BY record_cnt DESC
        """
        rows, _ = run_query(session, sql)
        for r in rows:
            print(f"  {r[0]:<20} {r[1]:>8,} records  {r[2]:>8,} props")

        # Breakdown of incidents by incident_type
        print("\nGold+ Incidents — breakdown by incident_type:")
        sql = f"""
            {gold_cte}
            SELECT COALESCE(i.incident_type, '(null)') AS itype,
                   COUNT(*) AS record_cnt,
                   COUNT(DISTINCT i.property_id) AS prop_cnt
            FROM gold_props gp
            INNER JOIN incidents i ON i.property_id = gp.property_id
            GROUP BY itype
            ORDER BY record_cnt DESC
        """
        rows, _ = run_query(session, sql)
        for r in rows:
            print(f"  {r[0]:<30} {r[1]:>8,} records  {r[2]:>8,} props")

        # ------------------------------------------------------------------ #
        # QUESTION 2: Permit types breakdown (top 50)
        # ------------------------------------------------------------------ #
        section("Q2: Building Permits — Top 50 Permit Types")

        sql = """
            SELECT COALESCE(permit_type, '(null)') AS ptype,
                   COUNT(*) AS cnt
            FROM building_permits
            GROUP BY ptype
            ORDER BY cnt DESC
            LIMIT 50
        """
        rows, _ = run_query(session, sql)
        print(f"\n{'Permit Type':<50} {'Count':>10}")
        print("-" * 62)
        for r in rows:
            print(f"  {r[0]:<48} {r[1]:>10,}")

        # Also show total
        sql_tot = "SELECT COUNT(*) FROM building_permits"
        rows2, _ = run_query(session, sql_tot)
        print(f"\n  Total building_permits rows: {rows2[0][0]:,}")

        # ------------------------------------------------------------------ #
        # QUESTION 3: Code violations by category
        # ------------------------------------------------------------------ #
        section("Q3: Code Violations by Category")

        # by violation_type
        sql = """
            SELECT COALESCE(violation_type, '(null)') AS vtype,
                   COUNT(*) AS cnt,
                   COUNT(DISTINCT property_id) AS prop_cnt
            FROM code_violations
            GROUP BY vtype
            ORDER BY cnt DESC
        """
        rows, _ = run_query(session, sql)
        print(f"\n{'Violation Type':<50} {'Records':>10} {'Properties':>12}")
        print("-" * 74)
        for r in rows:
            print(f"  {r[0]:<48} {r[1]:>10,} {r[2]:>12,}")

        # by severity_tier
        sql2 = """
            SELECT COALESCE(severity_tier, '(null)') AS stier,
                   COUNT(*) AS cnt
            FROM code_violations
            GROUP BY stier
            ORDER BY cnt DESC
        """
        rows2, _ = run_query(session, sql2)
        print("\nCode Violations by severity_tier:")
        for r in rows2:
            print(f"  {r[0]:<20} {r[1]:>10,}")

        # Total code violations
        sql_tot = "SELECT COUNT(*) FROM code_violations"
        rows3, _ = run_query(session, sql_tot)
        print(f"\n  Total code_violations rows: {rows3[0][0]:,}")

        # ------------------------------------------------------------------ #
        # QUESTION 4: Permit activity in last 12 months
        # ------------------------------------------------------------------ #
        section("Q4: Permit Activity in Last 12 Months")

        sql_total_props = "SELECT COUNT(*) FROM properties"
        rows_tp, _ = run_query(session, sql_total_props)
        total_props = rows_tp[0][0]
        print(f"\n  Total properties: {total_props:,}")

        sql_recent = """
            SELECT COUNT(DISTINCT property_id)
            FROM building_permits
            WHERE issue_date >= CURRENT_DATE - INTERVAL '365 days'
        """
        rows_rp, _ = run_query(session, sql_recent)
        recent_props = rows_rp[0][0]
        pct_recent = (recent_props / total_props * 100) if total_props else 0
        print(f"  Properties with permit in last 12 months: {recent_props:,}  ({pct_recent:.2f}%)")

        # Also break down by year for context
        sql_yr = """
            SELECT EXTRACT(YEAR FROM issue_date)::int AS yr,
                   COUNT(*) AS permit_cnt,
                   COUNT(DISTINCT property_id) AS prop_cnt
            FROM building_permits
            WHERE issue_date IS NOT NULL
            GROUP BY yr
            ORDER BY yr DESC
            LIMIT 10
        """
        rows_yr, _ = run_query(session, sql_yr)
        print("\n  Permit counts by year (top 10 most recent):")
        print(f"  {'Year':<8} {'Permits':>10} {'Properties':>12}")
        print("  " + "-" * 32)
        for r in rows_yr:
            print(f"  {r[0]:<8} {r[1]:>10,} {r[2]:>12,}")

        # ------------------------------------------------------------------ #
        # QUESTION 5: Vertical scores distribution
        # ------------------------------------------------------------------ #
        section("Q5: Vertical Scores Distribution (latest score per property)")

        # First, discover what vertical keys exist in the JSONB
        sql_keys = """
            SELECT DISTINCT jsonb_object_keys(vertical_scores) AS vkey
            FROM distress_scores
            WHERE vertical_scores IS NOT NULL
              AND vertical_scores != '{}'::jsonb
            LIMIT 100
        """
        rows_k, _ = run_query(session, sql_keys)
        verticals = sorted([r[0] for r in rows_k])
        print(f"\n  Detected vertical keys in vertical_scores JSONB: {verticals}")

        # For each vertical, get avg score and count of properties with score > 0
        # using latest score per property
        print("\n  Per-vertical stats (latest score per property):")
        print(f"  {'Vertical':<30} {'Avg Score':>12} {'Props > 0':>12} {'Props w/ data':>15}")
        print("  " + "-" * 72)

        for v in verticals:
            sql_v = f"""
                WITH latest AS (
                    SELECT DISTINCT ON (property_id)
                        property_id,
                        (vertical_scores->>'{v}')::numeric AS vscore
                    FROM distress_scores
                    WHERE vertical_scores ? '{v}'
                    ORDER BY property_id, score_date DESC
                )
                SELECT
                    ROUND(AVG(vscore)::numeric, 4)        AS avg_score,
                    COUNT(*) FILTER (WHERE vscore > 0)    AS props_gt0,
                    COUNT(*)                              AS props_total
                FROM latest
                WHERE vscore IS NOT NULL
            """
            rows_v, _ = run_query(session, sql_v)
            if rows_v and rows_v[0][0] is not None:
                avg_s, gt0, tot = rows_v[0]
                print(f"  {v:<30} {float(avg_s):>12.4f} {int(gt0):>12,} {int(tot):>15,}")
            else:
                print(f"  {v:<30} {'N/A':>12} {'N/A':>12} {'N/A':>15}")

        # Overall score distribution (lead tiers, latest per property)
        sql_tiers = """
            WITH latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, lead_tier, final_cds_score
                FROM distress_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT
                COALESCE(lead_tier, '(null)') AS tier,
                COUNT(*) AS cnt,
                ROUND(AVG(final_cds_score)::numeric, 2) AS avg_cds
            FROM latest
            GROUP BY tier
            ORDER BY cnt DESC
        """
        rows_t, _ = run_query(session, sql_tiers)
        print("\n  Lead tier distribution (latest score per property):")
        print(f"  {'Tier':<25} {'Count':>10} {'Avg CDS Score':>15}")
        print("  " + "-" * 52)
        for r in rows_t:
            print(f"  {r[0]:<25} {r[1]:>10,} {float(r[2]) if r[2] is not None else 0:>15.2f}")

        # Total scored properties
        sql_scored = """
            SELECT COUNT(DISTINCT property_id) FROM distress_scores
        """
        rows_sc, _ = run_query(session, sql_scored)
        print(f"\n  Total distinct properties ever scored: {rows_sc[0][0]:,}")

        print("\n" + "=" * 70)
        print("  ANALYSIS COMPLETE")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
