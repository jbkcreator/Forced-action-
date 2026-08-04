"""
Diagnostic: measure actual homeowner vs investor split in current whale list.
Run: PYTHONPATH=. python scripts/check_whale_split.py
"""
import os
import sys

os.environ.setdefault("DATABASE_URL", "postgresql://distress_user:distressadmin123@5.78.184.159:5432/distress_db")

from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

with engine.connect() as conn:
    # 1. Overall whale entity breakdown
    r1 = conn.execute(text("""
        SELECT
            COUNT(*)                                                        AS total_whales,
            COUNT(*) FILTER (WHERE entity_type IN ('LLC', 'Corporate'))     AS llc_corporate,
            COUNT(*) FILTER (WHERE entity_type = 'Individual')              AS individuals,
            COUNT(*) FILTER (WHERE entity_type = 'Trust')                   AS trusts
        FROM buyer_entities
        WHERE is_whale = true
    """)).fetchone()

    print("=== Whale entity breakdown ===")
    print(f"  Total whales      : {r1.total_whales}")
    print(f"  LLC / Corporate   : {r1.llc_corporate}  ({r1.llc_corporate/max(r1.total_whales,1)*100:.1f}%)")
    print(f"  Individual        : {r1.individuals}  ({r1.individuals/max(r1.total_whales,1)*100:.1f}%)")
    print(f"  Trust             : {r1.trusts}  ({r1.trusts/max(r1.total_whales,1)*100:.1f}%)")

    # 2. Simulate new AND rule for Individuals
    r2 = conn.execute(text("""
        WITH recent_purchases AS (
            SELECT bel.buyer_entity_id, COUNT(DISTINCT d.property_id) AS recent_count
            FROM buyer_entity_links bel
            JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
            WHERE d.record_date >= (CURRENT_DATE - (548 * INTERVAL '1 day'))
              AND (d.sale_price IS NULL OR d.sale_price >= 1000)
            GROUP BY bel.buyer_entity_id
        )
        SELECT
            COUNT(*) AS current_whales,
            COUNT(*) FILTER (WHERE
                CASE
                    WHEN be.entity_type IN ('LLC', 'Corporate') THEN
                        (COALESCE(rp.recent_count, 0) >= 3 OR be.total_cash_volume > 500000)
                    ELSE
                        (COALESCE(rp.recent_count, 0) >= 3 AND be.total_cash_volume > 500000)
                END
            ) AS survives_new_rule,
            COUNT(*) FILTER (WHERE
                NOT CASE
                    WHEN be.entity_type IN ('LLC', 'Corporate') THEN
                        (COALESCE(rp.recent_count, 0) >= 3 OR be.total_cash_volume > 500000)
                    ELSE
                        (COALESCE(rp.recent_count, 0) >= 3 AND be.total_cash_volume > 500000)
                END
            ) AS would_be_demoted
        FROM buyer_entities be
        LEFT JOIN recent_purchases rp ON rp.buyer_entity_id = be.id
        WHERE be.is_whale = true
    """)).fetchone()

    print("\n=== Impact of AND fix (Option B) ===")
    print(f"  Current whales       : {r2.current_whales}")
    print(f"  Survive new rule     : {r2.survives_new_rule}  ({r2.survives_new_rule/max(r2.current_whales,1)*100:.1f}%)")
    print(f"  Would be demoted     : {r2.would_be_demoted}  ({r2.would_be_demoted/max(r2.current_whales,1)*100:.1f}%)")

    # 3. Check what exemption columns exist in properties
    cols = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'properties'
        ORDER BY ordinal_position
    """)).fetchall()
    prop_cols = [c[0] for c in cols]
    exemption_cols = [c for c in prop_cols if any(k in c for k in ('exempt', 'homestead', 'owner_occ'))]
    print(f"\n=== Properties exemption-related columns ===")
    print(f"  {exemption_cols if exemption_cols else 'NONE FOUND'}")
    print(f"  (all cols: {prop_cols})")

    # 3. Individual whales — purchase count distribution (proxy for investor vs homeowner)
    r3 = conn.execute(text("""
        SELECT
            COUNT(DISTINCT be.id)                                                   AS individual_whales,
            COUNT(DISTINCT be.id) FILTER (WHERE be.total_purchase_count >= 10)      AS heavy_investors,
            COUNT(DISTINCT be.id) FILTER (WHERE be.total_purchase_count BETWEEN 3 AND 5) AS borderline,
            COUNT(DISTINCT be.id) FILTER (WHERE be.total_cash_volume > 500000)      AS high_volume,
            AVG(be.total_purchase_count)                                            AS avg_purchases,
            AVG(be.total_cash_volume)                                               AS avg_volume
        FROM buyer_entities be
        WHERE be.is_whale = true AND be.entity_type = 'Individual'
    """)).fetchone()

    print("\n=== Individual whales: purchase distribution ===")
    print(f"  Individual whales total    : {r3.individual_whales}")
    print(f"  Heavy investors (10+)      : {r3.heavy_investors}  ({r3.heavy_investors/max(r3.individual_whales,1)*100:.1f}%)")
    print(f"  Borderline (3-5 purchases) : {r3.borderline}  ({r3.borderline/max(r3.individual_whales,1)*100:.1f}%)")
    print(f"  High cash volume (>$500K)  : {r3.high_volume}  ({r3.high_volume/max(r3.individual_whales,1)*100:.1f}%)")
    print(f"  Avg purchases              : {float(r3.avg_purchases or 0):.1f}")
    print(f"  Avg cash volume            : ${float(r3.avg_volume or 0):,.0f}")
