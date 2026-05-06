"""
Diagnostic for the 5 follow-up questions about tier shape, vertical balance,
permit signals, and dedup. Read-only.
"""
from __future__ import annotations
import os, sys, json
from collections import Counter, defaultdict
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

VERTICALS = ["wholesalers", "fix_flip", "restoration", "roofing", "public_adjusters", "attorneys"]


def main() -> int:
    load_dotenv()
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.connect() as conn:
        biggest = conn.execute(text("""
            SELECT score_date::date AS d, COUNT(*) AS n
            FROM distress_scores
            GROUP BY 1 ORDER BY n DESC LIMIT 1
        """)).one()
        ref_date = biggest.d
        print(f"Reference scoring date: {ref_date} ({biggest.n:,} rows)")
        print()

        # ── Q1 sanity: how many rows in distress_scores total, vs distinct properties? ──
        totals = conn.execute(text("""
            SELECT
              COUNT(*) AS rows_total,
              COUNT(DISTINCT property_id) AS props_total,
              MIN(score_date::date) AS earliest,
              MAX(score_date::date) AS latest
            FROM distress_scores
        """)).one()
        print(f"=== Q1: Score table stats ===")
        print(f"  Total rows in distress_scores:    {totals.rows_total:,}")
        print(f"  Distinct properties scored:       {totals.props_total:,}")
        print(f"  Date range: {totals.earliest} -> {totals.latest}")
        print()

        # ── Q2: Restoration tier shape using THREE queries: per-vertical, driving-vertical, assigned-tier
        print(f"=== Q2: Restoration tier shape — three different lenses ===")
        # Lens A: per-vertical score (a property counts in restoration if its restoration vertical_score >= threshold)
        per_vertical = conn.execute(text("""
            SELECT
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 92 THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 78 AND (vertical_scores->>'restoration')::float < 92 THEN 1 ELSE 0 END) AS plat,
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 55 AND (vertical_scores->>'restoration')::float < 78 THEN 1 ELSE 0 END) AS gold,
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 55 THEN 1 ELSE 0 END) AS gold_plus
            FROM distress_scores WHERE score_date::date = :d
        """), {"d": ref_date}).one()
        print(f"  Lens A — vertical_scores->'restoration' band (NEW thresholds 92/78/55):")
        print(f"    Ultra Platinum: {per_vertical.up:>5,}    Platinum: {per_vertical.plat:>5,}    Gold: {per_vertical.gold:>5,}    Gold+: {per_vertical.gold_plus:>5,}")
        # at OLD thresholds for comparison
        old = conn.execute(text("""
            SELECT
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 95 THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 83 AND (vertical_scores->>'restoration')::float < 95 THEN 1 ELSE 0 END) AS plat,
              SUM(CASE WHEN (vertical_scores->>'restoration')::float >= 57 AND (vertical_scores->>'restoration')::float < 83 THEN 1 ELSE 0 END) AS gold
            FROM distress_scores WHERE score_date::date = :d
        """), {"d": ref_date}).one()
        print(f"  Lens A — same lens at OLD thresholds 95/83/57 (matches user's reported numbers):")
        print(f"    Ultra Platinum: {old.up:>5,}    Platinum: {old.plat:>5,}    Gold: {old.gold:>5,}")

        # Lens B: driving vertical (max-vertical IS restoration AND that score >= UP threshold)
        rows = conn.execute(text("""
            SELECT vertical_scores
            FROM distress_scores
            WHERE score_date::date = :d
              AND vertical_scores IS NOT NULL
        """), {"d": ref_date}).all()
        bands = {"UP": 0, "P": 0, "G": 0}
        for r in rows:
            vs = r.vertical_scores
            if not isinstance(vs, dict) or not vs:
                continue
            top_v, top_s = max(vs.items(), key=lambda kv: float(kv[1] or 0))
            if top_v != "restoration":
                continue
            s = float(top_s or 0)
            if s >= 95: bands["UP"] += 1
            elif s >= 83: bands["P"] += 1
            elif s >= 57: bands["G"] += 1
        print(f"  Lens B — restoration is the DRIVING vertical (single max), at OLD thresholds 95/83/57:")
        print(f"    Ultra Platinum: {bands['UP']:>5,}    Platinum: {bands['P']:>5,}    Gold: {bands['G']:>5,}")
        print()

        # ── Q3: per-vertical Gold+ counts at OLD thresholds (matches user reporting style)
        print(f"=== Q3: Per-vertical Gold+ counts (OLD thresholds 95/83/57, vertical_scores lens) ===")
        for v in VERTICALS:
            row = conn.execute(text(f"""
                SELECT
                  SUM(CASE WHEN (vertical_scores->>'{v}')::float >= 95 THEN 1 ELSE 0 END) AS up,
                  SUM(CASE WHEN (vertical_scores->>'{v}')::float >= 83 AND (vertical_scores->>'{v}')::float < 95 THEN 1 ELSE 0 END) AS p,
                  SUM(CASE WHEN (vertical_scores->>'{v}')::float >= 57 AND (vertical_scores->>'{v}')::float < 83 THEN 1 ELSE 0 END) AS g,
                  SUM(CASE WHEN (vertical_scores->>'{v}')::float >= 57 THEN 1 ELSE 0 END) AS gold_plus
                FROM distress_scores WHERE score_date::date = :d
            """), {"d": ref_date}).one()
            print(f"  {v:20s}  UP {row.up:>5,}    P {row.p:>5,}    G {row.g:>5,}    Gold+ {row.gold_plus:>5,}")
        print()

        # ── Q4: enforcement_permit reality check — count vs roofing classifier
        print(f"=== Q4: Building permits — enforcement vs roofing classifier ===")
        bp_total = conn.execute(text("""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN is_enforcement_permit THEN 1 ELSE 0 END) AS enforcement,
              SUM(CASE WHEN is_enforcement_permit IS NULL OR NOT is_enforcement_permit THEN 1 ELSE 0 END) AS regular
            FROM building_permits
        """)).one()
        print(f"  Total building_permits rows:        {bp_total.total:>6,}")
        print(f"    enforcement (stop work / etc.):   {bp_total.enforcement:>6,}")
        print(f"    regular permits:                  {bp_total.regular:>6,}")
        bp_recent = conn.execute(text("""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN is_enforcement_permit THEN 1 ELSE 0 END) AS enforcement
            FROM building_permits
            WHERE issue_date >= NOW() - INTERVAL '30 days'
        """)).one()
        print(f"  Issued in last 30 days:             {bp_recent.total:>6,}")
        print(f"    enforcement subset:               {bp_recent.enforcement:>6,}")

        # roofing_permit incidents (the classifier output)
        try:
            roof_inc = conn.execute(text("""
                SELECT COUNT(*) AS total
                FROM incidents
                WHERE incident_type IN ('roofing_permit')
            """)).one()
            print(f"  roofing_permit incidents (cumul):   {roof_inc.total:>6,}")
        except Exception as e:
            print(f"  roofing_permit incidents:  (incidents table query failed: {e})")
        try:
            roof_recent = conn.execute(text("""
                SELECT COUNT(*) AS total
                FROM incidents
                WHERE incident_type IN ('roofing_permit')
                  AND incident_date >= NOW() - INTERVAL '30 days'
            """)).one()
            print(f"  roofing_permit incidents (30d):     {roof_recent.total:>6,}")
        except Exception as e:
            print(f"  roofing_permit incidents 30d: (failed: {e})")

        # Roofing vertical score-time signal mix (which signals appear in roofing UP scores)
        print()
        print(f"  Top distress_types appearing in distress_scores rows where roofing >= 80:")
        rows = conn.execute(text("""
            SELECT distress_types
            FROM distress_scores
            WHERE score_date::date = :d
              AND (vertical_scores->>'roofing')::float >= 80
        """), {"d": ref_date}).all()
        sig_count = Counter()
        for r in rows:
            for t in (r.distress_types or []):
                sig_count[t] += 1
        for sig, n in sig_count.most_common(15):
            print(f"    {sig:25s}  {n:>5,}")
        print()

        # ── Q5: Multi-vertical dedup
        print(f"=== Q5: Multi-vertical dedup ===")
        # Count: properties where 2+ verticals are >= 57 (old Gold+)
        rows = conn.execute(text("""
            SELECT vertical_scores, lead_tier, final_cds_score
            FROM distress_scores
            WHERE score_date::date = :d
              AND vertical_scores IS NOT NULL
        """), {"d": ref_date}).all()
        vert_count_distribution = Counter()
        gp_per_property = 0
        gp_per_vertical_instance_57 = 0
        gp_per_vertical_instance_55 = 0  # new threshold
        for r in rows:
            vs = r.vertical_scores
            if not isinstance(vs, dict):
                continue
            n_verts_57 = sum(1 for v in vs.values() if float(v or 0) >= 57)
            n_verts_55 = sum(1 for v in vs.values() if float(v or 0) >= 55)
            vert_count_distribution[n_verts_57] += 1
            if n_verts_57 >= 1:
                gp_per_property += 1
            gp_per_vertical_instance_57 += n_verts_57
            gp_per_vertical_instance_55 += n_verts_55
        print(f"  Distribution: how many verticals does each property reach Gold+ (>=57) in?")
        for k in sorted(vert_count_distribution):
            print(f"    {k} verticals  {vert_count_distribution[k]:>5,}")
        print()
        print(f"  Gold+ count, deduped (one row per property):              {gp_per_property:>6,}")
        print(f"  Sum of per-vertical Gold+ instances (57 cutoff):          {gp_per_vertical_instance_57:>6,}")
        print(f"  Sum of per-vertical Gold+ instances (NEW 55 cutoff):      {gp_per_vertical_instance_55:>6,}")
        if gp_per_property:
            print(f"  Inflation factor if reported per-vertical (57): {gp_per_vertical_instance_57 / gp_per_property:.2f}x")

    return 0


if __name__ == "__main__":
    sys.exit(main())
