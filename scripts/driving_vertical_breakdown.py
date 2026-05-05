"""
Per-vertical 'driving vertical' Gold+ counts — confirms why Public Adjusters
appears tiny when measured by the assigned tier.

Read-only.
"""
from __future__ import annotations
import os, sys
from collections import Counter
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
        print(f"Reference scoring date: {ref_date} ({biggest.n:,} rows)\n")

        # Lens B: properties whose DRIVING (highest-scoring) vertical is V, at OLD thresholds
        rows = conn.execute(text("""
            SELECT vertical_scores
            FROM distress_scores
            WHERE score_date::date = :d
              AND vertical_scores IS NOT NULL
        """), {"d": ref_date}).all()

        per_vert = {v: {"UP": 0, "P": 0, "G": 0, "Gold+": 0, "S": 0, "B": 0, "drives": 0} for v in VERTICALS}
        ties_count = 0
        for r in rows:
            vs = r.vertical_scores
            if not isinstance(vs, dict) or not vs:
                continue
            # Find the single max-scoring vertical (ties: alphabetical first wins, like Python max)
            sorted_v = sorted(vs.items(), key=lambda kv: (-float(kv[1] or 0), kv[0]))
            top_v, top_s = sorted_v[0]
            second_s = float(sorted_v[1][1] or 0) if len(sorted_v) > 1 else 0
            if abs(float(top_s or 0) - second_s) < 0.001:
                ties_count += 1  # ambiguous driving vertical
            s = float(top_s or 0)
            if top_v in per_vert:
                per_vert[top_v]["drives"] += 1
                if s >= 95: per_vert[top_v]["UP"] += 1
                elif s >= 83: per_vert[top_v]["P"] += 1
                elif s >= 57: per_vert[top_v]["G"] += 1
                elif s >= 40: per_vert[top_v]["S"] += 1
                else: per_vert[top_v]["B"] += 1
                if s >= 57: per_vert[top_v]["Gold+"] += 1

        print("=== Driving-vertical lens: 'this vertical IS the highest-scoring one' ===")
        print(f"   (Old thresholds 95/83/57. Each property counted exactly once.)\n")
        print(f"  {'vertical':20s}  {'UP':>5s}  {'P':>5s}  {'G':>5s}  {'Gold+':>6s}  {'S':>5s}  {'B':>5s}  {'drives':>7s}")
        for v in VERTICALS:
            d = per_vert[v]
            print(f"  {v:20s}  {d['UP']:>5,}  {d['P']:>5,}  {d['G']:>5,}  {d['Gold+']:>6,}  {d['S']:>5,}  {d['B']:>5,}  {d['drives']:>7,}")
        print(f"\n  Properties with two-way tie at top score: {ties_count:,}")

        # Why is PA almost never the driver? Show its rank distribution.
        print("\n=== Public Adjusters rank in vertical_scores (where rank=1 means highest) ===")
        rank_dist = Counter()
        for r in rows:
            vs = r.vertical_scores
            if not isinstance(vs, dict) or "public_adjusters" not in vs:
                continue
            pa = float(vs["public_adjusters"] or 0)
            higher = sum(1 for v, s in vs.items() if float(s or 0) > pa and v != "public_adjusters")
            rank_dist[higher + 1] += 1
        for rank in sorted(rank_dist):
            print(f"  rank {rank:>2}  {rank_dist[rank]:>5,}")

        # When PA = restoration (tied), does PA ever win? (Python max picks the FIRST in dict order.)
        # In Python the dict order matches insertion order so it depends on how vertical_scores was built.
        # Check: how often is PA score == restoration score AND both >=57?
        pa_eq_resto = conn.execute(text("""
            SELECT COUNT(*) FROM distress_scores
            WHERE score_date::date = :d
              AND (vertical_scores->>'public_adjusters')::float = (vertical_scores->>'restoration')::float
              AND (vertical_scores->>'public_adjusters')::float >= 57
        """), {"d": ref_date}).scalar()
        pa_gt_resto = conn.execute(text("""
            SELECT COUNT(*) FROM distress_scores
            WHERE score_date::date = :d
              AND (vertical_scores->>'public_adjusters')::float > (vertical_scores->>'restoration')::float
              AND (vertical_scores->>'public_adjusters')::float >= 57
        """), {"d": ref_date}).scalar()
        pa_lt_resto = conn.execute(text("""
            SELECT COUNT(*) FROM distress_scores
            WHERE score_date::date = :d
              AND (vertical_scores->>'public_adjusters')::float < (vertical_scores->>'restoration')::float
              AND (vertical_scores->>'public_adjusters')::float >= 57
        """), {"d": ref_date}).scalar()
        print(f"\n=== PA vs Restoration (Gold+ properties) ===")
        print(f"  PA = Restoration:  {pa_eq_resto:>5,}")
        print(f"  PA > Restoration:  {pa_gt_resto:>5,}")
        print(f"  PA < Restoration:  {pa_lt_resto:>5,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
