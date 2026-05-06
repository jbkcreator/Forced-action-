"""
Follow-up diagnostic: which vertical is driving the cap-hit pile?

Read-only.
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict

from sqlalchemy import create_engine, text
from dotenv import load_dotenv


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

        # Pull all UP rows with their vertical_scores JSON, count which verticals are at 100.
        rows = conn.execute(text("""
            SELECT property_id, final_cds_score, vertical_scores
            FROM distress_scores
            WHERE score_date::date = :d
              AND final_cds_score >= 95
        """), {"d": ref_date}).all()

        per_vert_at_max = defaultdict(int)
        per_vert_above95 = defaultdict(int)
        n_drives_score = defaultdict(int)  # which vertical's score == final_cds_score
        n_caps_at_100 = 0
        for r in rows:
            vs = r.vertical_scores or {}
            if not isinstance(vs, dict):
                continue
            final = float(r.final_cds_score or 0)
            if final >= 100:
                n_caps_at_100 += 1
            # which vertical drives the final score (== max(vs.values()))?
            top = max(vs.items(), key=lambda kv: float(kv[1] or 0)) if vs else None
            if top:
                n_drives_score[top[0]] += 1
            for v, s in vs.items():
                fs = float(s or 0)
                if fs >= 100:
                    per_vert_at_max[v] += 1
                if fs >= 95:
                    per_vert_above95[v] += 1

        print(f"=== Of {len(rows):,} Ultra Platinum rows ===")
        print(f"  Cap-hit (final = 100): {n_caps_at_100:,}")
        print()

        print("=== Driving vertical (which vertical's score equals the final UP score) ===")
        total = sum(n_drives_score.values())
        for v, n in sorted(n_drives_score.items(), key=lambda kv: -kv[1]):
            pct = 100.0 * n / total if total else 0
            print(f"  {v:20s}  {n:>5,}  ({pct:>5.1f}%)")
        print()

        print("=== Per-vertical at 100 (any vertical hitting cap) ===")
        for v, n in sorted(per_vert_at_max.items(), key=lambda kv: -kv[1]):
            print(f"  {v:20s}  {n:>5,} at 100   ({per_vert_above95[v]:>5,} >= 95)")
        print()

        # ── Joint distribution: which combination of verticals is most common at UP? ──
        from collections import Counter
        combo_counter = Counter()
        for r in rows:
            vs = r.vertical_scores or {}
            if not isinstance(vs, dict):
                continue
            hot = tuple(sorted(v for v, s in vs.items() if float(s or 0) >= 95))
            combo_counter[hot] += 1
        print("=== Top 10 vertical combinations among UP (vertical, score >= 95 each) ===")
        for combo, n in combo_counter.most_common(10):
            label = ",".join(combo) if combo else "(none >=95)"
            print(f"  {n:>5,}  {label}")
        print()

        # ── What if we lowered restoration code_violations from 90 → 80?
        # We can't simulate rescore from this output alone, but we can show the
        # "headroom" between final_cds_score and the second-best vertical, to give
        # an idea how much would shift.
        print("=== If we cut the cap-hit pile, how concentrated is the impact? ===")
        # Show: for each cap-hit (=100) row, by how much would the final score drop
        # if we shaved 10 points off the leading vertical?
        shaved = []
        for r in rows:
            vs = r.vertical_scores or {}
            if not isinstance(vs, dict):
                continue
            if float(r.final_cds_score or 0) < 100:
                continue
            sorted_v = sorted(vs.items(), key=lambda kv: -float(kv[1] or 0))
            top_v, top_s = sorted_v[0]
            second_s = float(sorted_v[1][1]) if len(sorted_v) > 1 else 0.0
            # If we shaved 10 from top vertical (90 base instead of 100), new final = max(top-10, second)
            new_final = max(float(top_s) - 10.0, second_s)
            shaved.append(new_final)
        if shaved:
            buckets = Counter()
            for s in shaved:
                if s >= 100:
                    buckets["100 still"] += 1
                elif s >= 98:
                    buckets["98-99"] += 1
                elif s >= 95:
                    buckets["95-97"] += 1
                elif s >= 90:
                    buckets["90-94"] += 1
                elif s >= 83:
                    buckets["83-89 (Platinum)"] += 1
                elif s >= 57:
                    buckets["57-82 (Gold)"] += 1
                else:
                    buckets["<57"] += 1
            print(f"  If top-vertical gets -10 pts (e.g. restoration code_violations 90 -> 80):")
            for label in ["100 still","98-99","95-97","90-94","83-89 (Platinum)","57-82 (Gold)","<57"]:
                if label in buckets:
                    print(f"    {label:24s}  {buckets[label]:>5,}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
