"""
One-off diagnostic: tier distribution audit.

Read-only — emits the histogram + cap-hit ratio for the latest scoring run so
we can decide whether the Ultra Platinum threshold needs raising and/or the
stacking cap needs tightening.

Run:
    python scripts/tier_distribution_audit.py
"""
from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set in environment.", file=sys.stderr)
        return 1

    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.connect() as conn:
        # ── How many distinct score_dates exist? ──────────────────────────
        date_summary = conn.execute(text("""
            SELECT score_date::date AS d, COUNT(*) AS n
            FROM distress_scores
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 10
        """)).all()
        print("=== Recent scoring runs (top 10 score_dates by count) ===")
        for r in date_summary:
            print(f"  {r.d}  {r.n:>7,} rows")
        print()

        # Use the date with the most rows as the "current" full run.
        biggest = conn.execute(text("""
            SELECT score_date::date AS d, COUNT(*) AS n
            FROM distress_scores
            GROUP BY 1
            ORDER BY n DESC
            LIMIT 1
        """)).one()
        ref_date = biggest.d
        print(f"Using reference scoring date: {ref_date} ({biggest.n:,} rows)")
        print()

        # ── Tier counts on the reference date ─────────────────────────────
        rows = conn.execute(text("""
            SELECT lead_tier, COUNT(*) AS n, MIN(final_cds_score) AS min_s, MAX(final_cds_score) AS max_s
            FROM distress_scores
            WHERE score_date::date = :d
            GROUP BY lead_tier
            ORDER BY MIN(final_cds_score) DESC NULLS LAST
        """), {"d": ref_date}).all()
        total = sum(r.n for r in rows)
        gold_plus = sum(r.n for r in rows if r.lead_tier in ("Ultra Platinum", "Platinum", "Gold"))
        print("=== Tier counts ===")
        print(f"  {'tier':16s}  {'n':>7s}  {'% all':>7s}  {'% Gold+':>9s}  range")
        for r in rows:
            pct = 100.0 * r.n / total if total else 0.0
            pct_gold = 100.0 * r.n / gold_plus if (gold_plus and r.lead_tier in ("Ultra Platinum", "Platinum", "Gold")) else 0.0
            tag = f"{pct_gold:>8.1f}%" if pct_gold else "        —"
            print(f"  {r.lead_tier or '<null>':16s}  {r.n:>7,}  {pct:>6.1f}%  {tag}  {r.min_s}–{r.max_s}")
        print(f"  {'TOTAL':16s}  {total:>7,}")
        print(f"  {'Gold+':16s}  {gold_plus:>7,}")
        print()

        # ── Ultra Platinum cap-hit breakdown ──────────────────────────────
        bands = conn.execute(text("""
            SELECT
              CASE
                WHEN final_cds_score >= 100 THEN '100 (cap-hit)'
                WHEN final_cds_score >=  98 THEN '98-99'
                WHEN final_cds_score >=  95 THEN '95-97'
              END AS band,
              COUNT(*) AS n
            FROM distress_scores
            WHERE score_date::date = :d
              AND final_cds_score >= 95
            GROUP BY 1
            ORDER BY 1
        """), {"d": ref_date}).all()
        ultra_total = sum(r.n for r in bands)
        print(f"=== Ultra Platinum band breakdown (95-100), n={ultra_total:,} ===")
        for r in bands:
            pct = 100.0 * r.n / ultra_total if ultra_total else 0.0
            print(f"  {r.band:16s}  {r.n:>6,}  ({pct:>5.1f}% of UP)")
        print()

        # ── Histogram across Gold+ band, 1-pt buckets ─────────────────────
        hist = conn.execute(text("""
            SELECT FLOOR(final_cds_score) AS s, COUNT(*) AS n
            FROM distress_scores
            WHERE score_date::date = :d
              AND final_cds_score >= 57
            GROUP BY 1
            ORDER BY 1 DESC
        """), {"d": ref_date}).all()
        print("=== Histogram, Gold+ (1-pt buckets, descending) ===")
        running = 0
        for r in hist:
            running += r.n
            bar = "#" * min(60, max(1, r.n // max(1, hist[0].n // 60)))
            print(f"  {int(r.s):>3d}  {r.n:>6,}   cum {running:>7,}  {bar}")
        print()

        # ── What-if cutoffs ────────────────────────────────────────────────
        whatif = conn.execute(text("""
            SELECT
              SUM(CASE WHEN final_cds_score >= 95 THEN 1 ELSE 0 END) AS up_95_current,
              SUM(CASE WHEN final_cds_score >= 98 THEN 1 ELSE 0 END) AS up_98,
              SUM(CASE WHEN final_cds_score >= 99 THEN 1 ELSE 0 END) AS up_99,
              SUM(CASE WHEN final_cds_score = 100 THEN 1 ELSE 0 END) AS up_100,
              SUM(CASE WHEN final_cds_score >= 88 AND final_cds_score < 98 THEN 1 ELSE 0 END) AS plat_88,
              SUM(CASE WHEN final_cds_score >= 60 AND final_cds_score < 88 THEN 1 ELSE 0 END) AS gold_60,
              SUM(CASE WHEN final_cds_score >= 60 THEN 1 ELSE 0 END) AS gp_60,
              SUM(CASE WHEN final_cds_score >= 57 THEN 1 ELSE 0 END) AS gp_57
            FROM distress_scores
            WHERE score_date::date = :d
        """), {"d": ref_date}).one()

        print("=== What-if at proposed cutoffs ===")
        print(f"Current (UP>=95, P>=83, G>=57)")
        print(f"  Ultra Platinum (>=95):  {whatif.up_95_current:>6,}")
        print(f"  Gold+ (>=57):           {whatif.gp_57:>6,}")
        if whatif.gp_57:
            print(f"  UP share of Gold+:      {100.0 * whatif.up_95_current / whatif.gp_57:>5.1f}%")
        print()
        print(f"Proposed (UP>=98, P>=88, G>=60):")
        if whatif.gp_60:
            print(f"  Ultra Platinum (>=98):  {whatif.up_98:>6,}  ({100.0 * whatif.up_98 / whatif.gp_60:>5.1f}% of new Gold+)")
            print(f"  Platinum       (88-97): {whatif.plat_88:>6,}  ({100.0 * whatif.plat_88 / whatif.gp_60:>5.1f}% of new Gold+)")
            print(f"  Gold           (60-87): {whatif.gold_60:>6,}  ({100.0 * whatif.gold_60 / whatif.gp_60:>5.1f}% of new Gold+)")
            print(f"  Gold+ (>=60):           {whatif.gp_60:>6,}")
        print()
        print(f"Tighter alternatives:")
        print(f"  At >=99 cutoff:         {whatif.up_99:>6,} would be Ultra Platinum")
        print(f"  At  =100 (cap only):    {whatif.up_100:>6,} would be Ultra Platinum")
        print()

        # ── Sample 5 Ultra Platinum properties to see what they look like
        print("=== Sample of Ultra Platinum (>=95) — first 5 ===")
        sample = conn.execute(text("""
            SELECT property_id, final_cds_score, lead_tier, urgency_level, vertical_scores
            FROM distress_scores
            WHERE score_date::date = :d
              AND final_cds_score >= 95
            ORDER BY final_cds_score DESC, property_id
            LIMIT 5
        """), {"d": ref_date}).all()
        for r in sample:
            vs = r.vertical_scores or {}
            top_v = sorted(vs.items(), key=lambda kv: -(kv[1] or 0))[:3] if isinstance(vs, dict) else []
            top_str = ", ".join(f"{k}={v}" for k, v in top_v) if top_v else "(none)"
            print(f"  property_id={r.property_id} score={r.final_cds_score} top: {top_str}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
