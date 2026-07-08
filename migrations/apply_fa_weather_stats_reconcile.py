"""
Reconcile inflated storm_damage / flood_damage rows in scraper_run_stats.

Background: the old blanket storm/flood engines created one Incident per property
across whole ZIPs/counties and recorded matched=<blanket count> (e.g. 523k on
2026-03-18). Those incidents were later purged, and the fixed targeted engine
now records realistic counts — but the historical stat rows were left behind,
overstating matched/total_scraped in every aggregate report (daily/weekly/
monthly/one-pager/dashboard) whose window covers those dates.

This script rewrites each storm_damage/flood_damage stat row so its counts equal
the ACTUAL incidents of that type still on disk for that run_date. Rows with no
surviving incidents collapse to zero. Idempotent: re-running is a no-op once
counts already match reality.

Run once against the shared DB:
    PYTHONPATH=. python migrations/apply_fa_weather_stats_reconcile.py
    PYTHONPATH=. python migrations/apply_fa_weather_stats_reconcile.py --dry-run
"""
import argparse
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

RECONCILE_SQL = """
UPDATE scraper_run_stats s
SET total_scraped = COALESCE(a.n, 0),
    matched       = COALESCE(a.n, 0),
    unmatched     = 0,
    skipped       = 0,
    updated_at    = now()
FROM (
    SELECT st.id,
           (SELECT count(*) FROM incidents i
             WHERE i.incident_type = st.source_type
               AND i.date_added::date = st.run_date
               AND (st.county_id IS NULL OR i.county_id = st.county_id)) AS n
    FROM scraper_run_stats st
    WHERE st.source_type IN ('storm_damage','flood_damage')
) a
WHERE s.id = a.id
  AND (s.total_scraped <> COALESCE(a.n,0)
       OR s.matched   <> COALESCE(a.n,0)
       OR s.unmatched <> 0
       OR s.skipped   <> 0);
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_db_context() as db:
        before = db.execute(text(
            "SELECT COALESCE(sum(total_scraped),0) sc, COALESCE(sum(matched),0) m "
            "FROM scraper_run_stats WHERE source_type IN ('storm_damage','flood_damage')"
        )).fetchone()
        print(f"before: total_scraped={before.sc} matched={before.m}")

        if args.dry_run:
            preview = db.execute(text("""
                SELECT st.run_date, st.source_type, st.county_id, st.matched AS old_matched,
                       (SELECT count(*) FROM incidents i
                         WHERE i.incident_type = st.source_type
                           AND i.date_added::date = st.run_date
                           AND (st.county_id IS NULL OR i.county_id = st.county_id)) AS new_matched
                FROM scraper_run_stats st
                WHERE st.source_type IN ('storm_damage','flood_damage') AND st.matched > 0
                ORDER BY st.matched DESC
            """)).fetchall()
            for r in preview:
                print(f"  {r.run_date} {r.source_type:<13} {r.county_id:<13} "
                      f"{r.old_matched:>9} -> {r.new_matched}")
            print("dry-run: no changes written")
            return 0

        result = db.execute(text(RECONCILE_SQL))
        db.commit()
        after = db.execute(text(
            "SELECT COALESCE(sum(total_scraped),0) sc, COALESCE(sum(matched),0) m "
            "FROM scraper_run_stats WHERE source_type IN ('storm_damage','flood_damage')"
        )).fetchone()
        print(f"rows updated: {result.rowcount}")
        print(f"after:  total_scraped={after.sc} matched={after.m}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
