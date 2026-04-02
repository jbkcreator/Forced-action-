"""
Match Rate Trend Report
=======================
Shows historical match rate by day and by source, with a 7-day rolling average.
Used to assess data quality trajectory before scaling to new counties.

Usage:
    python scripts/match_rate_trend.py
    python scripts/match_rate_trend.py --county hillsborough --days 30
    python scripts/match_rate_trend.py --by-source
"""
import argparse
from datetime import date, timedelta
from collections import defaultdict

from sqlalchemy import text

from src.core.database import get_db_context


# Sources excluded from match rate calculation — these are bulk/reference
# loads that always match 100% and would skew the trend (e.g. master parcel,
# flood_damage from HCPA bulk load).
EXCLUDE_SOURCES = {"flood_damage", "storm_damage", "fire_incidents"}

TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]


def fetch_daily_totals(db, county_id: str, since: date):
    """Overall match rate per day (all sources combined, excluding bulk loads)."""
    rows = db.execute(text("""
        SELECT
            run_date,
            SUM(total_scraped) AS scraped,
            SUM(matched)       AS matched,
            SUM(CASE WHEN run_success THEN 0 ELSE 1 END) AS failures
        FROM scraper_run_stats
        WHERE county_id   = :county
          AND run_date    >= :since
          AND source_type NOT IN :exclude
          AND total_scraped > 0
        GROUP BY run_date
        ORDER BY run_date
    """), {"county": county_id, "since": since,
           "exclude": tuple(EXCLUDE_SOURCES)}).fetchall()
    return rows


def fetch_source_totals(db, county_id: str, since: date):
    """Match rate per source per day."""
    rows = db.execute(text("""
        SELECT
            run_date,
            source_type,
            total_scraped,
            matched,
            run_success,
            error_message
        FROM scraper_run_stats
        WHERE county_id   = :county
          AND run_date    >= :since
          AND source_type NOT IN :exclude
          AND total_scraped > 0
        ORDER BY run_date, source_type
    """), {"county": county_id, "since": since,
           "exclude": tuple(EXCLUDE_SOURCES)}).fetchall()
    return rows


def rolling_avg(values: list, window: int = 7) -> list:
    """Return rolling average for each position (left-padded with None)."""
    result = []
    for i, v in enumerate(values):
        if v is None:
            result.append(None)
            continue
        window_vals = [x for x in values[max(0, i - window + 1):i + 1] if x is not None]
        result.append(sum(window_vals) / len(window_vals) if window_vals else None)
    return result


def print_daily_trend(rows, window: int = 7):
    if not rows:
        print("No data.")
        return

    dates = [r[0] for r in rows]
    scraped_list = [r[1] for r in rows]
    matched_list = [r[2] for r in rows]
    failure_list = [r[3] for r in rows]
    rates = [
        (m / s * 100) if s else None
        for m, s in zip(matched_list, scraped_list)
    ]
    roll = rolling_avg(rates, window)

    print(f"\n{'='*72}")
    print(f"  OVERALL MATCH RATE TREND  |  county: hillsborough  |  {window}-day rolling avg")
    print(f"{'='*72}")
    print(f"  {'Date':<12} {'Scraped':>8} {'Matched':>8} {'Rate':>7}  {'7d Avg':>7}  {'Fails':>5}")
    print(f"  {'-'*60}")

    for i, row in enumerate(rows):
        d, scraped, matched, fails = row
        rate = rates[i]
        avg  = roll[i]
        rate_str = f"{rate:6.1f}%" if rate is not None else "    —  "
        avg_str  = f"{avg:6.1f}%" if avg  is not None else "    —  "
        fail_str = f"{fails}" if fails else "  -"
        print(f"  {str(d):<12} {scraped:>8,} {matched:>8,} {rate_str}  {avg_str}  {fail_str:>5}")

    # Summary
    all_scraped = sum(r[1] for r in rows)
    all_matched = sum(r[2] for r in rows)
    overall = all_matched / all_scraped * 100 if all_scraped else 0
    valid_rates = [r for r in rates if r is not None]
    first_week  = sum(valid_rates[:7])  / len(valid_rates[:7])  if valid_rates[:7]  else 0
    last_week   = sum(valid_rates[-7:]) / len(valid_rates[-7:]) if valid_rates[-7:] else 0
    delta = last_week - first_week
    direction = "^ improving" if delta > 1 else ("v declining" if delta < -1 else "= stable")

    print(f"  {'-'*60}")
    print(f"  Period total : {all_scraped:,} scraped / {all_matched:,} matched = {overall:.1f}%")
    print(f"  First 7 days : {first_week:.1f}%   Last 7 days: {last_week:.1f}%   Trend: {direction} ({delta:+.1f}pp)")
    print()


def print_source_trend(rows):
    if not rows:
        print("No data.")
        return

    # Aggregate per source
    by_source = defaultdict(lambda: {"scraped": 0, "matched": 0, "days": 0, "fails": 0})
    for run_date, source, scraped, matched, success, _ in rows:
        by_source[source]["scraped"] += scraped
        by_source[source]["matched"] += matched
        by_source[source]["days"]    += 1
        if not success:
            by_source[source]["fails"] += 1

    print(f"\n{'='*72}")
    print(f"  MATCH RATE BY SOURCE  (period totals)")
    print(f"{'='*72}")
    print(f"  {'Source':<22} {'Scraped':>9} {'Matched':>9} {'Rate':>7} {'Days':>5} {'Fails':>6}")
    print(f"  {'-'*62}")

    # Sort by match rate ascending (worst first — easier to spot problems)
    sorted_sources = sorted(
        by_source.items(),
        key=lambda x: x[1]["matched"] / x[1]["scraped"] if x[1]["scraped"] else 0
    )

    for source, s in sorted_sources:
        rate = (s["matched"] / s["scraped"] * 100) if s["scraped"] else 0
        flag = " !" if rate < 75 else ""
        print(f"  {source:<22} {s['scraped']:>9,} {s['matched']:>9,} {rate:>6.1f}%{flag:2} "
              f"{s['days']:>5} {s['fails']:>6}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Match rate trend report")
    parser.add_argument("--county",     default="hillsborough")
    parser.add_argument("--days",       type=int, default=30,
                        help="Number of days of history to show")
    parser.add_argument("--by-source",  action="store_true",
                        help="Also show per-source breakdown")
    parser.add_argument("--window",     type=int, default=7,
                        help="Rolling average window in days")
    args = parser.parse_args()

    since = date.today() - timedelta(days=args.days)

    with get_db_context() as db:
        daily_rows  = fetch_daily_totals(db, args.county, since)
        source_rows = fetch_source_totals(db, args.county, since) if args.by_source else []

    print_daily_trend(daily_rows, window=args.window)
    if args.by_source:
        print_source_trend(source_rows)


if __name__ == "__main__":
    main()
