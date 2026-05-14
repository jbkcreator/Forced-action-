"""
Lead Conversion Report — tier hit-rate analysis.

Measures what % of scored leads at each tier (Ultra Platinum → Bronze)
actually transacted (deed transfer or foreclosure filing) after being scored,
within a configurable lookback window.

Usage:
    python -m src.tasks.conversion_report                        # full window, all counties
    python -m src.tasks.conversion_report --days 60              # last 60 days only
    python -m src.tasks.conversion_report --county hillsborough
    python -m src.tasks.conversion_report --csv                  # save to reports/conversion/
"""

import argparse
import csv
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

TIER_ORDER = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]

REPORTS_DIR = Path("reports/conversion")


# ---------------------------------------------------------------------------
# Core query
# ---------------------------------------------------------------------------

def _run_conversion_query(session, since: date, county_id: str) -> list[dict]:
    """
    For each tier, return total leads scored since `since`, how many
    transacted after scoring, and the hit rate.
    """
    rows = session.execute(text("""
        WITH scored AS (
            SELECT
                ds.property_id,
                MIN(ds.score_date)                                          AS first_scored_at,
                (ARRAY_AGG(ds.lead_tier ORDER BY ds.score_date ASC))[1]    AS tier
            FROM distress_scores ds
            JOIN properties p ON p.id = ds.property_id
            WHERE ds.score_date >= :since
              AND (:county = 'all' OR p.county_id = :county)
            GROUP BY ds.property_id
        ),
        deed_hits AS (
            SELECT DISTINCT s.property_id
            FROM scored s
            JOIN deeds d ON d.property_id = s.property_id
            WHERE d.record_date > s.first_scored_at
        ),
        fc_hits AS (
            SELECT DISTINCT s.property_id
            FROM scored s
            JOIN foreclosures f ON f.property_id = s.property_id
            WHERE f.filing_date > s.first_scored_at
        ),
        any_hit AS (
            SELECT property_id FROM deed_hits
            UNION
            SELECT property_id FROM fc_hits
        )
        SELECT
            s.tier,
            COUNT(DISTINCT s.property_id)                                            AS total,
            COUNT(DISTINCT h.property_id)                                            AS transacted,
            COUNT(DISTINCT dh.property_id)                                           AS deed_transfers,
            COUNT(DISTINCT fh.property_id)                                           AS foreclosures,
            ROUND(
                COUNT(DISTINCT h.property_id)::numeric /
                NULLIF(COUNT(DISTINCT s.property_id), 0) * 100, 2
            )                                                                        AS hit_rate_pct
        FROM scored s
        LEFT JOIN any_hit  h  ON h.property_id  = s.property_id
        LEFT JOIN deed_hits dh ON dh.property_id = s.property_id
        LEFT JOIN fc_hits   fh ON fh.property_id = s.property_id
        GROUP BY s.tier
    """), {"since": since, "county": county_id}).fetchall()

    # Index by tier for ordered output
    by_tier = {r[0]: r for r in rows}
    result = []
    for tier in TIER_ORDER:
        if tier not in by_tier:
            continue
        r = by_tier[tier]
        result.append({
            "tier":           r[0],
            "total":          r[1],
            "transacted":     r[2],
            "deed_transfers": r[3],
            "foreclosures":   r[4],
            "hit_rate_pct":   float(r[5]) if r[5] is not None else 0.0,
        })
    return result


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def _print_report(rows: list[dict], since: date, county_id: str) -> None:
    window_days = (date.today() - since).days
    print()
    print("=" * 65)
    print("  LEAD CONVERSION REPORT — tier hit-rate analysis")
    print(f"  Window  : {since}  ->  {date.today()}  ({window_days} days)")
    print(f"  County  : {county_id}")
    print("=" * 65)
    print(f"  {'Tier':<18} {'Total':>7} {'Converted':>10} {'Deeds':>7} {'FC':>5} {'Hit Rate':>10}")
    print("  " + "-" * 61)

    for r in rows:
        bar_len = int(r["hit_rate_pct"] / 0.5)  # 1 char per 0.5%
        bar = "#" * min(bar_len, 20)
        print(
            f"  {r['tier']:<18} {r['total']:>7,} {r['transacted']:>10,} "
            f"{r['deed_transfers']:>7,} {r['foreclosures']:>5,} "
            f"{r['hit_rate_pct']:>9.2f}%  {bar}"
        )

    total_leads      = sum(r["total"]      for r in rows)
    total_transacted = sum(r["transacted"] for r in rows)
    overall_rate     = round(total_transacted / total_leads * 100, 2) if total_leads else 0
    print("  " + "-" * 61)
    print(f"  {'ALL TIERS':<18} {total_leads:>7,} {total_transacted:>10,} {'':>7} {'':>5} {overall_rate:>9.2f}%")
    print("=" * 65)
    print()

    # Model lift: compare top tier vs bottom tier
    if len(rows) >= 2:
        top = rows[0]
        bot = rows[-1]
        if bot["hit_rate_pct"] > 0:
            lift = round(top["hit_rate_pct"] / bot["hit_rate_pct"], 1)
            print(f"  Model lift: {top['tier']} converts at {lift}x the rate of {bot['tier']}")
        print()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def _save_csv(rows: list[dict], since: date, county_id: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    fname = REPORTS_DIR / f"conversion_{since}_{date.today()}_{county_id}.csv"
    with open(fname, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tier", "total", "transacted", "deed_transfers", "foreclosures", "hit_rate_pct"
        ])
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved: {fname}")
    return fname


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_conversion_report(
    days: int = 0,
    county_id: str = "hillsborough",
    save_csv: bool = False,
) -> list[dict]:
    if days:
        since = date.today() - timedelta(days=days)
    else:
        # Default: full available window — earliest score date
        with get_db_context() as s:
            row = s.execute(text("SELECT MIN(score_date) FROM distress_scores")).fetchone()
            since = row[0].date() if row[0] else date.today()

    with get_db_context() as session:
        rows = _run_conversion_query(session, since, county_id)

    _print_report(rows, since, county_id)

    if save_csv:
        _save_csv(rows, since, county_id)

    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lead conversion report by tier")
    parser.add_argument("--days",    type=int, default=0,
                        help="Lookback window in days (default: full history)")
    parser.add_argument("--county",  dest="county_id", default="hillsborough")
    parser.add_argument("--csv",     dest="save_csv", action="store_true",
                        help="Save output to reports/conversion/")
    args = parser.parse_args()

    run_conversion_report(
        days=args.days,
        county_id=args.county_id,
        save_csv=args.save_csv,
    )
