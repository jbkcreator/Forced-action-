"""
Monthly operations report — CSV export.

Queries scraper_run_stats, platform_daily_stats, and distress_scores for a full
calendar month and writes a structured CSV to reports/monthly/report_YYYY-MM.csv.

Usage:
    python -m src.tasks.monthly_report                    # current month, hillsborough
    python -m src.tasks.monthly_report --month 2026-03    # specific month
    python -m src.tasks.monthly_report --county hillsborough

Cron (run on the 1st of each month for the prior month, e.g. 6 AM):
    0 6 1 * * cd /path/to/app && python -m src.tasks.monthly_report --month $(date -d 'last month' +%%Y-%%m) >> logs/monthly_report.log 2>&1
"""

import argparse
import calendar
import csv
import logging
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import func

from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit, CodeViolation, Deed, DistressScore, Foreclosure,
    Incident, LegalAndLien, LegalProceeding, PlatformDailyStats,
    ScraperRunStats, TaxDelinquency, UnmatchedRecord,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/monthly")

# Display order for scraper rows
SCRAPER_ORDER = [
    "judgments",
    "permits",
    "deeds",
    "violations",
    "probate",
    "roofing_permits",
    "evictions",
    "lien_ml",
    "lien_tcl",
    "lien_hoa",
    "lien_ccl",
    "lien_tl",
    "foreclosures",
    "bankruptcy",
    "tax_delinquencies",
    "flood_damage",
    "insurance_claims",
    "storm_damage",
    "fire_incidents",
]

VERTICAL_BUCKETS = {
    "roofing":          "Roofing",
    "restoration":      "Remediation",
    "wholesalers":      "Wholesale / Investor",
    "fix_flip":         "Wholesale / Investor",
    "public_adjusters": "Wholesale / Investor",
    "attorneys":        "Wholesale / Investor",
}

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

_SOURCE_MODELS = [
    BuildingPermit, CodeViolation, Deed, Foreclosure,
    Incident, LegalAndLien, LegalProceeding, TaxDelinquency,
]


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _build_source_totals(session, month_start: date, month_end: date, county_id: str) -> list:
    """Per-scraper totals for the month from ScraperRunStats."""
    rows = (
        session.query(
            ScraperRunStats.source_type,
            func.sum(ScraperRunStats.total_scraped).label("scraped"),
            func.sum(ScraperRunStats.matched).label("matched"),
            func.sum(ScraperRunStats.unmatched).label("unmatched"),
        )
        .filter(
            ScraperRunStats.run_date >= month_start,
            ScraperRunStats.run_date <= month_end,
            ScraperRunStats.county_id == county_id,
        )
        .group_by(ScraperRunStats.source_type)
        .all()
    )

    totals_by_type = {r.source_type: r for r in rows}
    ordered = [t for t in SCRAPER_ORDER if t in totals_by_type]
    extras  = sorted(t for t in totals_by_type if t not in SCRAPER_ORDER)

    result = []
    for source_type in ordered + extras:
        r = totals_by_type[source_type]
        scraped   = int(r.scraped   or 0)
        matched   = int(r.matched   or 0)
        unmatched = int(r.unmatched or 0)
        result.append({
            "label":     source_type.replace("_", " ").title(),
            "scraped":   scraped,
            "matched":   matched   if scraped > 0 else None,
            "unmatched": unmatched if scraped > 0 else None,
            "match_pct": (matched / scraped * 100) if scraped else 0.0,
        })

    for source_type in SCRAPER_ORDER:
        if source_type not in totals_by_type:
            result.append({
                "label": source_type.replace("_", " ").title(),
                "scraped": 0, "matched": None, "unmatched": None, "match_pct": 0.0,
            })

    return result


def _reconstruct_matched_by_day(session, month_start: date, month_end: date, county_id: str) -> dict:
    """Sum matched records per day from raw source tables (fallback for days with no ScraperRunStats)."""
    counts: dict = defaultdict(int)
    for model in _SOURCE_MODELS:
        rows = (
            session.query(model.date_added, func.count().label("cnt"))
            .filter(
                model.date_added >= month_start,
                model.date_added <= month_end,
                model.county_id == county_id,
            )
            .group_by(model.date_added)
            .all()
        )
        for day, cnt in rows:
            counts[day] += cnt
    return counts


def _reconstruct_unmatched_by_day(session, month_start: date, month_end: date, county_id: str) -> dict:
    """Sum unmatched records per day from unmatched_records table."""
    rows = (
        session.query(
            func.date(UnmatchedRecord.date_added).label("day"),
            func.count().label("cnt"),
        )
        .filter(
            func.date(UnmatchedRecord.date_added) >= month_start,
            func.date(UnmatchedRecord.date_added) <= month_end,
            UnmatchedRecord.county_id == county_id,
        )
        .group_by(func.date(UnmatchedRecord.date_added))
        .all()
    )
    return {r.day: r.cnt for r in rows}


def _build_daily_history(session, month_start: date, month_end: date, county_id: str) -> list:
    """Daily breakdown: scraper counts + Gold+ tier counts for every day in the month."""
    # Official ScraperRunStats
    stat_rows = (
        session.query(
            ScraperRunStats.run_date.label("day"),
            func.sum(ScraperRunStats.total_scraped).label("scraped"),
            func.sum(ScraperRunStats.matched).label("matched"),
        )
        .filter(
            ScraperRunStats.run_date >= month_start,
            ScraperRunStats.run_date <= month_end,
            ScraperRunStats.county_id == county_id,
        )
        .group_by(ScraperRunStats.run_date)
        .all()
    )
    scraper_by_day = {r.day: {"scraped": int(r.scraped or 0), "matched": int(r.matched or 0)} for r in stat_rows}

    # Reconstruct earlier days missing from ScraperRunStats
    matched_by_day   = _reconstruct_matched_by_day(session, month_start, month_end, county_id)
    unmatched_by_day = _reconstruct_unmatched_by_day(session, month_start, month_end, county_id)

    all_days_set = set(scraper_by_day.keys()) | set(matched_by_day.keys()) | set(unmatched_by_day.keys())
    for day in all_days_set:
        if day not in scraper_by_day:
            matched   = matched_by_day.get(day, 0)
            unmatched = unmatched_by_day.get(day, 0)
            scraper_by_day[day] = {"scraped": matched + unmatched, "matched": matched}

    # Gold+ tier counts per day
    tier_rows = (
        session.query(
            func.date(DistressScore.score_date).label("day"),
            DistressScore.lead_tier,
            func.count().label("cnt"),
        )
        .filter(
            func.date(DistressScore.score_date) >= month_start,
            func.date(DistressScore.score_date) <= month_end,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .group_by(func.date(DistressScore.score_date), DistressScore.lead_tier)
        .all()
    )
    tier_by_day = defaultdict(lambda: {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0})
    for day, tier, cnt in tier_rows:
        tier_by_day[day][tier] = cnt

    all_days = sorted(set(scraper_by_day.keys()) | set(tier_by_day.keys()), reverse=True)

    history = []
    for day in all_days:
        sc = scraper_by_day.get(day, {"scraped": 0, "matched": 0})
        scraped   = sc["scraped"]
        matched   = sc["matched"]
        match_pct = (matched / scraped * 100) if scraped else 0.0
        up = tier_by_day[day]["Ultra Platinum"]
        pl = tier_by_day[day]["Platinum"]
        go = tier_by_day[day]["Gold"]
        history.append({
            "date":           str(day),
            "scraped":        scraped,
            "matched":        matched,
            "match_pct":      match_pct,
            "ultra_platinum": up,
            "platinum":       pl,
            "gold":           go,
            "total_gold_plus": up + pl + go,
        })
    return history


def _build_scoring_totals(session, month_start: date, month_end: date, county_id: str) -> dict:
    """Aggregate platform_daily_stats for the full month."""
    rows = (
        session.query(PlatformDailyStats)
        .filter(
            PlatformDailyStats.run_date >= month_start,
            PlatformDailyStats.run_date <= month_end,
            PlatformDailyStats.county_id == county_id,
        )
        .all()
    )
    totals = {
        "properties_scored":       0,
        "properties_with_signals": 0,
        "leads_new":               0,
        "leads_updated":           0,
        "leads_unchanged":         0,
        "tier_ultra_platinum":     0,
        "tier_platinum":           0,
        "tier_gold":               0,
        "tier_silver":             0,
        "tier_bronze":             0,
    }
    for r in rows:
        totals["properties_scored"]       += r.properties_scored or 0
        totals["properties_with_signals"] += r.properties_with_signals or 0
        totals["leads_new"]               += r.leads_new or 0
        totals["leads_updated"]           += r.leads_updated or 0
        totals["leads_unchanged"]         += r.leads_unchanged or 0
        totals["tier_ultra_platinum"]     += r.tier_ultra_platinum or 0
        totals["tier_platinum"]           += r.tier_platinum or 0
        totals["tier_gold"]               += r.tier_gold or 0
        totals["tier_silver"]             += r.tier_silver or 0
        totals["tier_bronze"]             += r.tier_bronze or 0
    return totals


def _build_vertical_breakdown(session, month_start: date, month_end: date, county_id: str) -> dict:
    """Gold+ leads for the month broken down by primary vertical bucket."""
    rows = (
        session.query(DistressScore)
        .filter(
            func.date(DistressScore.score_date) >= month_start,
            func.date(DistressScore.score_date) <= month_end,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .all()
    )

    bucket_counts = defaultdict(int)
    unclassified = 0

    for r in rows:
        vs = r.vertical_scores or {}
        if not vs:
            unclassified += 1
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        bucket = VERTICAL_BUCKETS.get(best)
        if bucket:
            bucket_counts[bucket] += 1
        else:
            unclassified += 1

    total = sum(bucket_counts.values()) + unclassified
    result = {}
    for bucket in ["Roofing", "Remediation", "Wholesale / Investor"]:
        cnt = bucket_counts.get(bucket, 0)
        pct = (cnt / total * 100) if total else 0.0
        result[bucket] = {"count": cnt, "pct": pct}
    if unclassified:
        result["Other / Unclassified"] = {
            "count": unclassified,
            "pct": (unclassified / total * 100) if total else 0.0,
        }
    result["_total"] = total
    return result


def build_report(month_start: date, month_end: date, county_id: str) -> dict:
    with get_db_context() as session:
        source_totals      = _build_source_totals(session, month_start, month_end, county_id)
        daily_history      = _build_daily_history(session, month_start, month_end, county_id)
        scoring_totals     = _build_scoring_totals(session, month_start, month_end, county_id)
        vertical_breakdown = _build_vertical_breakdown(session, month_start, month_end, county_id)

    return {
        "month_start":        month_start,
        "month_end":          month_end,
        "county_id":          county_id,
        "source_totals":      source_totals,
        "daily_history":      daily_history,
        "scoring_totals":     scoring_totals,
        "vertical_breakdown": vertical_breakdown,
    }


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    month_label = report["month_start"].strftime("%B %Y")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Monthly Operations Report"])
        w.writerow([f"Month: {month_label}", f"County: {report['county_id']}",
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        w.writerow([])

        # ── Section 1: Scraper Totals (month) ────────────────────────────
        st = report["source_totals"]
        month_scraped   = sum(r["scraped"] for r in st)
        month_matched   = sum(r["matched"] or 0 for r in st)
        month_match_pct = (month_matched / month_scraped * 100) if month_scraped else 0.0

        w.writerow([f"SCRAPER INGEST — {month_label}"])
        w.writerow([f"Total: {month_scraped:,} scraped | {month_matched:,} matched ({month_match_pct:.1f}%)"])
        w.writerow([])
        w.writerow(["Scraper", "Scraped", "Matched", "Unmatched", "Match %"])
        for row in st:
            w.writerow([
                row["label"],
                f"{row['scraped']:,}",
                f"{row['matched']:,}"   if row["matched"]   is not None else "—",
                f"{row['unmatched']:,}" if row["unmatched"] is not None else "—",
                f"{row['match_pct']:.1f}%" if row["scraped"] > 0 else "—",
            ])
        w.writerow([])

        # ── Section 2: Scoring Totals (month) ────────────────────────────
        sc = report["scoring_totals"]
        w.writerow([f"SCORING TOTALS — {month_label}"])
        w.writerow([])
        w.writerow(["Metric", "Total"])
        for key, label in [
            ("properties_scored",       "Properties scored"),
            ("properties_with_signals", "Properties w/ signals"),
            ("leads_new",               "New leads"),
            ("leads_updated",           "Updated leads"),
            ("leads_unchanged",         "Unchanged leads"),
        ]:
            w.writerow([label, f"{sc[key]:,}"])
        w.writerow([])

        # ── Section 3: Tier Totals (month) ───────────────────────────────
        w.writerow(["TIER TOTALS (cumulative scoring events)"])
        w.writerow(["Tier", "Count"])
        for key, label in [
            ("tier_ultra_platinum", "Ultra Platinum"),
            ("tier_platinum",       "Platinum"),
            ("tier_gold",           "Gold"),
            ("tier_silver",         "Silver"),
            ("tier_bronze",         "Bronze"),
        ]:
            w.writerow([label, f"{sc[key]:,}"])
        w.writerow([])

        # ── Section 4: Vertical Breakdown (month) ─────────────────────────
        vb = report["vertical_breakdown"]
        total_vb = vb.pop("_total", 0)
        w.writerow([f"LEAD TYPE BREAKDOWN — Gold+ by Vertical ({month_label})"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Leads", "% of Gold+"])
        for bucket, stats in vb.items():
            w.writerow([bucket, f"{stats['count']:,}", f"{stats['pct']:.1f}%"])
        w.writerow([])

        # ── Section 5: Daily Breakdown ────────────────────────────────────
        w.writerow([f"DAILY BREAKDOWN — {month_label}"])
        w.writerow(["Date", "Scraped", "Matched", "Match %", "Ultra Platinum", "Platinum", "Gold", "Total Gold+"])
        for row in report["daily_history"]:
            w.writerow([
                row["date"],
                f"{row['scraped']:,}",
                f"{row['matched']:,}",
                f"{row['match_pct']:.1f}%",
                row["ultra_platinum"],
                row["platinum"],
                row["gold"],
                row["total_gold_plus"],
            ])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_month(month_str: str) -> tuple[date, date]:
    """Parse 'YYYY-MM' into (month_start, month_end) dates."""
    dt = datetime.strptime(month_str, "%Y-%m")
    month_start = dt.date().replace(day=1)
    last_day = calendar.monthrange(month_start.year, month_start.month)[1]
    month_end = month_start.replace(day=last_day)
    return month_start, month_end


def generate_report(month_str: str, county_id: str) -> Path:
    month_start, month_end = _parse_month(month_str)
    logger.info(f"[monthly_report] Generating for {month_str} / {county_id}")

    report = build_report(month_start, month_end, county_id)

    output_path = REPORTS_DIR / f"report_{month_str}.csv"
    write_csv(report, output_path)
    logger.info(f"[monthly_report] Written to {output_path}")

    sc = report["scoring_totals"]
    st = report["source_totals"]
    month_scraped = sum(r["scraped"] for r in st)
    month_matched = sum(r["matched"] or 0 for r in st)
    month_match_pct = (month_matched / month_scraped * 100) if month_scraped else 0.0

    print(
        f"\nForced Action Monthly Report — {month_start.strftime('%B %Y')}\n"
        f"  Ingest  : {month_scraped:,} scraped | {month_matched:,} matched ({month_match_pct:.1f}%)\n"
        f"  Leads   : {sc['leads_new']:,} new | {sc['leads_updated']:,} updated\n"
        f"  Gold+   : Ultra Plat {sc['tier_ultra_platinum']:,} | Plat {sc['tier_platinum']:,} | Gold {sc['tier_gold']:,}\n"
        f"  Days    : {len(report['daily_history'])} days with activity\n"
        f"  Saved   : {output_path}\n"
    )

    return output_path


if __name__ == "__main__":
    today = date.today()
    default_month = today.strftime("%Y-%m")

    parser = argparse.ArgumentParser(description="Generate monthly operations report CSV")
    parser.add_argument("--month", default=default_month,
                        help="Month to report (YYYY-MM), defaults to current month")
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()

    try:
        generate_report(args.month, args.county)
    except Exception as e:
        logger.error(f"[monthly_report] Failed: {e}", exc_info=True)
        sys.exit(1)
