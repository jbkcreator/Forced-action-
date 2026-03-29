"""
Daily operations report — CSV export.

Queries scraper_run_stats, platform_daily_stats, and distress_scores for a given date
and writes a structured CSV to reports/daily/report_YYYY-MM-DD.csv.
Files older than 7 days are automatically pruned.

Usage:
    python -m src.tasks.daily_report                     # today, hillsborough
    python -m src.tasks.daily_report --date 2026-03-25   # specific date
    python -m src.tasks.daily_report --county hillsborough

Cron (after CDS engine completes, e.g. 4 AM):
    0 4 * * 1-5 cd /path/to/app && python -m src.tasks.daily_report >> logs/daily_report.log 2>&1
"""

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import func

from src.core.database import get_db_context
from src.core.models import (
    DistressScore, PlatformDailyStats, ScraperRunStats,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/daily")
RETENTION_DAYS = 7
TIER_HISTORY_DAYS = 7  # How many days to show in the tier-by-day table

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

# Vertical → buyer bucket mapping for lead type breakdown
VERTICAL_BUCKETS = {
    "roofing":          "Roofing",
    "restoration":      "Remediation",
    "wholesalers":      "Wholesale / Investor",
    "fix_flip":         "Wholesale / Investor",
    "public_adjusters": "Wholesale / Investor",
    "attorneys":        "Wholesale / Investor",
}

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _build_scraper_section(session, run_date: date, county_id: str):
    scraper_rows = (
        session.query(ScraperRunStats)
        .filter(ScraperRunStats.run_date == run_date, ScraperRunStats.county_id == county_id)
        .all()
    )
    scraper_by_type = {r.source_type: r for r in scraper_rows}
    ordered = [t for t in SCRAPER_ORDER if t in scraper_by_type]
    extras = sorted(t for t in scraper_by_type if t not in SCRAPER_ORDER)

    scraper_data = []
    total_scraped = total_matched = 0
    errors = []

    for source_type in ordered + extras:
        row = scraper_by_type[source_type]
        total_scraped += row.total_scraped
        total_matched += row.matched
        scraper_data.append({
            "label":    source_type.replace("_", " ").title(),
            "scraped":  row.total_scraped,
            "matched":  row.matched   if row.total_scraped > 0 else None,
            "unmatched":row.unmatched if row.total_scraped > 0 else None,
            "ok":       row.run_success,
            "error":    row.error_message,
        })
        if not row.run_success:
            errors.append(f"{source_type}: {row.error_message or 'unknown error'}")

    for source_type in SCRAPER_ORDER:
        if source_type not in scraper_by_type:
            scraper_data.append({
                "label": source_type.replace("_", " ").title(),
                "scraped": 0, "matched": None, "unmatched": None,
                "ok": None, "error": "No run recorded",
            })

    match_pct = (total_matched / total_scraped * 100) if total_scraped else 0.0
    return scraper_data, total_scraped, total_matched, match_pct, errors


def _build_scoring_section(session, run_date: date, county_id: str, errors: list):
    platform_row = (
        session.query(PlatformDailyStats)
        .filter(PlatformDailyStats.run_date == run_date, PlatformDailyStats.county_id == county_id)
        .first()
    )
    if platform_row:
        scoring = {
            "properties_scored":        platform_row.properties_scored,
            "properties_with_signals":  platform_row.properties_with_signals,
            "leads_new":                platform_row.leads_new,
            "leads_updated":            platform_row.leads_updated,
            "leads_unchanged":          platform_row.leads_unchanged,
        }
        tiers = {
            "Ultra Platinum": platform_row.tier_ultra_platinum,
            "Platinum":       platform_row.tier_platinum,
            "Gold":           platform_row.tier_gold,
            "Silver":         platform_row.tier_silver,
            "Bronze":         platform_row.tier_bronze,
        }
    else:
        scoring = {k: 0 for k in ("properties_scored", "properties_with_signals",
                                   "leads_new", "leads_updated", "leads_unchanged")}
        tiers = {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0, "Silver": 0, "Bronze": 0}
        errors.append("platform_daily_stats: no row found for this date")
    return scoring, tiers


def _build_tier_history(session, run_date: date, county_id: str) -> list:
    """Gold+ counts per day for the last TIER_HISTORY_DAYS days."""
    since = run_date - timedelta(days=TIER_HISTORY_DAYS - 1)
    rows = (
        session.query(
            func.date(DistressScore.score_date).label("day"),
            DistressScore.lead_tier,
            func.count().label("cnt"),
        )
        .filter(
            func.date(DistressScore.score_date) >= since,
            func.date(DistressScore.score_date) <= run_date,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .group_by(func.date(DistressScore.score_date), DistressScore.lead_tier)
        .all()
    )

    data = defaultdict(lambda: {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0})
    for day, tier, cnt in rows:
        data[day][tier] = cnt

    history = []
    for day in sorted(data.keys(), reverse=True):
        up = data[day]["Ultra Platinum"]
        pl = data[day]["Platinum"]
        go = data[day]["Gold"]
        history.append({
            "date":          str(day),
            "ultra_platinum": up,
            "platinum":       pl,
            "gold":           go,
            "total":          up + pl + go,
        })
    return history


def _build_vertical_breakdown(session, run_date: date, county_id: str) -> dict:
    """
    For today's Gold+ leads, determine primary vertical per lead
    (highest vertical_score wins) then map to Roofing / Remediation / Wholesale+Investor.
    """
    rows = (
        session.query(DistressScore)
        .filter(
            func.date(DistressScore.score_date) == run_date,
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
        result["Other / Unclassified"] = {"count": unclassified, "pct": (unclassified / total * 100) if total else 0.0}

    result["_total"] = total
    return result


def build_report(run_date: date, county_id: str) -> dict:
    errors = []
    with get_db_context() as session:
        scraper_data, total_scraped, total_matched, match_pct, scraper_errors = \
            _build_scraper_section(session, run_date, county_id)
        errors.extend(scraper_errors)

        scoring, tiers     = _build_scoring_section(session, run_date, county_id, errors)
        vertical_breakdown = _build_vertical_breakdown(session, run_date, county_id)

    return {
        "run_date":           run_date,
        "county_id":          county_id,
        "total_scraped":      total_scraped,
        "total_matched":      total_matched,
        "match_pct":          match_pct,
        "scraper_data":       scraper_data,
        "scoring":            scoring,
        "tiers":              tiers,
        "vertical_breakdown": vertical_breakdown,
        "errors":             errors,
    }


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Daily Operations Report"])
        w.writerow([f"Date: {report['run_date']}", f"County: {report['county_id']}",
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        w.writerow([])

        # ── Section 1: Scraper Ingest (today) ────────────────────────────
        w.writerow(["SCRAPER INGEST"])
        w.writerow([f"Total: {report['total_scraped']:,} scraped | "
                    f"{report['total_matched']:,} matched ({report['match_pct']:.1f}%)"])
        w.writerow([])
        w.writerow(["Scraper", "Scraped", "Matched", "Unmatched"])
        for row in report["scraper_data"]:
            w.writerow([
                row["label"],
                row["scraped"],
                row["matched"]   if row["matched"]   is not None else "—",
                row["unmatched"] if row["unmatched"] is not None else "—",
            ])
        w.writerow([])

        # ── Section 2: Scoring Summary ────────────────────────────────────
        w.writerow(["SCORING"])
        w.writerow([f"Total Properties Scored: {report['scoring']['properties_scored']:,}"])
        w.writerow([])
        w.writerow(["Metric", "Count"])
        for key, label in [
            ("properties_with_signals", "Properties w/ signals"),
            ("leads_new",               "Leads new today"),
            ("leads_updated",           "Leads updated"),
            ("leads_unchanged",         "Leads unchanged"),
        ]:
            w.writerow([label, f"{report['scoring'][key]:,}"])
        w.writerow([])

        # ── Section 3: Tier Breakdown (today) ─────────────────────────────
        w.writerow(["TIER BREAKDOWN"])
        w.writerow(["Tier", "Count"])
        for tier, count in report["tiers"].items():
            w.writerow([tier, f"{count:,}"])
        w.writerow([])

        # ── Section 4: Lead Type Breakdown by Vertical ────────────────────
        vb = report["vertical_breakdown"]
        total_vb = vb.pop("_total", 0)
        w.writerow(["LEAD TYPE BREAKDOWN (Gold+ by Vertical)"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Leads", "% of Gold+"])
        for bucket, stats in vb.items():
            w.writerow([bucket, f"{stats['count']:,}", f"{stats['pct']:.1f}%"])
        w.writerow([])

        # ── Section 5: Alerts ─────────────────────────────────────────────
        w.writerow(["ALERTS"])
        if report["errors"]:
            for err in report["errors"]:
                w.writerow([f"WARNING: {err}"])
        else:
            w.writerow(["No errors or alerts."])


# ---------------------------------------------------------------------------
# Pruning + entry point
# ---------------------------------------------------------------------------

def prune_old_reports(directory: Path) -> int:
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted = 0
    for f in directory.glob("report_*.csv"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            logger.info(f"[daily_report] Pruned: {f.name}")
            deleted += 1
    return deleted


def generate_report(run_date: date, county_id: str) -> Path:
    logger.info(f"[daily_report] Generating for {run_date} / {county_id}")
    report = build_report(run_date, county_id)

    output_path = REPORTS_DIR / f"report_{run_date}.csv"
    write_csv(report, output_path)
    logger.info(f"[daily_report] Written to {output_path}")

    deleted = prune_old_reports(REPORTS_DIR)
    if deleted:
        logger.info(f"[daily_report] Pruned {deleted} old report(s)")

    s = report["scoring"]
    t = report["tiers"]
    vb = report["vertical_breakdown"]
    print(
        f"\nForced Action Daily Report — {run_date}\n"
        f"  Ingest  : {report['total_scraped']:,} scraped | {report['total_matched']:,} matched ({report['match_pct']:.1f}%)\n"
        f"  Leads   : {s['leads_new']:,} new | {s['leads_updated']:,} updated | {s['leads_unchanged']:,} unchanged\n"
        f"  Gold+   : Ultra Plat {t['Ultra Platinum']:,} | Plat {t['Platinum']:,} | Gold {t['Gold']:,}\n"
        f"  Verticals: " + " | ".join(f"{b} {d['count']:,}" for b, d in vb.items()) + "\n"
        f"  Alerts  : {len(report['errors'])} error(s)\n"
        f"  Saved   : {output_path}\n"
    )
    for err in report["errors"]:
        print(f"  WARNING: {err}")

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate daily operations report CSV")
    parser.add_argument("--date", type=lambda s: date.fromisoformat(s), default=date.today())
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()

    try:
        generate_report(args.date, args.county)
    except Exception as e:
        logger.error(f"[daily_report] Failed: {e}", exc_info=True)
        sys.exit(1)
