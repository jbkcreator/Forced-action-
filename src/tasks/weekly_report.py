"""
Weekly operations report — CSV export.

Aggregates scraper_run_stats and platform_daily_stats across Mon–Fri of the
target week. Scoring/tier snapshot is taken from the last day of the week
that has a platform_daily_stats row. Vertical breakdown aggregates all Gold+
leads scored during the week.

Usage:
    python -m src.tasks.weekly_report                          # week ending last Friday
    python -m src.tasks.weekly_report --week-ending 2026-04-11 # specific Friday
    python -m src.tasks.weekly_report --county hillsborough

Cron (Monday at 09:00 AM UTC, reporting on the week just ended):
    0 9 * * 1 $PROJECT/scripts/cron/run.sh src.tasks.weekly_report
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
    BuildingPermit, CodeViolation, Deed, DistressScore, Foreclosure,
    Incident, LegalAndLien, LegalProceeding, PlatformDailyStats,
    ScraperRunStats, TaxDelinquency,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/weekly")
RETENTION_WEEKS = 12  # keep ~3 months of weekly reports

SCRAPER_ORDER = [
    "judgments", "permits", "deeds", "violations", "probate",
    "roofing_permits", "evictions", "lien_ml", "lien_tcl", "lien_hoa",
    "lien_ccl", "lien_tl", "foreclosures", "bankruptcy",
    "tax_delinquencies", "flood_damage", "insurance_claims",
    "storm_damage", "fire_incidents",
]

VERTICAL_DISPLAY = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesalers",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

_FRESHNESS_MAP = {
    "permits":          (BuildingPermit,   BuildingPermit.date_added,   None),
    "roofing_permits":  (BuildingPermit,   BuildingPermit.date_added,   None),
    "violations":       (CodeViolation,    CodeViolation.date_added,    None),
    "deeds":            (Deed,             Deed.date_added,             None),
    "foreclosures":     (Foreclosure,      Foreclosure.date_added,      None),
    "tax_delinquencies":(TaxDelinquency,   TaxDelinquency.date_added,   None),
    "insurance_claims": (Incident,         Incident.date_added,         ("incident_type", "insurance_claim")),
    "fire_incidents":   (Incident,         Incident.date_added,         ("incident_type", "Fire")),
    "storm_damage":     (Incident,         Incident.date_added,         ("incident_type", "storm_damage")),
    "flood_damage":     (Incident,         Incident.date_added,         ("incident_type", "flood_damage")),
    "probate":          (LegalProceeding,  LegalProceeding.date_added,  ("record_type", "Probate")),
    "evictions":        (LegalProceeding,  LegalProceeding.date_added,  ("record_type", "Eviction")),
    "bankruptcy":       (LegalProceeding,  LegalProceeding.date_added,  ("record_type", "Bankruptcy")),
    "judgments":        (LegalAndLien,     LegalAndLien.date_added,     ("record_type", "Judgment")),
    "lien_ml":          (LegalAndLien,     LegalAndLien.date_added,     None),
    "lien_tcl":         (LegalAndLien,     LegalAndLien.date_added,     None),
    "lien_hoa":         (LegalAndLien,     LegalAndLien.date_added,     None),
    "lien_ccl":         (LegalAndLien,     LegalAndLien.date_added,     None),
    "lien_tl":          (LegalAndLien,     LegalAndLien.date_added,     None),
}


def _week_range(week_ending: date):
    """Return (monday, friday) for the week ending on week_ending."""
    # Snap to the Friday on or before week_ending
    days_since_friday = (week_ending.weekday() - 4) % 7
    friday = week_ending - timedelta(days=days_since_friday)
    monday = friday - timedelta(days=4)
    return monday, friday


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _build_scraper_section(session, monday: date, friday: date, county_id: str):
    rows = (
        session.query(ScraperRunStats)
        .filter(
            ScraperRunStats.run_date >= monday,
            ScraperRunStats.run_date <= friday,
            ScraperRunStats.county_id == county_id,
        )
        .all()
    )

    # Aggregate per source_type across the week
    agg = defaultdict(lambda: {"scraped": 0, "matched": 0, "unmatched": 0,
                                "runs": 0, "failures": 0, "errors": []})
    for r in rows:
        a = agg[r.source_type]
        a["scraped"]   += r.total_scraped
        a["matched"]   += r.matched
        a["unmatched"] += r.unmatched
        a["runs"]      += 1
        if not r.run_success:
            a["failures"] += 1
            if r.error_message:
                a["errors"].append(f"{r.run_date}: {r.error_message}")

    ordered = [t for t in SCRAPER_ORDER if t in agg]
    extras  = sorted(t for t in agg if t not in SCRAPER_ORDER)

    scraper_data = []
    total_scraped = total_matched = 0
    week_errors = []

    for source_type in ordered + extras:
        a = agg[source_type]
        total_scraped += a["scraped"]
        total_matched += a["matched"]
        scraper_data.append({
            "label":    source_type.replace("_", " ").title(),
            "scraped":  a["scraped"],
            "matched":  a["matched"]   if a["scraped"] > 0 else None,
            "unmatched":a["unmatched"] if a["scraped"] > 0 else None,
            "failures": a["failures"],
            "runs":     a["runs"],
        })
        if a["failures"]:
            week_errors.extend(a["errors"] or [f"{source_type}: {a['failures']} failed run(s)"])

    # Zero rows for scrapers with no runs this week
    for source_type in SCRAPER_ORDER:
        if source_type not in agg:
            scraper_data.append({
                "label": source_type.replace("_", " ").title(),
                "scraped": 0, "matched": None, "unmatched": None,
                "failures": 0, "runs": 0,
            })

    match_pct = (total_matched / total_scraped * 100) if total_scraped else 0.0
    return scraper_data, total_scraped, total_matched, match_pct, week_errors


def _build_scoring_section(session, monday: date, friday: date, county_id: str, errors: list):
    """Sum leads_new/updated over the week; use last available day for snapshot."""
    platform_rows = (
        session.query(PlatformDailyStats)
        .filter(
            PlatformDailyStats.run_date >= monday,
            PlatformDailyStats.run_date <= friday,
            PlatformDailyStats.county_id == county_id,
        )
        .order_by(PlatformDailyStats.run_date)
        .all()
    )

    if not platform_rows:
        errors.append("platform_daily_stats: no rows found for this week")
        return (
            {k: 0 for k in ("properties_scored", "properties_with_signals",
                             "leads_new", "leads_updated", "leads_unchanged")},
            {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0, "Silver": 0, "Bronze": 0},
        )

    last = platform_rows[-1]  # end-of-week snapshot for scored/tiers
    scoring = {
        "properties_scored":       last.properties_scored,
        "properties_with_signals": last.properties_with_signals,
        "leads_new":      sum(r.leads_new     for r in platform_rows),
        "leads_updated":  sum(r.leads_updated for r in platform_rows),
        "leads_unchanged":last.leads_unchanged,
    }
    tiers = {
        "Ultra Platinum": last.tier_ultra_platinum,
        "Platinum":       last.tier_platinum,
        "Gold":           last.tier_gold,
        "Silver":         last.tier_silver,
        "Bronze":         last.tier_bronze,
    }
    return scoring, tiers


def _build_daily_scraper_totals(session, monday: date, friday: date, county_id: str) -> list:
    """Total scraped per day across all scrapers — for the day-by-day table."""
    rows = (
        session.query(
            ScraperRunStats.run_date,
            func.sum(ScraperRunStats.total_scraped).label("scraped"),
            func.sum(ScraperRunStats.matched).label("matched"),
        )
        .filter(
            ScraperRunStats.run_date >= monday,
            ScraperRunStats.run_date <= friday,
            ScraperRunStats.county_id == county_id,
        )
        .group_by(ScraperRunStats.run_date)
        .order_by(ScraperRunStats.run_date)
        .all()
    )
    result = []
    for r in rows:
        pct = (r.matched / r.scraped * 100) if r.scraped else 0.0
        result.append({"date": str(r.run_date), "scraped": r.scraped,
                        "matched": r.matched, "pct": pct})
    return result


def _build_vertical_breakdown(session, monday: date, friday: date, county_id: str) -> dict:
    """Aggregate Gold+ vertical breakdown across the whole week."""
    rows = (
        session.query(DistressScore)
        .filter(
            func.date(DistressScore.score_date) >= monday,
            func.date(DistressScore.score_date) <= friday,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .all()
    )

    vertical_counts = defaultdict(int)
    unclassified = 0

    for r in rows:
        vs = r.vertical_scores or {}
        if not vs:
            unclassified += 1
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best in VERTICAL_DISPLAY:
            vertical_counts[best] += 1
        else:
            unclassified += 1

    total = sum(vertical_counts.values()) + unclassified
    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        cnt = vertical_counts.get(key, 0)
        pct = (cnt / total * 100) if total else 0.0
        result[label] = {"count": cnt, "pct": pct}
    if unclassified:
        result["Other / Unclassified"] = {
            "count": unclassified,
            "pct": (unclassified / total * 100) if total else 0.0,
        }
    result["_total"] = total
    return result


def _build_signal_freshness(session, county_id: str) -> dict:
    today = date.today()
    freshness = {}
    for source_type, (model, date_col, filter_pair) in _FRESHNESS_MAP.items():
        try:
            q = session.query(func.max(date_col)).filter(
                getattr(model, "county_id", None) == county_id
                if hasattr(model, "county_id") else True
            )
            if filter_pair:
                field, value = filter_pair
                q = q.filter(getattr(model, field) == value)
            newest = q.scalar()
            if newest is None:
                freshness[source_type] = None
            else:
                if hasattr(newest, "date"):
                    newest = newest.date()
                freshness[source_type] = (today - newest).days
        except Exception:
            freshness[source_type] = None
    return freshness


def build_report(week_ending: date, county_id: str) -> dict:
    monday, friday = _week_range(week_ending)
    errors = []
    with get_db_context() as session:
        scraper_data, total_scraped, total_matched, match_pct, scraper_errors = \
            _build_scraper_section(session, monday, friday, county_id)
        errors.extend(scraper_errors)

        scoring, tiers      = _build_scoring_section(session, monday, friday, county_id, errors)
        daily_totals        = _build_daily_scraper_totals(session, monday, friday, county_id)
        vertical_breakdown  = _build_vertical_breakdown(session, monday, friday, county_id)
        signal_freshness    = _build_signal_freshness(session, county_id)

    return {
        "week_start":         monday,
        "week_end":           friday,
        "county_id":          county_id,
        "total_scraped":      total_scraped,
        "total_matched":      total_matched,
        "match_pct":          match_pct,
        "scraper_data":       scraper_data,
        "daily_totals":       daily_totals,
        "scoring":            scoring,
        "tiers":              tiers,
        "vertical_breakdown": vertical_breakdown,
        "signal_freshness":   signal_freshness,
        "errors":             errors,
    }


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Weekly Operations Report"])
        w.writerow([
            f"Week: {report['week_start']} to {report['week_end']}",
            f"County: {report['county_id']}",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        ])
        w.writerow([])

        # ── Section 1: Scraper Ingest (weekly totals) ─────────────────────
        w.writerow(["SCRAPER INGEST (WEEKLY TOTALS)"])
        w.writerow([
            f"Total: {report['total_scraped']:,} scraped | "
            f"{report['total_matched']:,} matched ({report['match_pct']:.1f}%)"
        ])
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

        # ── Section 2: Day-by-Day Ingest ──────────────────────────────────
        w.writerow(["DAILY BREAKDOWN"])
        w.writerow(["Date", "Scraped", "Matched", "Match %"])
        for d in report["daily_totals"]:
            w.writerow([d["date"], f"{d['scraped']:,}", f"{d['matched']:,}", f"{d['pct']:.1f}%"])
        w.writerow([])

        # ── Section 3: Scoring Summary ────────────────────────────────────
        w.writerow(["SCORING"])
        w.writerow([f"Total Properties Scored (end of week): {report['scoring']['properties_scored']:,}"])
        w.writerow([])
        w.writerow(["Metric", "Count"])
        for key, label in [
            ("properties_with_signals", "Properties w/ signals"),
            ("leads_new",               "Leads new this week"),
            ("leads_updated",           "Leads updated this week"),
            ("leads_unchanged",         "Leads unchanged (end of week)"),
        ]:
            w.writerow([label, f"{report['scoring'][key]:,}"])
        w.writerow([])

        # ── Section 4: Tier Breakdown (end of week snapshot) ──────────────
        w.writerow(["TIER BREAKDOWN (end of week)"])
        w.writerow(["Tier", "Count"])
        for tier, count in report["tiers"].items():
            w.writerow([tier, f"{count:,}"])
        w.writerow([])

        # ── Section 5: Lead Type Breakdown by Vertical ────────────────────
        vb = report["vertical_breakdown"]
        total_vb = vb.pop("_total", 0)
        w.writerow(["LEAD TYPE BREAKDOWN — Gold+ by Vertical (weekly, 6 verticals)"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Leads", "% of Gold+"])
        for bucket, stats in vb.items():
            w.writerow([bucket, f"{stats['count']:,}", f"{stats['pct']:.1f}%"])
        w.writerow([])

        # ── Section 6: Signal Freshness ───────────────────────────────────
        w.writerow(["SIGNAL FRESHNESS (newest record age per source)"])
        w.writerow(["Source", "Newest Record Age"])
        sf = report.get("signal_freshness", {})
        for source_type in SCRAPER_ORDER:
            days = sf.get(source_type)
            label = source_type.replace("_", " ").title()
            w.writerow([label, "No records" if days is None else f"{days} day(s) old"])
        w.writerow([])

        # ── Section 7: Alerts ─────────────────────────────────────────────
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
    cutoff = datetime.now() - timedelta(weeks=RETENTION_WEEKS)
    deleted = 0
    for f in directory.glob("report_week_*.csv"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            logger.info(f"[weekly_report] Pruned: {f.name}")
            deleted += 1
    return deleted


def generate_report(week_ending: date, county_id: str) -> Path:
    monday, friday = _week_range(week_ending)
    logger.info(f"[weekly_report] Generating for {monday} – {friday} / {county_id}")

    report = build_report(week_ending, county_id)
    output_path = REPORTS_DIR / f"report_week_{monday}.csv"
    write_csv(report, output_path)
    logger.info(f"[weekly_report] Written to {output_path}")

    deleted = prune_old_reports(REPORTS_DIR)
    if deleted:
        logger.info(f"[weekly_report] Pruned {deleted} old report(s)")

    # Email report to stakeholders
    try:
        from src.tasks.report_emailer import send_weekly_report
        send_weekly_report(report)
    except Exception as exc:
        logger.error(f"[weekly_report] Failed to send email report: {exc}")

    s = report["scoring"]
    t = report["tiers"]
    print(
        f"\nForced Action Weekly Report — {monday} to {friday}\n"
        f"  Ingest  : {report['total_scraped']:,} scraped | {report['total_matched']:,} matched ({report['match_pct']:.1f}%)\n"
        f"  Leads   : {s['leads_new']:,} new | {s['leads_updated']:,} updated\n"
        f"  Gold+   : Ultra Plat {t['Ultra Platinum']:,} | Plat {t['Platinum']:,} | Gold {t['Gold']:,}\n"
        f"  Alerts  : {len(report['errors'])} error(s)\n"
        f"  Saved   : {output_path}\n"
    )
    for err in report["errors"]:
        print(f"  WARNING: {err}")

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate weekly operations report CSV")
    parser.add_argument(
        "--week-ending",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Date of Friday (week end). Defaults to last Friday.",
    )
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()

    if args.week_ending is None:
        today = date.today()
        days_since_friday = (today.weekday() - 4) % 7
        args.week_ending = today - timedelta(days=days_since_friday)

    try:
        generate_report(args.week_ending, args.county)
    except Exception as e:
        logger.error(f"[weekly_report] Failed: {e}", exc_info=True)
        sys.exit(1)
