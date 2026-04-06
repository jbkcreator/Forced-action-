"""
Weekly operations report — CSV export.

Aggregates the last 7 days as a single range rather than 7 separate days.
Scraper ingest is summed across the full window. Scoring reflects current
state with a "new this week" delta (Gold+ today vs Gold+ 7 days ago).

Usage:
    python scripts/weekly_report.py                  # last 7 days ending today
    python scripts/weekly_report.py --end 2026-04-06 # last 7 days ending on date
    python scripts/weekly_report.py --county hillsborough

Output: reports/weekly/weekly_YYYY-MM-DD.csv  (end date in filename)
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, text

from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit, CodeViolation, Deed, DistressScore, Foreclosure,
    Incident, LegalAndLien, LegalProceeding, PlatformDailyStats, Property,
    ScraperRunStats, TaxDelinquency,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports/weekly")
WINDOW_DAYS = 7

SCRAPER_ORDER = [
    "judgments", "permits", "deeds", "violations", "probate",
    "roofing_permits", "evictions", "lien_ml", "lien_tcl", "lien_hoa",
    "lien_ccl", "lien_tl", "foreclosures", "bankruptcy", "lis_pendens",
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


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _build_scraper_section(session, start: date, end: date, county_id: str):
    """Sum scraper stats across the full 7-day window per source type."""
    rows = (
        session.query(ScraperRunStats)
        .filter(
            ScraperRunStats.run_date >= start,
            ScraperRunStats.run_date <= end,
            ScraperRunStats.county_id == county_id,
        )
        .all()
    )

    # Aggregate per source_type across all days
    agg = defaultdict(lambda: {"scraped": 0, "matched": 0, "unmatched": 0})
    for r in rows:
        k = r.source_type
        agg[k]["scraped"]   += r.total_scraped
        agg[k]["matched"]   += r.matched
        agg[k]["unmatched"] += r.unmatched

    total_scraped = total_matched = 0
    scraper_data = []
    ordered = [t for t in SCRAPER_ORDER if t in agg]
    extras = sorted(t for t in agg if t not in SCRAPER_ORDER)

    for source_type in ordered + extras:
        d = agg[source_type]
        total_scraped += d["scraped"]
        total_matched += d["matched"]
        scraper_data.append({
            "label":    source_type.replace("_", " ").title(),
            "scraped":  d["scraped"],
            "matched":  d["matched"]   if d["scraped"] > 0 else None,
            "unmatched":d["unmatched"] if d["scraped"] > 0 else None,
        })

    # Add zero rows for scrapers with no runs this week
    for source_type in SCRAPER_ORDER:
        if source_type not in agg:
            scraper_data.append({
                "label": source_type.replace("_", " ").title(),
                "scraped": 0, "matched": None, "unmatched": None,
                "runs": 0, "failures": 0,
            })

    match_pct = (total_matched / total_scraped * 100) if total_scraped else 0.0
    return scraper_data, total_scraped, total_matched, match_pct


def _fetch_gold_plus(session, on_date: date, county_id: str) -> dict:
    """
    Return {property_id: (best_vertical, lead_tier)} for Gold+ on or before on_date.
    Uses the most recent available score_date <= on_date to handle weekends/gaps.
    """
    latest = session.query(func.max(func.date(DistressScore.score_date))).filter(
        func.date(DistressScore.score_date) <= on_date,
        DistressScore.county_id == county_id,
    ).scalar()

    if not latest:
        return {}

    rows = (
        session.query(DistressScore)
        .filter(
            func.date(DistressScore.score_date) == latest,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .all()
    )
    result = {}
    for r in rows:
        vs = r.vertical_scores or {}
        if not vs:
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best in VERTICAL_DISPLAY:
            result[r.property_id] = (best, r.lead_tier)
    return result


def _build_scoring_section(session, end: date, start: date, county_id: str):
    """Current Gold+ state + new-this-week delta + weekly aggregate from platform_daily_stats."""
    today_map    = _fetch_gold_plus(session, end, county_id)
    # Baseline: most recent available score before the window started
    week_ago_ids = set(_fetch_gold_plus(session, start - timedelta(days=1), county_id).keys())

    tiers = defaultdict(int)
    new_this_week = 0
    for pid, (_, tier) in today_map.items():
        tiers[tier] += 1
        if pid not in week_ago_ids:
            new_this_week += 1

    total_gold_plus = sum(tiers.values())

    # Weekly aggregate from platform_daily_stats
    platform_rows = (
        session.query(PlatformDailyStats)
        .filter(
            PlatformDailyStats.run_date >= start,
            PlatformDailyStats.run_date <= end,
            PlatformDailyStats.county_id == county_id,
        )
        .order_by(PlatformDailyStats.run_date.desc())
        .all()
    )
    days_with_data          = len(platform_rows)
    leads_new_week          = sum(r.leads_new              for r in platform_rows)
    leads_updated_week      = sum(r.leads_updated          for r in platform_rows)
    leads_unchanged_week    = sum(r.leads_unchanged        for r in platform_rows)
    # properties_scored is cumulative (same 523K every day) — use latest day's value
    latest                  = platform_rows[0] if platform_rows else None
    properties_scored       = latest.properties_scored       if latest else 0
    properties_with_signals = latest.properties_with_signals if latest else 0

    return {
        "total_gold_plus":         total_gold_plus,
        "new_this_week":           new_this_week,
        "ultra_platinum":          tiers.get("Ultra Platinum", 0),
        "platinum":                tiers.get("Platinum", 0),
        "gold":                    tiers.get("Gold", 0),
        "leads_new_week":          leads_new_week,
        "leads_updated_week":      leads_updated_week,
        "leads_unchanged_week":    leads_unchanged_week,
        "properties_scored":       properties_scored,
        "properties_with_signals": properties_with_signals,
        "days_with_data":          days_with_data,
    }


def _build_vertical_tier_crosstab(session, end: date, start: date, county_id: str) -> dict:
    """Gold+ by vertical × tier with new-this-week count."""
    today_map   = _fetch_gold_plus(session, end, county_id)
    week_ago_ids = set(_fetch_gold_plus(session, start, county_id).keys())

    agg = defaultdict(lambda: defaultdict(lambda: {"count": 0, "new_this_week": 0}))
    for pid, (vertical, tier) in today_map.items():
        agg[vertical][tier]["count"] += 1
        if pid not in week_ago_ids:
            agg[vertical][tier]["new_this_week"] += 1

    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        result[label] = {}
        for tier in ("Ultra Platinum", "Platinum", "Gold"):
            result[label][tier] = agg[key].get(tier, {"count": 0, "new_this_week": 0})
    return result


def _build_zip_breakdown(session, end: date, county_id: str, top_n: int = 20) -> list:
    """Top ZIPs by Gold+ count as of end date."""
    rows = (
        session.query(
            Property.zip,
            DistressScore.lead_tier,
            func.count().label("cnt"),
        )
        .join(Property, Property.id == DistressScore.property_id)
        .filter(
            func.date(DistressScore.score_date) == end,
            DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
            Property.zip.isnot(None),
        )
        .group_by(Property.zip, DistressScore.lead_tier)
        .all()
    )

    zip_data = defaultdict(lambda: {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0})
    for zip_code, tier, cnt in rows:
        zip_data[zip_code][tier] = cnt

    result = []
    for zip_code, tiers in zip_data.items():
        total = sum(tiers.values())
        result.append({
            "zip":            zip_code,
            "ultra_platinum": tiers["Ultra Platinum"],
            "platinum":       tiers["Platinum"],
            "gold":           tiers["Gold"],
            "total":          total,
        })

    result.sort(key=lambda x: x["total"], reverse=True)
    return result[:top_n]


def _build_signal_composition(session, end: date, county_id: str) -> dict:
    """Top signals driving Gold+ scores per vertical as of end date."""
    sql = text("""
        SELECT
            ds.vertical_scores,
            jsonb_array_elements_text(ds.distress_types) AS signal,
            COUNT(*) AS cnt
        FROM distress_scores ds
        WHERE
            DATE(ds.score_date) = :run_date
            AND ds.lead_tier = ANY(:tiers)
            AND ds.county_id = :county_id
            AND ds.distress_types IS NOT NULL
            AND ds.distress_types != 'null'::jsonb
        GROUP BY ds.vertical_scores, signal
    """)

    try:
        rows = session.execute(sql, {
            "run_date":  end,
            "tiers":     list(GOLD_PLUS_TIERS),
            "county_id": county_id,
        }).fetchall()
    except Exception:
        return {}

    vertical_signals = defaultdict(lambda: defaultdict(int))
    for vs_json, signal, cnt in rows:
        if not vs_json:
            continue
        best = max(vs_json, key=lambda k: vs_json.get(k, 0))
        if best in VERTICAL_DISPLAY:
            vertical_signals[best][signal] += cnt

    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        signals = vertical_signals.get(key, {})
        top = sorted(signals.items(), key=lambda x: x[1], reverse=True)[:5]
        result[label] = top
    return result


def _build_signal_freshness(session, county_id: str) -> dict:
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
    today = date.today()
    freshness = {}
    for source_type, (model, date_col, filter_pair) in _FRESHNESS_MAP.items():
        try:
            q = session.query(func.max(date_col))
            if hasattr(model, "county_id"):
                q = q.filter(model.county_id == county_id)
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


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Weekly Operations Report"])
        w.writerow(["Period", f"{report['start']} to {report['end']}"])
        w.writerow(["County", report['county_id']])
        w.writerow(["Generated", datetime.now().strftime('%Y-%m-%d %H:%M')])
        w.writerow([])

        # ── Section 1: Scraper Ingest (7-day aggregate) ───────────────────
        w.writerow(["SCRAPER INGEST — 7-DAY AGGREGATE"])
        w.writerow(["Total Scraped", report['total_scraped']])
        w.writerow(["Total Matched", report['total_matched']])
        w.writerow(["Match Rate", f"{report['match_pct']:.1f}%"])
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
        sc = report["scoring"]
        w.writerow(["SCORING SUMMARY"])
        w.writerow(["Scoring days in period", sc['days_with_data']])
        w.writerow([])
        w.writerow(["Weekly Aggregate"])
        w.writerow(["Leads new", sc['leads_new_week']])
        w.writerow(["Leads updated", sc['leads_updated_week']])
        w.writerow(["Leads unchanged", sc['leads_unchanged_week']])
        w.writerow([])
        w.writerow(["Current State — as of end date"])
        w.writerow(["Total properties scored", sc['properties_scored']])
        w.writerow(["Properties with signals", sc['properties_with_signals']])
        w.writerow(["Total Gold+", sc['total_gold_plus']])
        w.writerow(["New this week", sc['new_this_week']])
        w.writerow(["Ultra Platinum", sc['ultra_platinum']])
        w.writerow(["Platinum", sc['platinum']])
        w.writerow(["Gold", sc['gold']])
        w.writerow([])

        # ── Section 3: Gold+ by Vertical × Tier ──────────────────────────
        w.writerow(["GOLD+ BY VERTICAL x TIER"])
        w.writerow(["Vertical", "Tier", "Count", "New This Week"])
        for vertical_label, tiers_data in report["vertical_tier_crosstab"].items():
            for tier in ("Ultra Platinum", "Platinum", "Gold"):
                d = tiers_data.get(tier, {"count": 0, "new_this_week": 0})
                if d["count"] > 0:
                    w.writerow([vertical_label, tier, d["count"], d["new_this_week"]])
        w.writerow([])

        # ── Section 4: ZIP-Level Gold+ Breakdown ─────────────────────────
        w.writerow(["ZIP-LEVEL GOLD+ BREAKDOWN — Top 20 as of end date"])
        w.writerow(["ZIP", "Ultra Platinum", "Platinum", "Gold", "Total"])
        for z in report["zip_breakdown"]:
            w.writerow([z["zip"], z["ultra_platinum"], z["platinum"], z["gold"], z["total"]])
        w.writerow([])

        # ── Section 5: Signal Composition by Vertical ────────────────────
        w.writerow(["SIGNAL COMPOSITION — Top 5 signals per vertical"])
        w.writerow(["Vertical", "Signal", "Count"])
        for vertical_label, signals in report["signal_composition"].items():
            if signals:
                for signal, cnt in signals:
                    w.writerow([vertical_label, signal, cnt])
            else:
                w.writerow([vertical_label, "No data", 0])
        w.writerow([])

        # ── Section 6: Signal Freshness ───────────────────────────────────
        w.writerow(["SIGNAL FRESHNESS — Newest record age per source"])
        w.writerow(["Source", "Newest Record Age"])
        sf = report.get("signal_freshness", {})
        for source_type in SCRAPER_ORDER:
            days = sf.get(source_type)
            label = source_type.replace("_", " ").title()
            if days is None:
                w.writerow([label, "No records"])
            else:
                w.writerow([label, f"{days} days old"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_weekly_report(end: date, county_id: str) -> Path:
    start = end - timedelta(days=WINDOW_DAYS - 1)
    logger.info("[weekly_report] Generating %s → %s / %s", start, end, county_id)

    with get_db_context() as session:
        scraper_data, total_scraped, total_matched, match_pct = \
            _build_scraper_section(session, start, end, county_id)
        scoring              = _build_scoring_section(session, end, start, county_id)
        vertical_tier_crosstab = _build_vertical_tier_crosstab(session, end, start, county_id)
        zip_breakdown        = _build_zip_breakdown(session, end, county_id)
        signal_composition   = _build_signal_composition(session, end, county_id)
        signal_freshness     = _build_signal_freshness(session, county_id)

    report = {
        "start":                 start,
        "end":                   end,
        "county_id":             county_id,
        "total_scraped":         total_scraped,
        "total_matched":         total_matched,
        "match_pct":             match_pct,
        "scraper_data":          scraper_data,
        "scoring":               scoring,
        "vertical_tier_crosstab": vertical_tier_crosstab,
        "zip_breakdown":         zip_breakdown,
        "signal_composition":    signal_composition,
        "signal_freshness":      signal_freshness,
    }

    output_path = REPORTS_DIR / f"weekly_{end}.csv"
    write_csv(report, output_path)
    logger.info("[weekly_report] Written to %s", output_path)

    sc = scoring
    print(
        f"\nForced Action Weekly Report — {start} → {end}\n"
        f"  Ingest  : {total_scraped:,} scraped | {total_matched:,} matched ({match_pct:.1f}%) over 7 days\n"
        f"  Gold+   : {sc['total_gold_plus']:,} total | {sc['new_this_week']:,} new this week\n"
        f"  Tiers   : Ultra Plat {sc['ultra_platinum']:,} | Plat {sc['platinum']:,} | Gold {sc['gold']:,}\n"
        f"  Saved   : {output_path}\n"
    )
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate weekly operations report CSV")
    parser.add_argument("--end",    type=lambda s: date.fromisoformat(s), default=date.today())
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()

    try:
        generate_weekly_report(args.end, args.county)
    except Exception as e:
        logger.error("[weekly_report] Failed: %s", e, exc_info=True)
        sys.exit(1)
