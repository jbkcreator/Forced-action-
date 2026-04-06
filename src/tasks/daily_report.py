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

from sqlalchemy import func, text

from src.core.database import get_db_context
from src.core.models import (
    BuildingPermit, CodeViolation, Deed, DistressScore, Foreclosure,
    Incident, LegalAndLien, LegalProceeding, PlatformDailyStats,
    Property, ScraperRunStats, TaxDelinquency,
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

# Display order and labels for all 6 verticals
VERTICAL_DISPLAY = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesalers",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}

GOLD_PLUS_TIERS = {"Ultra Platinum", "Platinum", "Gold"}

# Maps scraper source_type → (Model, date_field, optional filter)
# Used to compute newest record age per scraper.
_FRESHNESS_MAP = {
    "permits":          (BuildingPermit,    BuildingPermit.date_added,    None),
    "roofing_permits":  (BuildingPermit,    BuildingPermit.date_added,    None),
    "violations":       (CodeViolation,     CodeViolation.date_added,     None),
    "deeds":            (Deed,              Deed.date_added,              None),
    "foreclosures":     (Foreclosure,       Foreclosure.date_added,       None),
    "tax_delinquencies":(TaxDelinquency,    TaxDelinquency.date_added,    None),
    "insurance_claims": (Incident,          Incident.date_added,          ("incident_type", "insurance_claim")),
    "fire_incidents":   (Incident,          Incident.date_added,          ("incident_type", "Fire")),
    "storm_damage":     (Incident,          Incident.date_added,          ("incident_type", "storm_damage")),
    "flood_damage":     (Incident,          Incident.date_added,          ("incident_type", "flood_damage")),
    "probate":          (LegalProceeding,   LegalProceeding.date_added,   ("record_type", "Probate")),
    "evictions":        (LegalProceeding,   LegalProceeding.date_added,   ("record_type", "Eviction")),
    "bankruptcy":       (LegalProceeding,   LegalProceeding.date_added,   ("record_type", "Bankruptcy")),
    "judgments":        (LegalAndLien,      LegalAndLien.date_added,      ("record_type", "Judgment")),
    "lien_ml":          (LegalAndLien,      LegalAndLien.date_added,      None),
    "lien_tcl":         (LegalAndLien,      LegalAndLien.date_added,      None),
    "lien_hoa":         (LegalAndLien,      LegalAndLien.date_added,      None),
    "lien_ccl":         (LegalAndLien,      LegalAndLien.date_added,      None),
    "lien_tl":          (LegalAndLien,      LegalAndLien.date_added,      None),
}


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


def _build_signal_freshness(session, county_id: str) -> dict:
    """Return newest record age (days) per scraper source_type.

    Queries max(date_added) from the source table for each known scraper.
    Returns {source_type: days_old} — None if no records exist.
    """
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


def _build_vertical_breakdown(session, run_date: date, county_id: str) -> dict:
    """
    For today's Gold+ leads, determine primary vertical per lead
    (highest vertical_score wins) and report all 6 verticals individually.
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
        result["Other / Unclassified"] = {"count": unclassified, "pct": (unclassified / total * 100) if total else 0.0}

    result["_total"] = total
    return result


def _build_vertical_tier_crosstab(session, run_date: date, county_id: str) -> dict:
    """
    Gold+ by vertical × tier with new-today count.
    Returns {vertical_label: {tier: {count, new_today}}}.
    new_today = Gold+ today that were NOT Gold+ yesterday.
    """
    yesterday = run_date - timedelta(days=1)

    def _fetch(d):
        rows = (
            session.query(DistressScore)
            .filter(
                func.date(DistressScore.score_date) == d,
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

    today_map = _fetch(run_date)
    yesterday_ids = set(_fetch(yesterday).keys())

    # Aggregate: {vertical: {tier: {count, new_today}}}
    agg = defaultdict(lambda: defaultdict(lambda: {"count": 0, "new_today": 0}))
    for pid, (vertical, tier) in today_map.items():
        agg[vertical][tier]["count"] += 1
        if pid not in yesterday_ids:
            agg[vertical][tier]["new_today"] += 1

    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        result[label] = {}
        for tier in ("Ultra Platinum", "Platinum", "Gold"):
            result[label][tier] = agg[key].get(tier, {"count": 0, "new_today": 0})
    return result


def _build_zip_breakdown(session, run_date: date, county_id: str, top_n: int = 20) -> list:
    """
    Top ZIPs by Gold+ lead count today.
    Returns list of {zip, ultra_platinum, platinum, gold, total}.
    """
    rows = (
        session.query(
            Property.zip,
            DistressScore.lead_tier,
            func.count().label("cnt"),
        )
        .join(Property, Property.id == DistressScore.property_id)
        .filter(
            func.date(DistressScore.score_date) == run_date,
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
            "zip":           zip_code,
            "ultra_platinum": tiers["Ultra Platinum"],
            "platinum":       tiers["Platinum"],
            "gold":           tiers["Gold"],
            "total":          total,
        })

    result.sort(key=lambda x: x["total"], reverse=True)
    return result[:top_n]


def _build_signal_composition(session, run_date: date, county_id: str) -> dict:
    """
    Top signals driving Gold+ scores per vertical today.
    Uses jsonb_array_elements_text on distress_types.
    Returns {vertical_label: [(signal, count), ...]}.
    """
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
            "run_date": run_date,
            "tiers":    list(GOLD_PLUS_TIERS),
            "county_id": county_id,
        }).fetchall()
    except Exception:
        return {}

    # Map best vertical → signal counts
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


def build_report(run_date: date, county_id: str) -> dict:
    errors = []
    with get_db_context() as session:
        scraper_data, total_scraped, total_matched, match_pct, scraper_errors = \
            _build_scraper_section(session, run_date, county_id)
        errors.extend(scraper_errors)

        scoring, tiers          = _build_scoring_section(session, run_date, county_id, errors)
        vertical_breakdown      = _build_vertical_breakdown(session, run_date, county_id)
        signal_freshness        = _build_signal_freshness(session, county_id)
        vertical_tier_crosstab  = _build_vertical_tier_crosstab(session, run_date, county_id)
        zip_breakdown           = _build_zip_breakdown(session, run_date, county_id)
        signal_composition      = _build_signal_composition(session, run_date, county_id)

    return {
        "run_date":              run_date,
        "county_id":             county_id,
        "total_scraped":         total_scraped,
        "total_matched":         total_matched,
        "match_pct":             match_pct,
        "scraper_data":          scraper_data,
        "scoring":               scoring,
        "tiers":                 tiers,
        "vertical_breakdown":    vertical_breakdown,
        "signal_freshness":      signal_freshness,
        "vertical_tier_crosstab": vertical_tier_crosstab,
        "zip_breakdown":         zip_breakdown,
        "signal_composition":    signal_composition,
        "errors":                errors,
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
        w.writerow(["LEAD TYPE BREAKDOWN — Gold+ by Vertical (6 verticals)"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Leads", "% of Gold+"])
        for bucket, stats in vb.items():
            w.writerow([bucket, f"{stats['count']:,}", f"{stats['pct']:.1f}%"])
        w.writerow([])

        # ── Section 5: Gold+ by Vertical × Tier (cross-tab) ─────────────
        w.writerow(["GOLD+ BY VERTICAL × TIER"])
        w.writerow(["Vertical", "Tier", "Count", "New Today"])
        for vertical_label, tiers_data in report["vertical_tier_crosstab"].items():
            for tier in ("Ultra Platinum", "Platinum", "Gold"):
                d = tiers_data.get(tier, {"count": 0, "new_today": 0})
                if d["count"] > 0:
                    w.writerow([vertical_label, tier, d["count"], d["new_today"]])
        w.writerow([])

        # ── Section 6: ZIP-Level Gold+ Breakdown ─────────────────────────
        w.writerow(["ZIP-LEVEL GOLD+ BREAKDOWN (top 20)"])
        w.writerow(["ZIP", "Ultra Platinum", "Platinum", "Gold", "Total"])
        for z in report["zip_breakdown"]:
            w.writerow([z["zip"], z["ultra_platinum"], z["platinum"], z["gold"], z["total"]])
        w.writerow([])

        # ── Section 7: Signal Composition by Vertical ────────────────────
        w.writerow(["SIGNAL COMPOSITION — Top 5 signals driving Gold+ per vertical"])
        w.writerow(["Vertical", "Signal", "Count"])
        for vertical_label, signals in report["signal_composition"].items():
            for signal, cnt in signals:
                w.writerow([vertical_label, signal, cnt])
            if not signals:
                w.writerow([vertical_label, "No data", ""])
        w.writerow([])

        # ── Section 9: Signal Freshness ───────────────────────────────────
        w.writerow(["SIGNAL FRESHNESS (newest record age per source)"])
        w.writerow(["Source", "Newest Record Age"])
        sf = report.get("signal_freshness", {})
        for source_type in SCRAPER_ORDER:
            days = sf.get(source_type)
            label = source_type.replace("_", " ").title()
            if days is None:
                w.writerow([label, "No records"])
            else:
                w.writerow([label, f"{days} day(s) old"])
        w.writerow([])

        # ── Section 10: Alerts ────────────────────────────────────────────
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
