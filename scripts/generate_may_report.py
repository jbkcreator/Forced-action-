"""
One-shot May 2026 monthly report generator.

Sources ALL data from signal tables + unmatched_records + distress_scores.
Does NOT use scraper_run_stats or platform_daily_stats (stale).

Ingest rows:
  - matched  = records in signal tables with date_added in May 2026
  - unmatched = unmatched_records with date(date_added) in May 2026,
                match_status IN ('unmatched', 'pending_review')
  - Liens + Judgments are ONE row ("Legal & Liens") — same table
  - No "Skipped (dupes)" column

Output:
  reports/monthly/report_2026-05_hillsborough.csv
  reports/monthly/report_2026-05_pinellas.csv

Run from repo root:
  python scripts/generate_may_report.py
"""

import csv
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text as sa_text

from src.core.database import get_db_context

MAY_START = date(2026, 5, 1)
MAY_END   = date(2026, 5, 31)

GOLD_PLUS_TIERS = ["Ultra Platinum", "Platinum", "Gold"]

VERTICAL_DISPLAY = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesalers",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}

# Unmatched source_types that roll up into each display row.
# Values must match actual source_type strings in unmatched_records —
# confirmed from DB: liens uses 'liens' (not individual subtypes),
# bankruptcy uses 'bankruptcies' (plural).
UNMATCHED_SOURCE_MAP = {
    "Legal & Liens":     ["judgments", "liens", "lis_pendens"],
    "Building Permits":  ["permits", "roofing_permits"],
    "Deeds":             ["deeds"],
    "Violations":        ["violations"],
    "Probate":           ["probate"],
    "Evictions":         ["evictions"],
    "Foreclosures":      ["foreclosures"],
    "Bankruptcy":        ["bankruptcies"],
    "Divorce Filings":   ["divorce_filings"],
    "Tax Delinquencies": ["tax_delinquencies"],
    "Flood Damage":      ["flood_damage"],
    "Insurance Claims":  ["insurance_claims"],
    "Storm Damage":      ["storm_damage"],
    "Fire Incidents":    ["fire_incidents"],
}


# ---------------------------------------------------------------------------
# Ingest section
# ---------------------------------------------------------------------------

def _build_ingest_section(session, county_id: str) -> list:
    params = {"county_id": county_id, "start": MAY_START, "end": MAY_END}

    # Matched counts straight from signal tables (date_added is a Date column)
    matched_rows = session.execute(sa_text("""
        SELECT 'Legal & Liens'     AS label, COUNT(*) AS cnt FROM legal_and_liens
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Building Permits', COUNT(*) FROM building_permits
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Deeds',            COUNT(*) FROM deeds
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Violations',       COUNT(*) FROM code_violations
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Probate',          COUNT(*) FROM legal_proceedings
         WHERE county_id = :county_id AND record_type = 'Probate'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Evictions',        COUNT(*) FROM legal_proceedings
         WHERE county_id = :county_id AND record_type = 'Eviction'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Foreclosures',     COUNT(*) FROM foreclosures
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Bankruptcy',       COUNT(*) FROM legal_proceedings
         WHERE county_id = :county_id AND record_type = 'Bankruptcy'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Divorce Filings',  COUNT(*) FROM legal_proceedings
         WHERE county_id = :county_id AND record_type = 'Divorce'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Tax Delinquencies', COUNT(*) FROM tax_delinquencies
         WHERE county_id = :county_id AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Flood Damage',     COUNT(*) FROM incidents
         WHERE county_id = :county_id AND incident_type = 'flood_damage'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Insurance Claims', COUNT(*) FROM incidents
         WHERE county_id = :county_id AND incident_type = 'insurance_claim'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Storm Damage',     COUNT(*) FROM incidents
         WHERE county_id = :county_id AND incident_type = 'storm_damage'
           AND date_added >= :start AND date_added <= :end
        UNION ALL
        SELECT 'Fire Incidents',   COUNT(*) FROM incidents
         WHERE county_id = :county_id AND incident_type = 'Fire'
           AND date_added >= :start AND date_added <= :end
    """), params).fetchall()

    matched_by_label = {label: int(cnt or 0) for label, cnt in matched_rows}

    # Unmatched counts from unmatched_records (date_added is timestamptz — cast needed)
    # Exclude 'skipped' and already-matched rows; keep 'unmatched' + 'pending_review'
    unmatched_rows = session.execute(sa_text("""
        SELECT source_type, COUNT(*) AS cnt
          FROM unmatched_records
         WHERE county_id = :county_id
           AND date(date_added AT TIME ZONE 'UTC') >= :start
           AND date(date_added AT TIME ZONE 'UTC') <= :end
           AND match_status IN ('unmatched', 'pending_review')
         GROUP BY source_type
    """), params).fetchall()

    raw_unmatched = {st: int(cnt or 0) for st, cnt in unmatched_rows}

    rows = []
    for label, source_types in UNMATCHED_SOURCE_MAP.items():
        matched   = matched_by_label.get(label, 0)
        unmatched = sum(raw_unmatched.get(st, 0) for st in source_types)
        scraped   = matched + unmatched
        rows.append({
            "label":     label,
            "scraped":   scraped,
            "matched":   matched   if scraped > 0 else None,
            "unmatched": unmatched if scraped > 0 else None,
        })

    return rows


# ---------------------------------------------------------------------------
# Scoring section — distress_scores only
# ---------------------------------------------------------------------------

def _build_scoring_section(session, county_id: str) -> dict:
    row = session.execute(sa_text("""
        SELECT
            COUNT(DISTINCT property_id)                                           AS total_scored,
            COUNT(DISTINCT CASE
                WHEN date(score_date) >= :may_start AND date(score_date) <= :may_end
                THEN property_id END)                                             AS scored_in_may,
            COUNT(DISTINCT CASE
                WHEN vertical_scores IS NOT NULL
                THEN property_id END)                                             AS with_signals
          FROM distress_scores
         WHERE county_id = :county_id
           AND date(score_date) <= :may_end
    """), {"county_id": county_id, "may_start": MAY_START, "may_end": MAY_END}).fetchone()

    if row:
        return {
            "total_scored":  int(row[0] or 0),
            "scored_in_may": int(row[1] or 0),
            "with_signals":  int(row[2] or 0),
        }
    return {"total_scored": 0, "scored_in_may": 0, "with_signals": 0}


def _build_tier_snapshot(session, county_id: str) -> dict:
    rows = session.execute(sa_text("""
        SELECT lead_tier, COUNT(*) AS cnt
          FROM (
              SELECT DISTINCT ON (property_id) lead_tier
                FROM distress_scores
               WHERE county_id = :county_id
                 AND date(score_date) <= :may_end
               ORDER BY property_id, score_date DESC
          ) latest
         WHERE lead_tier IS NOT NULL
         GROUP BY lead_tier
    """), {"county_id": county_id, "may_end": MAY_END}).fetchall()

    tiers = {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0, "Silver": 0, "Bronze": 0}
    for tier, cnt in rows:
        if tier in tiers:
            tiers[tier] = int(cnt)
    return tiers


# ---------------------------------------------------------------------------
# Gold+ sections
# ---------------------------------------------------------------------------

def _gold_plus_inventory(session, county_id: str):
    return session.execute(sa_text("""
        SELECT property_id, lead_tier, vertical_scores, factor_scores,
               date(score_date) AS score_day
          FROM (
              SELECT DISTINCT ON (property_id)
                     property_id, lead_tier, vertical_scores, factor_scores, score_date
                FROM distress_scores
               WHERE county_id = :county_id
                 AND date(score_date) <= :may_end
               ORDER BY property_id, score_date DESC
          ) latest
         WHERE lead_tier = ANY(:tiers)
    """), {"county_id": county_id, "may_end": MAY_END, "tiers": GOLD_PLUS_TIERS}).fetchall()


def _build_vertical_breakdown(gp_rows):
    vertical_counts = defaultdict(int)
    unclassified = 0
    for _pid, _tier, vs, _fs, _sd in gp_rows:
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
        result[label] = {"count": cnt, "pct": (cnt / total * 100) if total else 0.0}
    if unclassified:
        result["Other / Unclassified"] = {
            "count": unclassified,
            "pct":   (unclassified / total * 100) if total else 0.0,
        }
    result["_total"] = total
    return result


def _build_vertical_tier_crosstab(gp_rows):
    agg = defaultdict(lambda: defaultdict(lambda: {"count": 0, "new_in_may": 0}))
    for _pid, tier, vs, _fs, score_day in gp_rows:
        if not vs:
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best not in VERTICAL_DISPLAY:
            continue
        if isinstance(score_day, str):
            score_day = date.fromisoformat(score_day)
        agg[best][tier]["count"] += 1
        if score_day and MAY_START <= score_day <= MAY_END:
            agg[best][tier]["new_in_may"] += 1

    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        result[label] = {}
        for tier in ("Ultra Platinum", "Platinum", "Gold"):
            result[label][tier] = agg[key].get(tier, {"count": 0, "new_in_may": 0})
    return result


def _build_zip_breakdown(session, county_id: str, top_n: int = 10) -> list:
    rows = session.execute(sa_text("""
        SELECT LEFT(p.zip, 5) AS zip, latest.lead_tier, COUNT(*) AS cnt
          FROM (
              SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.lead_tier
                FROM distress_scores ds
               WHERE ds.county_id = :county_id
                 AND date(ds.score_date) <= :may_end
               ORDER BY ds.property_id, ds.score_date DESC
          ) latest
          JOIN properties p ON p.id = latest.property_id
         WHERE latest.lead_tier = ANY(:tiers)
           AND p.zip IS NOT NULL
         GROUP BY LEFT(p.zip, 5), latest.lead_tier
    """), {"county_id": county_id, "may_end": MAY_END, "tiers": GOLD_PLUS_TIERS}).fetchall()

    zip_data = defaultdict(lambda: {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0})
    for zip_code, tier, cnt in rows:
        zip_data[zip_code][tier] = int(cnt)

    result = [
        {
            "zip":            z,
            "ultra_platinum": d["Ultra Platinum"],
            "platinum":       d["Platinum"],
            "gold":           d["Gold"],
            "total":          sum(d.values()),
        }
        for z, d in zip_data.items()
    ]
    result.sort(key=lambda x: x["total"], reverse=True)
    return result[:top_n]


def _build_phone_coverage(session, county_id: str) -> dict:
    rows = session.execute(sa_text("""
        WITH latest AS (
            SELECT DISTINCT ON (property_id) property_id, lead_tier
              FROM distress_scores
             WHERE county_id = :county_id
               AND date(score_date) <= :may_end
             ORDER BY property_id, score_date DESC
        )
        SELECT
            l.lead_tier,
            COUNT(*) AS total,
            SUM(CASE WHEN o.phone_1 IS NOT NULL AND length(trim(o.phone_1)) > 0
                     THEN 1 ELSE 0 END) AS with_phone
          FROM latest l
          LEFT JOIN owners o ON o.property_id = l.property_id
         WHERE l.lead_tier = ANY(:tiers)
         GROUP BY l.lead_tier
    """), {"county_id": county_id, "may_end": MAY_END, "tiers": GOLD_PLUS_TIERS}).fetchall()

    by_tier = {}
    grand_total = grand_with = 0
    for tier, total, with_phone in rows:
        total = int(total or 0)
        wp    = int(with_phone or 0)
        by_tier[tier] = {"total": total, "with_phone": wp, "without_phone": total - wp}
        grand_total += total
        grand_with  += wp

    for tier in ("Ultra Platinum", "Platinum", "Gold"):
        by_tier.setdefault(tier, {"total": 0, "with_phone": 0, "without_phone": 0})

    grand_without = grand_total - grand_with
    pct = (100.0 * grand_without / grand_total) if grand_total else 0.0
    return {
        "total_gold_plus":   grand_total,
        "with_phone":        grand_with,
        "without_phone":     grand_without,
        "without_phone_pct": round(pct, 1),
        "by_tier":           by_tier,
    }


def _build_signal_composition(gp_rows) -> dict:
    vertical_signals = {key: Counter() for key in VERTICAL_DISPLAY}
    for _pid, _tier, vs, fs, _sd in gp_rows:
        if not vs or not fs:
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best not in VERTICAL_DISPLAY:
            continue
        primary = (fs.get("vertical_breakdown") or {}).get(best, {}).get("primary_signal")
        if primary:
            vertical_signals[best][primary] += 1

    return {
        label: vertical_signals[key].most_common(10)
        for key, label in VERTICAL_DISPLAY.items()
    }


def _build_signal_freshness(session, county_id: str) -> dict:
    checks = [
        ("Legal & Liens",     "SELECT MAX(date_added) FROM legal_and_liens   WHERE county_id = :county_id AND date_added <= :end"),
        ("Building Permits",  "SELECT MAX(date_added) FROM building_permits  WHERE county_id = :county_id AND date_added <= :end"),
        ("Deeds",             "SELECT MAX(date_added) FROM deeds              WHERE county_id = :county_id AND date_added <= :end"),
        ("Violations",        "SELECT MAX(date_added) FROM code_violations    WHERE county_id = :county_id AND date_added <= :end"),
        ("Probate",           "SELECT MAX(date_added) FROM legal_proceedings  WHERE county_id = :county_id AND record_type = 'Probate'    AND date_added <= :end"),
        ("Evictions",         "SELECT MAX(date_added) FROM legal_proceedings  WHERE county_id = :county_id AND record_type = 'Eviction'   AND date_added <= :end"),
        ("Foreclosures",      "SELECT MAX(date_added) FROM foreclosures       WHERE county_id = :county_id AND date_added <= :end"),
        ("Bankruptcy",        "SELECT MAX(date_added) FROM legal_proceedings  WHERE county_id = :county_id AND record_type = 'Bankruptcy' AND date_added <= :end"),
        ("Divorce Filings",   "SELECT MAX(date_added) FROM legal_proceedings  WHERE county_id = :county_id AND record_type = 'Divorce'    AND date_added <= :end"),
        ("Tax Delinquencies", "SELECT MAX(date_added) FROM tax_delinquencies  WHERE county_id = :county_id AND date_added <= :end"),
        ("Flood Damage",      "SELECT MAX(date_added) FROM incidents          WHERE county_id = :county_id AND incident_type = 'flood_damage'     AND date_added <= :end"),
        ("Insurance Claims",  "SELECT MAX(date_added) FROM incidents          WHERE county_id = :county_id AND incident_type = 'insurance_claim'   AND date_added <= :end"),
        ("Storm Damage",      "SELECT MAX(date_added) FROM incidents          WHERE county_id = :county_id AND incident_type = 'storm_damage'      AND date_added <= :end"),
        ("Fire Incidents",    "SELECT MAX(date_added) FROM incidents          WHERE county_id = :county_id AND incident_type = 'Fire'              AND date_added <= :end"),
    ]

    freshness = {}
    for label, sql in checks:
        try:
            newest = session.execute(sa_text(sql), {"county_id": county_id, "end": MAY_END}).scalar()
            if newest is None:
                freshness[label] = None
            else:
                if hasattr(newest, "date"):
                    newest = newest.date()
                freshness[label] = (MAY_END - newest).days
        except Exception as exc:
            print(f"  [WARN] freshness check failed for {label}: {exc}")
            freshness[label] = None

    return freshness


# ---------------------------------------------------------------------------
# Orchestrate + write CSV
# ---------------------------------------------------------------------------

def build_report(county_id: str) -> dict:
    with get_db_context() as session:
        ingest     = _build_ingest_section(session, county_id)
        scoring    = _build_scoring_section(session, county_id)
        tiers      = _build_tier_snapshot(session, county_id)
        gp_rows    = _gold_plus_inventory(session, county_id)
        vert_bd    = _build_vertical_breakdown(gp_rows)
        vert_cross = _build_vertical_tier_crosstab(gp_rows)
        zip_bd     = _build_zip_breakdown(session, county_id)
        phone_cov  = _build_phone_coverage(session, county_id)
        sig_comp   = _build_signal_composition(gp_rows)
        sig_fresh  = _build_signal_freshness(session, county_id)

    total_matched   = sum(r["matched"]   or 0 for r in ingest)
    total_unmatched = sum(r["unmatched"] or 0 for r in ingest)
    total_scraped   = total_matched + total_unmatched
    match_pct       = (total_matched / total_scraped * 100) if total_scraped else 0.0

    return {
        "county_id":               county_id,
        "ingest":                  ingest,
        "total_scraped":           total_scraped,
        "total_matched":           total_matched,
        "total_unmatched":         total_unmatched,
        "match_pct":               match_pct,
        "scoring":                 scoring,
        "tiers":                   tiers,
        "vertical_breakdown":      vert_bd,
        "vertical_tier_crosstab":  vert_cross,
        "zip_breakdown":           zip_bd,
        "phone_coverage":          phone_cov,
        "signal_composition":      sig_comp,
        "signal_freshness":        sig_fresh,
    }


def write_csv(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    county_id = report["county_id"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — May 2026 Monthly Operations Report"])
        w.writerow([f"Period: May 2026", f"County: {county_id}",
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
        w.writerow([])

        # ── Section 1: SCRAPER INGEST ─────────────────────────────────────
        w.writerow(["SCRAPER INGEST — May 2026"])
        w.writerow([f"Total: {report['total_scraped']:,} scraped | "
                    f"{report['total_matched']:,} matched ({report['match_pct']:.1f}%)"])
        w.writerow([])
        w.writerow(["Scraper", "Scraped", "Matched", "Unmatched"])
        for row in report["ingest"]:
            w.writerow([
                row["label"],
                row["scraped"],
                row["matched"]   if row["matched"]   is not None else "—",
                row["unmatched"] if row["unmatched"] is not None else "—",
            ])
        w.writerow([])

        # ── Section 2: SCORING ────────────────────────────────────────────
        sc = report["scoring"]
        w.writerow(["SCORING (portfolio state as of 2026-05-31 — full rescore)"])
        w.writerow([f"Total Properties Scored: {sc['total_scored']:,}"])
        w.writerow([])
        w.writerow(["Metric", "Count"])
        w.writerow(["Properties w/ signals",          f"{sc['with_signals']:,}"])
        w.writerow(["Properties scored/updated in May", f"{sc['scored_in_may']:,}"])
        w.writerow([])

        # ── Section 3: TIER BREAKDOWN ─────────────────────────────────────
        w.writerow(["TIER BREAKDOWN (full portfolio snapshot as of 2026-05-31)"])
        w.writerow(["Tier", "Count"])
        for tier, count in report["tiers"].items():
            w.writerow([tier, f"{count:,}"])
        w.writerow([])

        # ── Section 4: GOLD+ VERTICAL SUMMARY ────────────────────────────
        vb = report["vertical_breakdown"]
        total_vb = vb.pop("_total", 0)
        crosstab = report["vertical_tier_crosstab"]
        w.writerow(["GOLD+ VERTICAL SUMMARY"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Total Gold+", "New in May", "% of Gold+"])
        for key, label in VERTICAL_DISPLAY.items():
            stats   = vb.get(label, {"count": 0, "pct": 0.0})
            tiers_d = crosstab.get(label, {})
            new_may = sum(tiers_d.get(t, {}).get("new_in_may", 0)
                          for t in ("Ultra Platinum", "Platinum", "Gold"))
            w.writerow([label, f"{stats['count']:,}", new_may, f"{stats['pct']:.1f}%"])
        if "Other / Unclassified" in vb:
            s = vb["Other / Unclassified"]
            w.writerow(["Other / Unclassified", f"{s['count']:,}", "", f"{s['pct']:.1f}%"])
        w.writerow([])

        # ── Section 5: GOLD+ BY VERTICAL × TIER ──────────────────────────
        w.writerow(["GOLD+ BY VERTICAL × TIER"])
        w.writerow(["Vertical", "Tier", "Count", "New in May"])
        for vertical_label, tiers_data in report["vertical_tier_crosstab"].items():
            vert_total = vert_new = 0
            for tier in ("Ultra Platinum", "Platinum", "Gold"):
                d = tiers_data.get(tier, {"count": 0, "new_in_may": 0})
                if d["count"] > 0:
                    w.writerow([vertical_label, tier, d["count"], d["new_in_may"]])
                vert_total += d["count"]
                vert_new   += d["new_in_may"]
            if vert_total > 0:
                w.writerow([vertical_label, "TOTAL", vert_total, vert_new])
        w.writerow([])

        # ── Section 6: ZIP-LEVEL GOLD+ BREAKDOWN ─────────────────────────
        w.writerow(["ZIP-LEVEL GOLD+ BREAKDOWN (top 10)"])
        w.writerow(["ZIP", "Ultra Platinum", "Platinum", "Gold", "Total"])
        for z in report["zip_breakdown"]:
            w.writerow([z["zip"], z["ultra_platinum"], z["platinum"], z["gold"], z["total"]])
        w.writerow([])

        # ── Section 6b: PHONE COVERAGE ────────────────────────────────────
        pc = report.get("phone_coverage") or {}
        w.writerow(["PHONE COVERAGE — Gold+ leads dropped upstream for missing phone"])
        w.writerow([
            f"Total Gold+: {pc.get('total_gold_plus', 0):,} | "
            f"With phone: {pc.get('with_phone', 0):,} | "
            f"DROPPED (no phone): {pc.get('without_phone', 0):,} "
            f"({pc.get('without_phone_pct', 0.0):.1f}%)"
        ])
        w.writerow([])
        w.writerow(["Tier", "Total", "With Phone", "Dropped (no phone)", "Drop %"])
        for tier in ("Ultra Platinum", "Platinum", "Gold"):
            td      = (pc.get("by_tier") or {}).get(tier, {"total": 0, "with_phone": 0, "without_phone": 0})
            total   = td["total"]
            wp      = td["with_phone"]
            dropped = td["without_phone"]
            drop_pct = (100.0 * dropped / total) if total else 0.0
            w.writerow([tier, total, wp, dropped, f"{drop_pct:.1f}%"])
        w.writerow([])

        # ── Section 7: SIGNAL COMPOSITION ────────────────────────────────
        w.writerow(["SIGNAL COMPOSITION — Top 10 signals driving Gold+ per vertical"])
        w.writerow(["Vertical", "Signal", "Count"])
        for vertical_label, signals in report["signal_composition"].items():
            for signal, cnt in signals:
                w.writerow([vertical_label, signal, cnt])
            if not signals:
                w.writerow([vertical_label, "No data", ""])
        w.writerow([])

        # ── Section 8: SIGNAL FRESHNESS ───────────────────────────────────
        w.writerow(["SIGNAL FRESHNESS (newest record age relative to 2026-05-31)"])
        w.writerow(["Source", "Newest Record Age"])
        for label, days in report["signal_freshness"].items():
            if days is None:
                w.writerow([label, "No records"])
            else:
                w.writerow([label, f"{days} day(s) old as of May 31"])
        w.writerow([])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    reports_dir = Path("reports/monthly")
    counties    = ["hillsborough", "pinellas"]

    for county_id in counties:
        print(f"\n{'='*60}")
        print(f"Building {county_id.title()} report...")

        try:
            report = build_report(county_id)
        except Exception as exc:
            print(f"  [ERROR] {exc}")
            import traceback; traceback.print_exc()
            continue

        path = reports_dir / f"report_2026-05_{county_id}.csv"
        write_csv(report, path)

        sc = report["scoring"]
        t  = report["tiers"]
        print(
            f"  Ingest : {report['total_scraped']:,} scraped | "
            f"{report['total_matched']:,} matched ({report['match_pct']:.1f}%)\n"
            f"  Scored : {sc['total_scored']:,} properties total | "
            f"{sc['with_signals']:,} w/ signals | {sc['scored_in_may']:,} touched in May\n"
            f"  Tiers  : "
            + " | ".join(f"{tier} {cnt:,}" for tier, cnt in t.items())
            + f"\n  Gold+  : {sum(cnt for tier, cnt in t.items() if tier in ('Ultra Platinum','Platinum','Gold')):,}\n"
            f"  Saved  : {path}"
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
