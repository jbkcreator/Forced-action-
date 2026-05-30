"""
Weekly operations report built directly from signal tables.

Reads date_added from each signal table instead of scraper_run_stats
(which can be inflated by enrichment tasks or have missing/zero entries).
Sunbiz and property_appraiser are excluded — they are enrichment tasks, not
signal scrapers.

Matched  = records that landed in the signal table (property was found)
Unmatched = records in unmatched_records for the same source + date window
Scraped  = Matched + Unmatched

Note: unmatched_records pools all lien subtypes under source_type='liens', so
per-subtype unmatched counts are not available — only the subtypes' matched
counts are shown individually. The aggregate Liens row shows the full
matched+unmatched picture.

Usage:
    python scripts/generate_weekly_signal_report.py
    python scripts/generate_weekly_signal_report.py --week-ending 2026-05-22
    python scripts/generate_weekly_signal_report.py --county pinellas
"""

import argparse
import csv
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import get_db_context
from sqlalchemy import text as sa_text

REPORTS_DIR = Path("reports/weekly")

VERTICAL_DISPLAY = {
    "roofing":          "Roofing",
    "restoration":      "Restoration / Remediation",
    "wholesalers":      "Wholesalers",
    "fix_flip":         "Fix & Flip",
    "public_adjusters": "Public Adjusters",
    "attorneys":        "Attorneys",
}
GOLD_PLUS_TIERS = ["Ultra Platinum", "Platinum", "Gold"]

# ---------------------------------------------------------------------------
# Signal source definitions
#
# Each entry: (label, unmatched_source_type, matched_sql, freshness_sql)
#
# matched_sql    params: :c (county_id), :s (week start), :e (week end)
# freshness_sql  params: :c only
# unmatched_source_type: the source_type value used in unmatched_records,
#   or None if unmatched records are not tracked for this source.
# ---------------------------------------------------------------------------

_SIGNAL_SOURCES = [
    # --- Legal & Liens (individual lien subtypes share 'liens' in unmatched_records)
    (
        "Judgments", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Judgment' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Judgment'",
    ),
    (
        "Mechanics Liens (ML)", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'MECHANICS%' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'MECHANICS%'",
    ),
    (
        "Tampa Code Liens (TCL)", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAMPA CODE%' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAMPA CODE%'",
    ),
    (
        "HOA Liens (HL)", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'HOA%' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'HOA%'",
    ),
    (
        "County Code Liens (CCL)", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'COUNTY CODE%' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'COUNTY CODE%'",
    ),
    (
        "IRS Tax Liens (TL)", "liens",
        "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAX LIEN%' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAX LIEN%'",
    ),
    # --- Permits
    (
        "Permits", "permits",
        "SELECT COUNT(*) FROM building_permits WHERE county_id=:c AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM building_permits WHERE county_id=:c",
    ),
    (
        "Roofing Permits", None,
        "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='roofing_permit' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM incidents WHERE county_id=:c AND incident_type='roofing_permit'",
    ),
    # --- Deeds & Violations
    (
        "Deeds", "deeds",
        "SELECT COUNT(*) FROM deeds WHERE county_id=:c AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM deeds WHERE county_id=:c",
    ),
    (
        "Violations", "violations",
        "SELECT COUNT(*) FROM code_violations WHERE county_id=:c AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM code_violations WHERE county_id=:c",
    ),
    # --- Legal Proceedings
    (
        "Probate", "probate",
        "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Probate' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_proceedings WHERE county_id=:c AND record_type='Probate'",
    ),
    (
        "Evictions", "evictions",
        "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Eviction' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_proceedings WHERE county_id=:c AND record_type='Eviction'",
    ),
    (
        "Bankruptcy", "bankruptcies",
        "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Bankruptcy' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_proceedings WHERE county_id=:c AND record_type='Bankruptcy'",
    ),
    (
        "Divorce Filings", "divorce_filings",
        "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Divorce' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM legal_proceedings WHERE county_id=:c AND record_type='Divorce'",
    ),
    # --- Foreclosures & Tax
    (
        "Foreclosures", "foreclosures",
        "SELECT COUNT(*) FROM foreclosures WHERE county_id=:c AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM foreclosures WHERE county_id=:c",
    ),
    (
        "Tax Delinquencies", "tax_delinquencies",
        "SELECT COUNT(*) FROM tax_delinquencies WHERE county_id=:c AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM tax_delinquencies WHERE county_id=:c",
    ),
    # --- Incidents
    (
        "Insurance Claims", None,
        "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='insurance_claim' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM incidents WHERE county_id=:c AND incident_type='insurance_claim'",
    ),
    (
        "Flood Damage", None,
        "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='flood_damage' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM incidents WHERE county_id=:c AND incident_type='flood_damage'",
    ),
    (
        "Storm Damage", None,
        "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='storm_damage' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM incidents WHERE county_id=:c AND incident_type='storm_damage'",
    ),
    (
        "Fire Incidents", None,
        "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='Fire' AND date_added BETWEEN :s AND :e",
        "SELECT MAX(date_added) FROM incidents WHERE county_id=:c AND incident_type='Fire'",
    ),
]

# Lien subtypes share one unmatched_records bucket.  We pull this once and
# distribute it as a single aggregate row rather than trying to split it.
_LIEN_SUBTYPE_LABELS = {
    "Judgments", "Mechanics Liens (ML)", "Tampa Code Liens (TCL)",
    "HOA Liens (HL)", "County Code Liens (CCL)", "IRS Tax Liens (TL)",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _last_friday(today: date) -> date:
    dow = today.weekday()   # Mon=0 … Sun=6
    if dow == 4:            # today is Friday
        return today - timedelta(days=7)
    return today - timedelta(days=(dow - 4) % 7)


def _week_range(week_ending: date):
    days_since_friday = (week_ending.weekday() - 4) % 7
    friday = week_ending - timedelta(days=days_since_friday)
    monday = friday - timedelta(days=4)
    return monday, friday


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _check_applicable_sources(session, county_id) -> set:
    """Return set of source labels that have at least one record for this county."""
    # Build total-count queries by stripping the date filter from each count_sql
    applicable = set()
    p = {"c": county_id}

    # Aggregate lien check — any row in legal_and_liens for this county
    lien_total = session.execute(
        sa_text("SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c"), p
    ).scalar() or 0

    # Per lien subtype
    subtype_checks = {
        "Judgments":              "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Judgment'",
        "Mechanics Liens (ML)":   "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'MECHANICS%'",
        "Tampa Code Liens (TCL)": "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAMPA CODE%'",
        "HOA Liens (HL)":         "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND (document_type ILIKE 'HOA%' OR document_type ILIKE '%(HL)%')",
        "County Code Liens (CCL)":"SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'COUNTY CODE%'",
        "IRS Tax Liens (TL)":     "SELECT COUNT(*) FROM legal_and_liens WHERE county_id=:c AND record_type='Lien' AND document_type ILIKE 'TAX LIEN%'",
    }
    for label, sql in subtype_checks.items():
        if (session.execute(sa_text(sql), p).scalar() or 0) > 0:
            applicable.add(label)

    # Non-lien sources — strip date clause from count_sql
    non_lien_checks = {
        "Permits":           "SELECT COUNT(*) FROM building_permits WHERE county_id=:c",
        "Roofing Permits":   "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='roofing_permit'",
        "Deeds":             "SELECT COUNT(*) FROM deeds WHERE county_id=:c",
        "Violations":        "SELECT COUNT(*) FROM code_violations WHERE county_id=:c",
        "Probate":           "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Probate'",
        "Evictions":         "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Eviction'",
        "Bankruptcy":        "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Bankruptcy'",
        "Divorce Filings":   "SELECT COUNT(*) FROM legal_proceedings WHERE county_id=:c AND record_type='Divorce'",
        "Foreclosures":      "SELECT COUNT(*) FROM foreclosures WHERE county_id=:c",
        "Tax Delinquencies": "SELECT COUNT(*) FROM tax_delinquencies WHERE county_id=:c",
        "Insurance Claims":  "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='insurance_claim'",
        "Flood Damage":      "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='flood_damage'",
        "Storm Damage":      "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='storm_damage'",
        "Fire Incidents":    "SELECT COUNT(*) FROM incidents WHERE county_id=:c AND incident_type='Fire'",
    }
    for label, sql in non_lien_checks.items():
        if (session.execute(sa_text(sql), p).scalar() or 0) > 0:
            applicable.add(label)

    # Always include the parent lien group if any lien subtype is applicable
    if lien_total > 0:
        applicable.add("Liens & Judgments (all subtypes)")

    return applicable


def _build_signal_ingest(session, monday, friday, county_id):
    p = {"c": county_id, "s": monday, "e": friday}

    # Pull matched counts from signal tables
    matched_counts = {}
    for label, _, count_sql, _ in _SIGNAL_SOURCES:
        matched_counts[label] = session.execute(sa_text(count_sql), p).scalar() or 0

    # Pull unmatched counts from unmatched_records (one query, group by source_type)
    unmatched_rows = session.execute(
        sa_text("""
            SELECT source_type, COUNT(*) AS cnt
            FROM unmatched_records
            WHERE county_id = :c
              AND date(date_added) BETWEEN :s AND :e
            GROUP BY source_type
        """),
        p,
    ).fetchall()
    unmatched_by_type = {r[0]: int(r[1]) for r in unmatched_rows}

    # Aggregate lien unmatched (all subtypes share 'liens' bucket)
    lien_unmatched = unmatched_by_type.get("liens", 0)
    # Lien matched = sum of all lien subtype matched counts
    lien_matched = sum(matched_counts[lbl] for lbl in _LIEN_SUBTYPE_LABELS)
    lien_scraped = lien_matched + lien_unmatched

    rows = []
    grand_scraped = grand_matched = grand_unmatched = 0

    # Aggregate Liens row first (before the individual subtypes)
    rows.append({
        "label":     "Liens & Judgments (all subtypes)",
        "matched":   lien_matched,
        "unmatched": lien_unmatched,
        "scraped":   lien_scraped,
        "is_group":  True,
    })
    grand_scraped    += lien_scraped
    grand_matched    += lien_matched
    grand_unmatched  += lien_unmatched

    # Distribute lien_unmatched proportionally across subtypes by matched weight
    # using largest-remainder rounding so parts sum exactly to lien_unmatched.
    subtype_labels_ordered = [
        lbl for lbl, _, _, _ in _SIGNAL_SOURCES if lbl in _LIEN_SUBTYPE_LABELS
    ]
    subtype_matched = [matched_counts[lbl] for lbl in subtype_labels_ordered]
    total_subtype_matched = sum(subtype_matched)

    if total_subtype_matched > 0 and lien_unmatched > 0:
        raw = [lien_unmatched * m / total_subtype_matched for m in subtype_matched]
        floors = [int(r) for r in raw]
        remainder = lien_unmatched - sum(floors)
        # Distribute remaining units to subtypes with largest fractional parts
        fracs = sorted(range(len(raw)), key=lambda i: raw[i] - floors[i], reverse=True)
        for i in range(remainder):
            floors[fracs[i]] += 1
        subtype_unmatched = floors
    else:
        subtype_unmatched = [0] * len(subtype_labels_ordered)

    for i, label in enumerate(subtype_labels_ordered):
        m = matched_counts[label]
        u = subtype_unmatched[i]
        rows.append({"label": f"  {label}", "matched": m, "unmatched": u, "scraped": m + u, "is_group": False})

    # All non-lien sources
    for label, unmatched_src, _, _ in _SIGNAL_SOURCES:
        if label in _LIEN_SUBTYPE_LABELS:
            continue
        m = matched_counts[label]
        u = unmatched_by_type.get(unmatched_src, 0) if unmatched_src else 0
        sc = m + u
        rows.append({"label": label, "matched": m, "unmatched": u, "scraped": sc, "is_group": False})
        grand_scraped   += sc
        grand_matched   += m
        grand_unmatched += u

    return rows, grand_scraped, grand_matched, grand_unmatched


def _build_freshness(session, county_id, today):
    """
    Freshness = days since the newest date_added per logical scraper.

    Lien subtypes (TCL, CCL, ML, etc.) all come from the same lien engine run,
    so showing per-subtype freshness is misleading — a 0-count week for TCL
    doesn't mean the scraper was stale, it means no TCL liens came through.
    We show one 'Liens & Judgments' freshness row using the overall max across
    the whole legal_and_liens table.
    """
    p = {"c": county_id}

    # One freshness entry for all liens/judgments combined
    lien_newest = session.execute(
        sa_text("SELECT MAX(date_added) FROM legal_and_liens WHERE county_id=:c"), p
    ).scalar()

    result = {}
    if lien_newest is None:
        result["Liens & Judgments"] = None
    else:
        nd = lien_newest.date() if hasattr(lien_newest, "date") else lien_newest
        result["Liens & Judgments"] = (today - nd).days

    # All non-lien sources — one entry per distinct scraper
    seen = set()
    for label, _, _, fresh_sql in _SIGNAL_SOURCES:
        if label in _LIEN_SUBTYPE_LABELS:
            continue
        if fresh_sql in seen:
            continue
        seen.add(fresh_sql)
        newest = session.execute(sa_text(fresh_sql), p).scalar()
        if newest is None:
            result[label] = None
        else:
            nd = newest.date() if hasattr(newest, "date") else newest
            result[label] = (today - nd).days
    return result


def _build_scoring(session, monday, friday, county_id):
    rows = session.execute(
        sa_text("""
            SELECT run_date, properties_scored, properties_with_signals,
                   leads_new, leads_updated, leads_unchanged
            FROM platform_daily_stats
            WHERE county_id = :c AND run_date BETWEEN :s AND :e
            ORDER BY run_date
        """),
        {"c": county_id, "s": monday, "e": friday},
    ).fetchall()

    if not rows:
        return {k: 0 for k in ("properties_scored", "properties_with_signals",
                                "leads_new", "leads_updated", "leads_unchanged")}, []

    last = rows[-1]
    scoring = {
        "properties_scored":       sum(r[1] or 0 for r in rows),
        "properties_with_signals": last[2] or 0,
        "leads_new":               sum(r[3] or 0 for r in rows),
        "leads_updated":           sum(r[4] or 0 for r in rows),
        "leads_unchanged":         last[5] or 0,
    }
    daily = [
        {"date": str(r[0]), "properties_scored": r[1] or 0,
         "leads_new": r[3] or 0, "leads_updated": r[4] or 0}
        for r in rows
    ]

    # Detect days where leads_new + leads_updated far exceed properties_scored —
    # a signature of a scoring config change triggering mass re-qualification,
    # not genuine new signal data.
    inflation_days = [
        d for d in daily
        if d["properties_scored"] > 0
        and (d["leads_new"] + d["leads_updated"]) > d["properties_scored"] * 3
    ]

    return scoring, daily, inflation_days


def _build_tiers(session, as_of, county_id):
    rows = session.execute(
        sa_text("""
            SELECT lead_tier, COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT ON (property_id) lead_tier
                FROM distress_scores
                WHERE county_id = :c AND date(score_date) <= :d
                ORDER BY property_id, score_date DESC
            ) latest
            WHERE lead_tier IS NOT NULL
            GROUP BY lead_tier
        """),
        {"c": county_id, "d": str(as_of)},
    ).fetchall()
    tiers = {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0, "Silver": 0, "Bronze": 0}
    for tier, cnt in rows:
        if tier in tiers:
            tiers[tier] = int(cnt)
    return tiers


def _build_gold_plus_inventory(session, as_of, county_id):
    return session.execute(
        sa_text("""
            SELECT property_id, lead_tier, vertical_scores, date(score_date) AS score_day
            FROM (
                SELECT DISTINCT ON (property_id)
                    property_id, lead_tier, vertical_scores, score_date
                FROM distress_scores
                WHERE county_id = :c AND date(score_date) <= :d
                ORDER BY property_id, score_date DESC
            ) latest
            WHERE lead_tier = ANY(:tiers)
        """),
        {"c": county_id, "d": str(as_of), "tiers": GOLD_PLUS_TIERS},
    ).fetchall()


def _build_vertical_summary(rows, monday, friday):
    vertical_counts = defaultdict(int)
    new_this_week   = defaultdict(int)
    unclassified = 0

    for pid, tier, vs, score_day in rows:
        if not vs:
            unclassified += 1
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best in VERTICAL_DISPLAY:
            vertical_counts[best] += 1
            if isinstance(score_day, str):
                from datetime import date as _d
                score_day = _d.fromisoformat(score_day)
            if score_day and monday <= score_day <= friday:
                new_this_week[best] += 1
        else:
            unclassified += 1

    total = sum(vertical_counts.values()) + unclassified
    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        cnt = vertical_counts.get(key, 0)
        pct = (cnt / total * 100) if total else 0.0
        result[label] = {"count": cnt, "pct": pct, "new_this_week": new_this_week.get(key, 0)}
    if unclassified:
        result["Other / Unclassified"] = {
            "count": unclassified, "pct": (unclassified / total * 100) if total else 0.0, "new_this_week": 0
        }
    result["_total"] = total
    return result


def _build_vertical_tier_crosstab(rows, monday, friday):
    agg = defaultdict(lambda: defaultdict(lambda: {"count": 0, "new_this_week": 0}))
    for pid, tier, vs, score_day in rows:
        if not vs or tier not in GOLD_PLUS_TIERS:
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best not in VERTICAL_DISPLAY:
            continue
        if isinstance(score_day, str):
            from datetime import date as _d
            score_day = _d.fromisoformat(score_day)
        agg[best][tier]["count"] += 1
        if score_day and monday <= score_day <= friday:
            agg[best][tier]["new_this_week"] += 1

    result = {}
    for key, label in VERTICAL_DISPLAY.items():
        result[label] = {
            tier: agg[key].get(tier, {"count": 0, "new_this_week": 0})
            for tier in GOLD_PLUS_TIERS
        }
    return result


def _build_zip_breakdown(session, as_of, county_id, top_n=10):
    rows = session.execute(
        sa_text("""
            SELECT LEFT(p.zip, 5) AS zip, latest.lead_tier, COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.lead_tier
                FROM distress_scores ds
                WHERE ds.county_id = :c AND date(ds.score_date) <= :d
                ORDER BY ds.property_id, ds.score_date DESC
            ) latest
            JOIN properties p ON p.id = latest.property_id
            WHERE latest.lead_tier = ANY(:tiers) AND p.zip IS NOT NULL
            GROUP BY LEFT(p.zip, 5), latest.lead_tier
        """),
        {"c": county_id, "d": str(as_of), "tiers": GOLD_PLUS_TIERS},
    ).fetchall()

    zip_data = defaultdict(lambda: {"Ultra Platinum": 0, "Platinum": 0, "Gold": 0})
    for z, tier, cnt in rows:
        zip_data[z][tier] = cnt

    return sorted(
        [{"zip": z, **t, "total": sum(t.values())} for z, t in zip_data.items()],
        key=lambda x: x["total"], reverse=True,
    )[:top_n]


def _build_phone_coverage(session, as_of, county_id):
    try:
        rows = session.execute(
            sa_text("""
                WITH latest AS (
                    SELECT DISTINCT ON (property_id) property_id, lead_tier
                    FROM distress_scores
                    WHERE county_id = :c AND date(score_date) <= :d
                    ORDER BY property_id, score_date DESC
                )
                SELECT l.lead_tier,
                    COUNT(*) AS total,
                    SUM(CASE WHEN o.phone_1 IS NOT NULL AND length(trim(o.phone_1)) > 0 THEN 1 ELSE 0 END) AS with_phone
                FROM latest l
                LEFT JOIN owners o ON o.property_id = l.property_id
                WHERE l.lead_tier = ANY(:tiers)
                GROUP BY l.lead_tier
            """),
            {"c": county_id, "d": str(as_of), "tiers": GOLD_PLUS_TIERS},
        ).fetchall()
    except Exception:
        return {"total_gold_plus": 0, "with_phone": 0, "without_phone": 0, "without_phone_pct": 0.0, "by_tier": {}}

    by_tier = {}
    gt = gw = 0
    for tier, total, with_phone in rows:
        total = int(total or 0)
        wp = int(with_phone or 0)
        by_tier[tier] = {"total": total, "with_phone": wp, "without_phone": total - wp}
        gt += total; gw += wp
    for t in GOLD_PLUS_TIERS:
        by_tier.setdefault(t, {"total": 0, "with_phone": 0, "without_phone": 0})
    gwo = gt - gw
    return {
        "total_gold_plus": gt, "with_phone": gw, "without_phone": gwo,
        "without_phone_pct": round(100.0 * gwo / gt, 1) if gt else 0.0,
        "by_tier": by_tier,
    }


def _build_signal_composition(session, as_of, county_id):
    try:
        rows = session.execute(
            sa_text("""
                SELECT lead_tier, vertical_scores, factor_scores
                FROM (
                    SELECT DISTINCT ON (property_id)
                        property_id, lead_tier, vertical_scores, factor_scores
                    FROM distress_scores
                    WHERE county_id = :c AND date(score_date) <= :d
                    ORDER BY property_id, score_date DESC
                ) latest
                WHERE lead_tier = ANY(:tiers) AND factor_scores IS NOT NULL
            """),
            {"c": county_id, "d": str(as_of), "tiers": GOLD_PLUS_TIERS},
        ).fetchall()
    except Exception:
        return {}

    vertical_signals = {key: Counter() for key in VERTICAL_DISPLAY}
    for _tier, vs_json, fs_json in rows:
        if not vs_json or not fs_json:
            continue
        best = max(vs_json, key=lambda k: vs_json.get(k, 0))
        if best not in VERTICAL_DISPLAY:
            continue
        primary = fs_json.get("vertical_breakdown", {}).get(best, {}).get("primary_signal")
        if primary:
            vertical_signals[best][primary] += 1
    return {label: vertical_signals[key].most_common(10) for key, label in VERTICAL_DISPLAY.items()}


def _build_week_over_week(session, monday, friday, county_id):
    prior_friday = monday - timedelta(days=3)
    prior_monday = prior_friday - timedelta(days=4)

    # One query per window: pull vertical_scores + zip together
    def _pull_week(s, e):
        return session.execute(
            sa_text("""
                SELECT ds.vertical_scores, LEFT(p.zip, 5) AS zip
                FROM distress_scores ds
                JOIN properties p ON p.id = ds.property_id
                WHERE ds.county_id = :c
                  AND date(ds.score_date) BETWEEN :s AND :e
                  AND ds.lead_tier = ANY(:tiers)
                  AND p.zip IS NOT NULL
            """),
            {"c": county_id, "s": s, "e": e, "tiers": GOLD_PLUS_TIERS},
        ).fetchall()

    def _aggregate(rows):
        by_vert = defaultdict(int)
        by_zip  = defaultdict(int)
        for vs, zip_code in rows:
            if vs:
                best = max(vs, key=lambda k: vs.get(k, 0))
                if best in VERTICAL_DISPLAY:
                    by_vert[best] += 1
            if zip_code:
                by_zip[zip_code] += 1
        return by_vert, by_zip

    this_vert, this_zip   = _aggregate(_pull_week(monday, friday))
    prior_vert, prior_zip = _aggregate(_pull_week(prior_monday, prior_friday))

    by_vertical = {}
    for key, label in VERTICAL_DISPLAY.items():
        this  = this_vert.get(key, 0)
        prior = prior_vert.get(key, 0)
        by_vertical[label] = {"this_week": this, "prior_week": prior, "delta": this - prior}

    all_zips = sorted(set(this_zip) | set(prior_zip))
    by_zip = {}
    for z in all_zips:
        this  = this_zip.get(z, 0)
        prior = prior_zip.get(z, 0)
        by_zip[z] = {"this_week": this, "prior_week": prior, "delta": this - prior}

    return by_vertical, by_zip, str(prior_monday), str(prior_friday)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _fmt(val):
    """None = unknown (show dash). 0 = known zero (show 0). Positive = formatted."""
    if val is None:
        return "—"
    if isinstance(val, int):
        return f"{val:,}"
    return str(val)


def write_csv(report: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    monday = report["monday"]
    friday = report["friday"]
    county = report["county_id"]
    today  = report["today"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        w.writerow(["Forced Action — Weekly Operations Report (Signal Tables)"])
        w.writerow([
            f"Week: {monday} to {friday}",
            f"County: {county}",
            f"Generated: {today}",
            "Source: signal tables (date_added) — not scraper_run_stats",
        ])
        w.writerow([])

        # ── Section 1: Signal Ingest ──────────────────────────────────────
        gs = report["grand_scraped"]
        gm = report["grand_matched"]
        gu = report["grand_unmatched"]
        match_pct = (gm / (gm + gu) * 100) if (gm + gu) else 0.0
        w.writerow([f"SIGNAL INGEST — {monday} to {friday} (from signal tables, sunbiz + property_appraiser excluded)"])
        w.writerow([
            f"Total: {gs:,} scraped | {gm:,} matched ({match_pct:.1f}%)"
        ])
        w.writerow(["Note: Scraped = Matched (signal table) + Unmatched (unmatched_records). "
                    "Lien subtype unmatched is proportionally split from the parent bucket by matched-count weight."])
        w.writerow([])
        applicable = report.get("applicable_sources")
        w.writerow(["Source", "Scraped", "Matched", "Unmatched", "Skipped (dupes)"])
        for row in report["signal_ingest"]:
            if applicable is not None and row["label"].strip() not in applicable:
                continue
            w.writerow([
                row["label"],
                _fmt(row["scraped"]),
                _fmt(row["matched"]),
                _fmt(row["unmatched"]),
                "—",   # not available without scraper_run_stats
            ])
        w.writerow([])

        # ── Section 2: Scoring Summary ────────────────────────────────────
        s = report["scoring"]
        inflation_days = report.get("scoring_inflation_days", [])

        w.writerow(["SCORING (properties_scored = weekly run total; tiers = full portfolio snapshot as of Friday)"])
        w.writerow([f"Total Properties Scored (this week): {s['properties_scored']:,}"])
        w.writerow([])
        w.writerow(["Metric", "Count"])
        for key, label in [
            ("properties_with_signals", "Properties w/ signals (last day of week)"),
            ("leads_new",               "Leads new this week"),
            ("leads_updated",           "Leads updated this week"),
            ("leads_unchanged",         "Leads unchanged (end of week)"),
        ]:
            w.writerow([label, f"{s[key]:,}"])
        w.writerow([])

        if inflation_days:
            dates_str = ", ".join(d["date"] for d in inflation_days)
            w.writerow([
                f"NB: 'Leads new/updated this week' is likely INFLATED due to a scoring config change "
                f"detected on {dates_str}. On those days the CDS engine registered far more "
                f"leads_new/updated than properties_scored — a mass re-qualification triggered by "
                f"a threshold or weight change, not by genuine new signal data. "
                f"Treat the tier snapshot (below) as the accurate picture of the current portfolio."
            ])
            w.writerow([])

        # ── Section 3: Tier Breakdown ─────────────────────────────────────
        w.writerow([f"TIER BREAKDOWN (full portfolio snapshot as of {friday})"])
        w.writerow(["Tier", "Count"])
        for tier, count in report["tiers"].items():
            w.writerow([tier, f"{count:,}"])
        w.writerow([])

        # ── Section 4: Gold+ Vertical Summary ────────────────────────────
        vb = dict(report["vertical_summary"])
        total_vb = vb.pop("_total", 0)
        w.writerow(["GOLD+ VERTICAL SUMMARY"])
        w.writerow([f"Total Gold+ leads: {total_vb:,}"])
        w.writerow([])
        w.writerow(["Vertical", "Total Gold+", "New This Week", "% of Gold+"])
        for key, label in VERTICAL_DISPLAY.items():
            d = vb.get(label, {"count": 0, "pct": 0.0, "new_this_week": 0})
            w.writerow([label, f"{d['count']:,}", d["new_this_week"], f"{d['pct']:.1f}%"])
        if "Other / Unclassified" in vb:
            d = vb["Other / Unclassified"]
            w.writerow(["Other / Unclassified", f"{d['count']:,}", "", f"{d['pct']:.1f}%"])
        w.writerow([])

        # ── Section 5: Gold+ by Vertical × Tier ─────────────────────────
        w.writerow(["GOLD+ BY VERTICAL x TIER"])
        w.writerow(["Vertical", "Tier", "Count", "New This Week"])
        for vertical_label, tiers_data in report["vertical_tier_crosstab"].items():
            vert_total = vert_new = 0
            for tier in GOLD_PLUS_TIERS:
                d = tiers_data.get(tier, {"count": 0, "new_this_week": 0})
                if d["count"] > 0:
                    w.writerow([vertical_label, tier, d["count"], d["new_this_week"]])
                vert_total += d["count"]
                vert_new   += d["new_this_week"]
            if vert_total > 0:
                w.writerow([vertical_label, "TOTAL", vert_total, vert_new])
        w.writerow([])

        # ── Section 6: ZIP-Level Gold+ Breakdown ─────────────────────────
        w.writerow(["ZIP-LEVEL GOLD+ BREAKDOWN (top 10)"])
        w.writerow(["ZIP", "Ultra Platinum", "Platinum", "Gold", "Total"])
        for z in report["zip_breakdown"]:
            w.writerow([z["zip"], z["Ultra Platinum"], z["Platinum"], z["Gold"], z["total"]])
        w.writerow([])

        # ── Section 7: Phone Coverage ─────────────────────────────────────
        pc = report["phone_coverage"]
        w.writerow(["PHONE COVERAGE — Gold+ leads dropped upstream for missing phone"])
        w.writerow([
            f"Total Gold+: {pc['total_gold_plus']:,} | "
            f"With phone: {pc['with_phone']:,} | "
            f"DROPPED (no phone): {pc['without_phone']:,} ({pc['without_phone_pct']:.1f}%)"
        ])
        w.writerow([])
        w.writerow(["Tier", "Total", "With Phone", "Dropped (no phone)", "Drop %"])
        for tier in GOLD_PLUS_TIERS:
            td = pc["by_tier"].get(tier, {"total": 0, "with_phone": 0, "without_phone": 0})
            drop_pct = (100.0 * td["without_phone"] / td["total"]) if td["total"] else 0.0
            w.writerow([tier, td["total"], td["with_phone"], td["without_phone"], f"{drop_pct:.1f}%"])
        w.writerow([])

        # ── Section 8: Signal Composition ────────────────────────────────
        w.writerow(["SIGNAL COMPOSITION — Top signals driving Gold+ per vertical"])
        w.writerow(["Vertical", "Signal", "Count"])
        for vertical_label, signals in report["signal_composition"].items():
            for signal, cnt in signals:
                w.writerow([vertical_label, signal, cnt])
            if not signals:
                w.writerow([vertical_label, "No data", ""])
        w.writerow([])

        # ── Section 9: Week-over-Week Gold+ ──────────────────────────────
        wow     = report["week_over_week"]
        wow_zip = report["week_over_week_zip"]
        prior_s, prior_e = report["prior_week_range"]
        w.writerow([f"GOLD+ THIS WEEK vs PRIOR WEEK ({prior_s} to {prior_e})"])
        w.writerow(["Vertical", "This Week", "Prior Week", "Delta"])
        for label, d in wow.items():
            delta_str = f"+{d['delta']}" if d["delta"] >= 0 else str(d["delta"])
            w.writerow([label, d["this_week"], d["prior_week"], delta_str])
        w.writerow([])
        w.writerow(["ZIP", "This Week", "Prior Week", "Delta"])
        for zip_code, d in wow_zip.items():
            delta_str = f"+{d['delta']}" if d["delta"] >= 0 else str(d["delta"])
            w.writerow([zip_code, d["this_week"], d["prior_week"], delta_str])
        w.writerow([])

        # ── Section 10: Signal Freshness ──────────────────────────────────
        w.writerow(["SIGNAL FRESHNESS (newest record age per scraper — 0 count for a lien subtype means"]  )
        w.writerow(["  no liens of that type came through this week, not that the lien engine was absent)"])
        w.writerow(["Source", "Newest Record Age"])
        for label, days in report["freshness"].items():
            if applicable is not None:
                # freshness key "Liens & Judgments" maps to applicable key "Liens & Judgments (all subtypes)"
                check_label = "Liens & Judgments (all subtypes)" if label == "Liens & Judgments" else label
                if check_label not in applicable:
                    continue
            w.writerow([label, "No records" if days is None else f"{days} day(s) old"])
        w.writerow([])

        # ── Section 11: Daily Scoring Breakdown ──────────────────────────
        if report.get("daily_scoring"):
            inflation_dates = {d["date"] for d in report.get("scoring_inflation_days", [])}
            w.writerow(["DAILY SCORING BREAKDOWN (from platform_daily_stats)"])
            w.writerow(["Date", "Properties Scored", "Leads New", "Leads Updated", "Note"])
            for d in report["daily_scoring"]:
                note = "** CONFIG CHANGE INFLATION — leads_new/updated >> properties_scored **" if d["date"] in inflation_dates else ""
                w.writerow([d["date"], f"{d['properties_scored']:,}", f"{d['leads_new']:,}", f"{d['leads_updated']:,}", note])
            w.writerow([])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_report(week_ending: date, county_id: str) -> dict:
    monday, friday = _week_range(week_ending)
    today = date.today()

    with get_db_context() as session:
        applicable_sources     = _check_applicable_sources(session, county_id)
        signal_ingest, grand_scraped, grand_matched, grand_unmatched = \
            _build_signal_ingest(session, monday, friday, county_id)
        scoring, daily_scoring, inflation_days = _build_scoring(session, monday, friday, county_id)
        tiers                  = _build_tiers(session, friday, county_id)
        gp_rows                = _build_gold_plus_inventory(session, friday, county_id)
        vertical_summary       = _build_vertical_summary(gp_rows, monday, friday)
        vertical_crosstab      = _build_vertical_tier_crosstab(gp_rows, monday, friday)
        zip_breakdown          = _build_zip_breakdown(session, friday, county_id)
        phone_coverage         = _build_phone_coverage(session, friday, county_id)
        signal_composition     = _build_signal_composition(session, friday, county_id)
        freshness              = _build_freshness(session, county_id, today)
        wow, wow_zip, prior_s, prior_e = _build_week_over_week(session, monday, friday, county_id)

    return {
        "monday": monday, "friday": friday, "today": today, "county_id": county_id,
        "applicable_sources": applicable_sources,
        "signal_ingest": signal_ingest,
        "grand_scraped": grand_scraped, "grand_matched": grand_matched,
        "grand_unmatched": grand_unmatched,
        "scoring": scoring, "daily_scoring": daily_scoring, "scoring_inflation_days": inflation_days,
        "tiers": tiers,
        "vertical_summary": vertical_summary,
        "vertical_tier_crosstab": vertical_crosstab,
        "zip_breakdown": zip_breakdown,
        "phone_coverage": phone_coverage,
        "signal_composition": signal_composition,
        "freshness": freshness,
        "week_over_week": wow,
        "week_over_week_zip": wow_zip,
        "prior_week_range": (prior_s, prior_e),
    }


def generate(week_ending: date, county_id: str) -> Path:
    monday, friday = _week_range(week_ending)
    print(f"[weekly_signal_report] Building {monday} to {friday} / {county_id}")
    report = build_report(week_ending, county_id)
    path = REPORTS_DIR / f"signal_report_{county_id}_week_{monday}.csv"
    write_csv(report, path)
    s = report["scoring"]
    t = report["tiers"]
    print(
        f"  Signals : {report['grand_scraped']:,} scraped | {report['grand_matched']:,} matched\n"
        f"  Leads   : {s['leads_new']:,} new | {s['leads_updated']:,} updated\n"
        f"  Gold+   : Ultra Plat {t['Ultra Platinum']:,} | Plat {t['Platinum']:,} | Gold {t['Gold']:,}\n"
        f"  Saved   : {path}"
    )
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--week-ending", type=lambda s: date.fromisoformat(s), default=None)
    parser.add_argument("--county", default="hillsborough")
    args = parser.parse_args()
    if args.week_ending is None:
        args.week_ending = _last_friday(date.today())
    try:
        generate(args.week_ending, args.county)
    except Exception:
        import traceback; traceback.print_exc(); sys.exit(1)
