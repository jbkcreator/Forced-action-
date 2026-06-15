"""
Daily operations dashboard PDF generator.

Unified across all active counties (DB-driven via counties.is_active).
Produces a 10-section executive PDF. Sections 1/3/5/6/7/8/9/10 show combined
totals; Section 2 shows per-county tiles; Section 4 shows per-county scraper rows.

Usage:
    python -m src.tasks.daily_dashboard                  # today, all active counties
    python -m src.tasks.daily_dashboard --date 2026-05-28

Output: reports/daily_dashboard/YYYY-MM-DD_forced_action_daily_dashboard.pdf
Retention: 30 days.
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.email import send_email
from src.tasks.daily_report import (
    ENRICHMENT_ONLY,
    GOLD_PLUS_TIERS,
    SCRAPER_ORDER,
    VERTICAL_DISPLAY,
    _build_phone_coverage,
    _build_signal_composition,
    _build_signal_freshness,
    _build_tier_history,
    _build_vertical_breakdown,
    _build_vertical_tier_crosstab,
    _build_zip_breakdown,
    _query_tier_snapshot,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

DASHBOARD_DIR = Path("reports/daily_dashboard")
RETENTION_DAYS = 30
TEMPLATES_DIR = Path("src/templates")

NA = "N/A"

# All five lead tiers in display order. GOLD_PLUS_TIERS covers the top three;
# this adds Silver/Bronze for tables that report the full distribution.
ALL_TIERS = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]

# Source types surfaced in the Data Quality / freshness tables in addition to
# daily_report.SCRAPER_ORDER. divorce_filings is a valid scraper_run_stats
# source_type but is not in the shared SCRAPER_ORDER list.
DASHBOARD_EXTRA_SOURCES = ["divorce_filings"]

# Default future-county pipeline (not yet active). Expansion managed via
# counties.is_active; these surface as queued rows in Section 2.
DEFAULT_FUTURE_COUNTIES = ["pasco", "polk", "manatee", "sarasota"]

# Lien and judgment subtypes that are common to every county.
LIEN_SUBTYPES: frozenset[str] = frozenset({
    "judgments", "lis_pendens",
    "lien_ml", "lien_tcl", "lien_hoa", "lien_ccl", "lien_tl", "lien_unknown",
})
LIEN_SUBTYPE_ORDER = [
    "judgments", "lis_pendens",
    "lien_ml", "lien_tcl", "lien_hoa", "lien_ccl", "lien_tl", "lien_unknown",
]
LIEN_LABELS: frozenset[str] = frozenset(st.replace("_", " ").title() for st in LIEN_SUBTYPES)

SOURCE_LABELS = {
    "batch_skip_tracing": "BatchData (BST)",
    "idi": "IDI Fallback",
    "pdl": "PDL",
}

# enriched_contacts.source -> enrichment_usage_logs.vendor, for real per-lead cost.
# Only vendors that log to enrichment_usage_logs (currently BatchData) yield a cost;
# others fall back to N/A rather than fabricating a number.
SOURCE_TO_VENDOR = {
    "batch_skip_tracing": "batchdata",
}

SIGNAL_TO_VERTICALS = {
    "roofing_permits":   ["roofing"],
    "insurance_claims":  ["roofing", "restoration", "public_adjusters"],
    "storm_damage":      ["roofing", "restoration", "public_adjusters"],
    "flood_damage":      ["roofing", "restoration", "public_adjusters"],
    "fire_incidents":    ["restoration", "public_adjusters"],
    "violations":        ["roofing", "restoration", "wholesalers", "fix_flip", "attorneys"],
    "permits":           ["roofing", "restoration"],
    "foreclosures":      ["wholesalers", "fix_flip", "attorneys"],
    "deeds":             ["wholesalers", "fix_flip"],
    "judgments":         ["attorneys", "wholesalers"],
    "lien_ml":           ["attorneys", "wholesalers"],
    "lien_tcl":          ["attorneys"],
    "lien_hoa":          ["attorneys", "wholesalers"],
    "lien_ccl":          ["attorneys"],
    "lien_tl":           ["attorneys"],
    "bankruptcy":        ["wholesalers", "fix_flip", "attorneys"],
    "probate":           ["wholesalers", "fix_flip", "attorneys"],
    "evictions":         ["wholesalers", "fix_flip", "attorneys"],
    "tax_delinquencies": ["wholesalers", "fix_flip", "attorneys"],
}


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _pct(n: Any, d: Any) -> str:
    try:
        return f"{100 * int(n) / int(d):.1f}%" if d else NA
    except (TypeError, ZeroDivisionError, ValueError):
        return NA


def _currency(v: Any) -> str:
    try:
        return f"${float(v):,.0f}" if v is not None else NA
    except (TypeError, ValueError):
        return NA


def _score_fmt(v: Any) -> str:
    try:
        return f"{float(v):.1f}" if v is not None else NA
    except (TypeError, ValueError):
        return NA


def _num_fmt(v: Any) -> str:
    try:
        return f"{int(v or 0):,}"
    except (TypeError, ValueError):
        return NA


def _delta_fmt(v: Any) -> str:
    try:
        fv = float(v)
        return f"+{fv:.1f}%" if fv > 0 else f"{fv:.1f}%"
    except (TypeError, ValueError):
        return NA


def _badge(val: Any) -> str:
    if val is None or val == NA:
        return "na"
    if val is True or val == "ok":
        return "ok"
    if val is False or val == "error":
        return "fail"
    if val == "warn":
        return "warn"
    if val == "caution":
        return "caution"
    return str(val)


def _staleness_badge(days: Any) -> str:
    if days is None:
        return "fail"
    if days <= 1:
        return "ok"
    if days <= 3:
        return "caution"
    return "warn"


def _quality_label(health: str) -> str:
    return {
        "ok": "🟢 Clean",
        "caution": "🟡 Moderate",
        "warn": "🟠 Stale",
        "fail": "🔴 Error",
        "na": "⚪ N/A",
    }.get(health, "—")


def _status_emoji(status: str) -> str:
    if status in ("ok", "healthy", "on_track", "exceeding"):
        return "✅"
    if status in ("warn", "caution", "early", "lagging"):
        return "⚠️"
    if status in ("fail", "prelaunch", "pending", "off_track"):
        return "⏳"
    return "—"


def _trend_arrow(val: str) -> str:
    return {
        "up": "↑",
        "down": "↓",
        "flat": "→",
    }.get(val, "→")


# ---------------------------------------------------------------------------
# County discovery
# ---------------------------------------------------------------------------

def _fetch_active_counties(session) -> list[str]:
    try:
        rows = session.execute(
            text("SELECT county_id FROM counties WHERE is_active = true ORDER BY county_id")
        ).fetchall()
        return [r[0] for r in rows] if rows else ["hillsborough"]
    except Exception as exc:
        logger.warning("_fetch_active_counties failed: %s", exc)
        return ["hillsborough"]


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------

def _merge_tier_snapshots(snapshots: list[dict]) -> dict:
    merged: dict = {}
    for snap in snapshots:
        for tier, cnt in snap.items():
            merged[tier] = merged.get(tier, 0) + int(cnt or 0)
    return merged


def _merge_tier_histories(histories: list[list]) -> list:
    by_date: dict = {}
    for hist in histories:
        for row in hist:
            d = row["date"]
            if d not in by_date:
                by_date[d] = dict(row)
            else:
                for k in ("ultra_platinum", "platinum", "gold", "total"):
                    by_date[d][k] = by_date[d].get(k, 0) + int(row.get(k, 0) or 0)
    return sorted(by_date.values(), key=lambda r: r["date"])


def _merge_vertical_breakdowns(breakdowns: list[dict]) -> dict:
    merged: dict = {}
    for bd in breakdowns:
        for vert, data in bd.items():
            if not isinstance(data, dict):
                continue
            if vert not in merged:
                merged[vert] = {"count": 0, "pct": 0.0}
            merged[vert]["count"] += int(data.get("count", 0) or 0)
    total = sum(v["count"] for v in merged.values())
    for v in merged.values():
        v["pct"] = (v["count"] / total * 100) if total else 0.0
    merged["_total"] = total
    return merged


def _merge_vertical_crosstabs(crosstabs: list[dict]) -> dict:
    merged: dict = {}
    for ct in crosstabs:
        for vert, tier_data in ct.items():
            if vert not in merged:
                merged[vert] = {}
            for tier, data in tier_data.items():
                if tier not in merged[vert]:
                    merged[vert][tier] = {"count": 0, "new_today": 0}
                merged[vert][tier]["count"] += int(data.get("count", 0) or 0)
                merged[vert][tier]["new_today"] += int(data.get("new_today", 0) or 0)
    return merged


def _merge_phone_coverages(coverages: list[dict]) -> dict:
    merged = {"total_gold_plus": 0, "with_phone": 0, "without_phone": 0}
    for pc in coverages:
        for k in ("total_gold_plus", "with_phone", "without_phone"):
            merged[k] += int(pc.get(k, 0) or 0)
    t = merged["total_gold_plus"]
    merged["with_phone_pct"] = round(merged["with_phone"] / t * 100, 1) if t else 0
    merged["without_phone_pct"] = round(merged["without_phone"] / t * 100, 1) if t else 0
    return merged


def _merge_signal_compositions(compositions: list[dict]) -> dict:
    merged: dict = {}
    for comp in compositions:
        for label, signals in comp.items():
            if label not in merged:
                merged[label] = {}
            for sig, cnt in (signals or []):
                merged[label][sig] = merged[label].get(sig, 0) + int(cnt or 0)
    return {
        label: sorted(sig_dict.items(), key=lambda x: -x[1])
        for label, sig_dict in merged.items()
    }


def _merge_signal_freshness_dicts(freshnesses: list[dict]) -> dict:
    merged: dict = {}
    for fresh in freshnesses:
        for src, days in fresh.items():
            if src not in merged:
                merged[src] = days
            elif days is None:
                merged[src] = None
            elif merged[src] is not None and days > merged[src]:
                merged[src] = days
    return merged


# ---------------------------------------------------------------------------
# Query functions — unified (county_ids: list[str])
# ---------------------------------------------------------------------------

def _fetch_tier_counts(session, run_date: date, county_ids: list[str]) -> dict:
    try:
        rows = session.execute(
            text("""
                SELECT lead_tier, COUNT(*) AS cnt
                FROM distress_scores
                WHERE date(score_date) = :today
                  AND county_id = ANY(:county_ids)
                GROUP BY lead_tier
            """),
            {"today": str(run_date), "county_ids": county_ids},
        ).fetchall()
        counts = {t: 0 for t in ("Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze")}
        for tier, cnt in rows:
            if tier in counts:
                counts[tier] = int(cnt)
        total = sum(counts.values())
        gold_plus = sum(counts[t] for t in GOLD_PLUS_TIERS)
        return {"counts": counts, "total": total, "gold_plus": gold_plus,
                "gold_plus_pct": _pct(gold_plus, total)}
    except Exception as exc:
        logger.warning("_fetch_tier_counts failed: %s", exc)
        return {"counts": {}, "total": 0, "gold_plus": 0, "gold_plus_pct": NA, "error": True}


def _fetch_weekly_tier_history(session, run_date: date, county_ids: list[str]) -> list:
    week_start = run_date - timedelta(days=run_date.weekday())
    try:
        rows = session.execute(
            text("""
                SELECT date(score_date) AS day, lead_tier, COUNT(*) AS cnt
                FROM distress_scores
                WHERE date(score_date) BETWEEN :week_start AND :today
                  AND county_id = ANY(:county_ids)
                GROUP BY 1, 2 ORDER BY 1
            """),
            {"week_start": str(week_start), "today": str(run_date), "county_ids": county_ids},
        ).fetchall()
        by_day: dict = defaultdict(
            lambda: {t: 0 for t in ("Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze")}
        )
        for day, tier, cnt in rows:
            by_day[str(day)][tier] = int(cnt)
        result = []
        d = week_start
        day_names = []
        while d <= run_date:
            key = str(d)
            counts = by_day.get(key, {t: 0 for t in ("Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze")})
            gp = sum(counts.get(t, 0) for t in GOLD_PLUS_TIERS)
            result.append({"date": key, "day_name": d.strftime("%a"), "counts": counts, "gold_plus": gp})
            day_names.append(d.strftime("%a"))
            d += timedelta(days=1)
        return result
    except Exception as exc:
        logger.warning("_fetch_weekly_tier_history failed: %s", exc)
        return []


def _fetch_tier_7day_avg(session, run_date: date, county_ids: list[str]) -> dict:
    """Return 7-day daily average count per tier."""
    seven_start = run_date - timedelta(days=6)
    try:
        rows = session.execute(
            text("""
                SELECT lead_tier, COUNT(*)::float / 7.0 AS avg_per_day
                FROM distress_scores
                WHERE date(score_date) BETWEEN :seven_start AND :today
                  AND county_id = ANY(:county_ids)
                GROUP BY lead_tier
            """),
            {"seven_start": str(seven_start), "today": str(run_date), "county_ids": county_ids},
        ).fetchall()
        return {tier: round(float(avg), 1) for tier, avg in rows}
    except Exception as exc:
        logger.warning("_fetch_tier_7day_avg failed: %s", exc)
        return {}


def _fetch_tier_this_week_total(session, run_date: date, county_ids: list[str]) -> dict:
    """Return this week's total count per tier."""
    week_start = run_date - timedelta(days=run_date.weekday())
    try:
        rows = session.execute(
            text("""
                SELECT lead_tier, COUNT(*) AS cnt
                FROM distress_scores
                WHERE date(score_date) BETWEEN :week_start AND :today
                  AND county_id = ANY(:county_ids)
                GROUP BY lead_tier
            """),
            {"week_start": str(week_start), "today": str(run_date), "county_ids": county_ids},
        ).fetchall()
        return {tier: int(cnt) for tier, cnt in rows}
    except Exception as exc:
        logger.warning("_fetch_tier_this_week_total failed: %s", exc)
        return {}


def _fetch_avg_cds(session, run_date: date, county_ids: list[str]) -> str:
    try:
        val = session.execute(
            text("""
                SELECT AVG(final_cds_score) FROM distress_scores
                WHERE date(score_date) = :today AND county_id = ANY(:county_ids)
                  AND final_cds_score IS NOT NULL
            """),
            {"today": str(run_date), "county_ids": county_ids},
        ).scalar()
        return _score_fmt(val)
    except Exception as exc:
        logger.warning("_fetch_avg_cds failed: %s", exc)
        return NA


def _fetch_active_subscriber_count(session, county_ids: list[str]) -> int:
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE status = 'active' AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar()
        return int(row or 0)
    except Exception as exc:
        logger.warning("_fetch_active_subscriber_count failed: %s", exc)
        return 0


def _trend(current: float | None, previous: float | None, *, lower_is_better: bool = False) -> str:
    if current is None or previous is None:
        return "flat"
    if abs(current - previous) < 0.01:
        return "flat"
    improved = current < previous if lower_is_better else current > previous
    return "up" if improved else "down"


def _market_status(value: float | None, good: float, caution: float) -> str:
    if value is None:
        return "pending"
    if value >= good:
        return "healthy"
    if value >= caution:
        return "early"
    return "prelaunch"


def _market_status_label(status: str) -> str:
    return {
        "healthy": "Healthy",
        "early": "Early",
        "pending": "Pre-launch",
        "prelaunch": "Pre-launch",
    }.get(status, "Pre-launch")


def _county_zip_list(county_id: str) -> list[str]:
    try:
        from src.services.nws_same_to_zip import SAME_TO_ZIPS
        fips_by_county = {
            "hillsborough": "012057",
            "pinellas": "012103",
            "pasco": "012101",
            "polk": "012105",
            "manatee": "012081",
        }
        return SAME_TO_ZIPS.get(fips_by_county.get(county_id, ""), [])
    except Exception:
        return []


def _fetch_county_revenue_kpis(session, run_date: date, county_id: str, active: int) -> dict:
    try:
        mrr_val = session.execute(
            text("""
                SELECT SUM(plan_price)
                FROM subscribers
                WHERE status = 'active'
                  AND plan_price IS NOT NULL
                  AND county_id = :county_id
            """),
            {"county_id": county_id},
        ).scalar()
        mrr = _currency(mrr_val) if mrr_val else NA

        churned_30d = int(session.execute(
            text("""
                SELECT COUNT(*)
                FROM subscribers
                WHERE churned_at >= now() - INTERVAL '30 days'
                  AND county_id = :county_id
            """),
            {"county_id": county_id},
        ).scalar() or 0)
        churned_prev_30d = int(session.execute(
            text("""
                SELECT COUNT(*)
                FROM subscribers
                WHERE churned_at >= now() - INTERVAL '60 days'
                  AND churned_at < now() - INTERVAL '30 days'
                  AND county_id = :county_id
            """),
            {"county_id": county_id},
        ).scalar() or 0)
        churn_denom = active + churned_30d
        prev_churn_denom = active + churned_prev_30d
        churn_pct = (churned_30d / churn_denom * 100) if churn_denom else None
        prev_churn_pct = (churned_prev_30d / prev_churn_denom * 100) if prev_churn_denom else None

        delivered_today = int(session.execute(
            text("""
                SELECT COUNT(DISTINCT sl.subscriber_id)
                FROM sent_leads sl
                JOIN subscribers s ON s.id = sl.subscriber_id
                WHERE s.status = 'active'
                  AND s.county_id = :county_id
                  AND date(sl.sent_at) = :run_date
            """),
            {"county_id": county_id, "run_date": str(run_date)},
        ).scalar() or 0)
        delivered_yesterday = int(session.execute(
            text("""
                SELECT COUNT(DISTINCT sl.subscriber_id)
                FROM sent_leads sl
                JOIN subscribers s ON s.id = sl.subscriber_id
                WHERE s.status = 'active'
                  AND s.county_id = :county_id
                  AND date(sl.sent_at) = :previous_date
            """),
            {"county_id": county_id, "previous_date": str(run_date - timedelta(days=1))},
        ).scalar() or 0)
        lead_sla_pct = (delivered_today / active * 100) if active else None
        prev_lead_sla_pct = (delivered_yesterday / active * 100) if active else None

        county_zips = _county_zip_list(county_id)
        if county_zips:
            sf_row = session.execute(
                text("""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE outcome = 'demo_requested') AS bookings
                    FROM synthflow_calls
                    WHERE call_date >= current_date - 30
                      AND zip_code = ANY(:zips)
                """),
                {"zips": county_zips},
            ).fetchone()
            sf_prev_row = session.execute(
                text("""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE outcome = 'demo_requested') AS bookings
                    FROM synthflow_calls
                    WHERE call_date >= current_date - 60
                      AND call_date < current_date - 30
                      AND zip_code = ANY(:zips)
                """),
                {"zips": county_zips},
            ).fetchone()
        else:
            sf_row = sf_prev_row = None

        sf_total = int(sf_row[0] or 0) if sf_row else 0
        sf_bookings = int(sf_row[1] or 0) if sf_row else 0
        sf_prev_total = int(sf_prev_row[0] or 0) if sf_prev_row else 0
        sf_prev_bookings = int(sf_prev_row[1] or 0) if sf_prev_row else 0
        sf_pct = (sf_bookings / sf_total * 100) if sf_total else None
        sf_prev_pct = (sf_prev_bookings / sf_prev_total * 100) if sf_prev_total else None

        return {
            "subscriber_mrr": mrr,
            "subscriber_mrr_trend": "flat",
            "subscriber_churn": f"{churn_pct:.1f}%" if churn_pct is not None else NA,
            "subscriber_churn_trend": _trend(churn_pct, prev_churn_pct, lower_is_better=True),
            "lead_delivery_sla": f"{lead_sla_pct:.1f}%" if lead_sla_pct is not None else NA,
            "lead_delivery_sla_ok": lead_sla_pct is not None and lead_sla_pct >= 95.0,
            "lead_delivery_sla_trend": _trend(lead_sla_pct, prev_lead_sla_pct),
            "synthflow_booking_rate": f"{sf_pct:.1f}%" if sf_pct is not None else NA,
            "synthflow_booking_rate_trend": _trend(sf_pct, sf_prev_pct),
        }
    except Exception as exc:
        logger.warning("_fetch_county_revenue_kpis failed for %s: %s", county_id, exc)
        return {
            "subscriber_mrr": NA, "subscriber_mrr_trend": "flat",
            "subscriber_churn": NA, "subscriber_churn_trend": "flat",
            "lead_delivery_sla": NA, "lead_delivery_sla_ok": False,
            "lead_delivery_sla_trend": "flat",
            "synthflow_booking_rate": NA, "synthflow_booking_rate_trend": "flat",
        }


def _count_distress_scores(
    session, county_id: str, start_date: date, end_date: date,
    tier_filter: str | None = None,
) -> int:
    tier_clause = ""
    params: dict[str, Any] = {
        "county_id": county_id,
        "start_date": str(start_date),
        "end_date": str(end_date),
    }
    if tier_filter == "gold_plus":
        tier_clause = "AND lead_tier = ANY(:tiers)"
        params["tiers"] = list(GOLD_PLUS_TIERS)
    elif tier_filter == "up_plat":
        tier_clause = "AND lead_tier IN ('Ultra Platinum', 'Platinum')"

    return int(session.execute(
        text(f"""
            SELECT COUNT(*)
            FROM distress_scores
            WHERE county_id = :county_id
              AND date(score_date) BETWEEN :start_date AND :end_date
              {tier_clause}
        """),
        params,
    ).scalar() or 0)


def _avg_cds_for_period(session, county_id: str, start_date: date, end_date: date) -> float | None:
    val = session.execute(
        text("""
            SELECT AVG(final_cds_score)
            FROM distress_scores
            WHERE county_id = :county_id
              AND date(score_date) BETWEEN :start_date AND :end_date
              AND final_cds_score IS NOT NULL
        """),
        {"county_id": county_id, "start_date": str(start_date), "end_date": str(end_date)},
    ).scalar()
    return float(val) if val is not None else None


def _phone_email_enrichment_pct(session, county_id: str, start_date: date, end_date: date) -> float | None:
    row = session.execute(
        text("""
            WITH gp AS (
                SELECT DISTINCT ds.property_id
                FROM distress_scores ds
                WHERE ds.county_id = :county_id
                  AND date(ds.score_date) BETWEEN :start_date AND :end_date
                  AND ds.lead_tier = ANY(:tiers)
            )
            SELECT COUNT(DISTINCT gp.property_id) AS total,
                   COUNT(DISTINCT CASE
                       WHEN ec.mobile_phone IS NOT NULL AND ec.email IS NOT NULL
                       THEN gp.property_id END
                   ) AS enriched
            FROM gp
            LEFT JOIN enriched_contacts ec
              ON ec.property_id = gp.property_id
             AND ec.match_success = true
        """),
        {
            "county_id": county_id,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "tiers": list(GOLD_PLUS_TIERS),
        },
    ).fetchone()
    if not row or not row[0]:
        return None
    return int(row[1] or 0) / int(row[0]) * 100


def _active_subs_on(session, county_id: str, as_of: date) -> int:
    return int(session.execute(
        text("""
            SELECT COUNT(*)
            FROM subscribers
            WHERE county_id = :county_id
              AND status = 'active'
              AND date(created_at) <= :as_of
              AND (churned_at IS NULL OR date(churned_at) > :as_of)
        """),
        {"county_id": county_id, "as_of": str(as_of)},
    ).scalar() or 0)


def _lead_delivery_sla_pct(session, county_id: str, start_date: date, end_date: date) -> float | None:
    active = _active_subs_on(session, county_id, end_date)
    if active <= 0:
        return None
    delivered = int(session.execute(
        text("""
            SELECT COUNT(DISTINCT sl.subscriber_id)
            FROM sent_leads sl
            JOIN subscribers s ON s.id = sl.subscriber_id
            WHERE s.county_id = :county_id
              AND date(sl.sent_at) BETWEEN :start_date AND :end_date
        """),
        {"county_id": county_id, "start_date": str(start_date), "end_date": str(end_date)},
    ).scalar() or 0)
    return delivered / active * 100


def _synthflow_booking_pct(session, county_id: str, start_date: date, end_date: date) -> float | None:
    county_zips = _county_zip_list(county_id)
    if not county_zips:
        return None
    row = session.execute(
        text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE outcome = 'demo_requested') AS bookings
            FROM synthflow_calls
            WHERE call_date BETWEEN :start_date AND :end_date
              AND zip_code = ANY(:zips)
        """),
        {"start_date": str(start_date), "end_date": str(end_date), "zips": county_zips},
    ).fetchone()
    if not row or not row[0]:
        return None
    return int(row[1] or 0) / int(row[0]) * 100


def _fetch_dbpr_prospects(session, county_id: str, start_date: date, end_date: date | None = None) -> dict:
    end_date = end_date or date.today()
    rows = session.execute(
        text("""
            SELECT COALESCE(vertical, 'unknown') AS vertical, COUNT(*) AS cnt
            FROM dbpr_contacts
            WHERE county_id = :county_id
              AND created_at::date BETWEEN :start_date AND :end_date
            GROUP BY COALESCE(vertical, 'unknown')
        """),
        {"county_id": county_id, "start_date": str(start_date), "end_date": str(end_date)},
    ).fetchall()
    by_vertical = {r[0]: int(r[1] or 0) for r in rows}
    return {
        "total": sum(by_vertical.values()),
        "roofing": by_vertical.get("roofing", 0),
        "remediation": by_vertical.get("remediation", 0),
        "investors": by_vertical.get("investor", 0) + by_vertical.get("investors", 0),
    }


def _county_metric_row(
    metric: str, today: str, this_week: str, avg_7d: str, monthly: str,
    target: str = "—", trend: str = "flat", status: str | None = None,
    notes: str | None = None,
) -> dict:
    return {
        "metric": metric, "today": today, "this_week": this_week,
        "avg_7d": avg_7d, "monthly": monthly, "target": target,
        "trend": trend, "status": status, "notes": notes,
    }


def _fetch_county_performance_table(session, run_date: date, county_id: str, market: str) -> dict:
    week_start = run_date - timedelta(days=run_date.weekday())
    seven_start = run_date - timedelta(days=6)
    month_start = run_date.replace(day=1)
    prev_day = run_date - timedelta(days=1)

    def counts(tier_filter: str | None = None) -> tuple[int, int, int, int, int]:
        today = _count_distress_scores(session, county_id, run_date, run_date, tier_filter)
        week = _count_distress_scores(session, county_id, week_start, run_date, tier_filter)
        seven = _count_distress_scores(session, county_id, seven_start, run_date, tier_filter)
        month = _count_distress_scores(session, county_id, month_start, run_date, tier_filter)
        prev = _count_distress_scores(session, county_id, prev_day, prev_day, tier_filter)
        return today, week, seven, month, prev

    (leads_today, leads_week, leads_7d, leads_month, leads_prev) = counts()
    (gold_today, gold_week, gold_7d, gold_month, gold_prev) = counts("gold_plus")
    (up_today, up_week, up_7d, up_month, up_prev) = counts("up_plat")

    cds_today = _avg_cds_for_period(session, county_id, run_date, run_date)
    cds_week = _avg_cds_for_period(session, county_id, week_start, run_date)
    cds_month = _avg_cds_for_period(session, county_id, month_start, run_date)
    cds_prev = _avg_cds_for_period(session, county_id, prev_day, prev_day)

    enrich_today = _phone_email_enrichment_pct(session, county_id, run_date, run_date)
    enrich_week = _phone_email_enrichment_pct(session, county_id, week_start, run_date)
    enrich_month = _phone_email_enrichment_pct(session, county_id, month_start, run_date)
    enrich_prev = _phone_email_enrichment_pct(session, county_id, prev_day, prev_day)

    active_now = _active_subs_on(session, county_id, run_date)
    active_week_start = _active_subs_on(session, county_id, week_start - timedelta(days=1))
    active_month_start = _active_subs_on(session, county_id, month_start - timedelta(days=1))
    active_prev = _active_subs_on(session, county_id, prev_day)

    kpis = _fetch_county_revenue_kpis(session, run_date, county_id, active_now)
    mrr_val = session.execute(
        text("""
            SELECT SUM(plan_price)
            FROM subscribers
            WHERE status = 'active'
              AND plan_price IS NOT NULL
              AND county_id = :county_id
        """),
        {"county_id": county_id},
    ).scalar()
    mrr_today = _currency(mrr_val) if mrr_val else NA

    churn_today = kpis["subscriber_churn"]
    churn_month = churn_today

    sla_today = _lead_delivery_sla_pct(session, county_id, run_date, run_date)
    sla_week = _lead_delivery_sla_pct(session, county_id, week_start, run_date)
    sla_month = _lead_delivery_sla_pct(session, county_id, month_start, run_date)
    sla_prev = _lead_delivery_sla_pct(session, county_id, prev_day, prev_day)

    sf_today = _synthflow_booking_pct(session, county_id, run_date, run_date)
    sf_week = _synthflow_booking_pct(session, county_id, week_start, run_date)
    sf_month = _synthflow_booking_pct(session, county_id, month_start, run_date)
    sf_prev = _synthflow_booking_pct(session, county_id, prev_day, prev_day)

    if market == "primary":
        rows = [
            _county_metric_row("New Leads Generated", _num_fmt(leads_today), _num_fmt(leads_week), f"{leads_7d / 7:.0f}/day", _num_fmt(leads_month), trend=_trend(leads_today, leads_prev)),
            _county_metric_row("Gold+ Leads", _num_fmt(gold_today), _num_fmt(gold_week), f"{gold_7d / 7:.0f}/day", _num_fmt(gold_month), trend=_trend(gold_today, gold_prev)),
            _county_metric_row("Ultra Plat + Plat", _num_fmt(up_today), _num_fmt(up_week), f"{up_7d / 7:.0f}/day", _num_fmt(up_month), trend=_trend(up_today, up_prev)),
            _county_metric_row("Avg CDS Score", _score_fmt(cds_today), _score_fmt(cds_week), "—", _score_fmt(cds_month), "70+", _trend(cds_today, cds_prev)),
            _county_metric_row("Enrichment % (Phone+Email)", f"{enrich_today:.1f}%" if enrich_today is not None else NA, f"{enrich_week:.1f}%" if enrich_week is not None else NA, "—", f"{enrich_month:.1f}%" if enrich_month is not None else NA, "75%", _trend(enrich_today, enrich_prev)),
            _county_metric_row("Active Subscribers", _num_fmt(active_now), f"{active_now - active_week_start:+d}", "—", _num_fmt(active_now), "—", _trend(active_now, active_prev)),
            _county_metric_row("Subscriber MRR", mrr_today, "—", "—", mrr_today, "—", kpis["subscriber_mrr_trend"]),
            _county_metric_row("Subscriber Churn", churn_today, "—", "—", churn_month, "<5%", kpis["subscriber_churn_trend"]),
            _county_metric_row("Lead Delivery SLA", f"{sla_today:.1f}%" if sla_today is not None else NA, f"{sla_week:.1f}%" if sla_week is not None else NA, "—", f"{sla_month:.1f}%" if sla_month is not None else NA, "95%+", _trend(sla_today, sla_prev), status="ok" if sla_today is not None and sla_today >= 95 else "warn"),
            _county_metric_row("Synthflow Booking Rate", f"{sf_today:.1f}%" if sf_today is not None else NA, f"{sf_week:.1f}%" if sf_week is not None else NA, "—", f"{sf_month:.1f}%" if sf_month is not None else NA, "8%+", _trend(sf_today, sf_prev)),
        ]
        constraints = []
        if enrich_today is not None and enrich_today < 75:
            constraints.append("low enrichment")
        if kpis["subscriber_churn"] != NA:
            try:
                if float(kpis["subscriber_churn"].rstrip("%")) >= 5:
                    constraints.append("high churn")
            except ValueError:
                pass
        if sla_today is not None and sla_today < 95:
            constraints.append("lead delivery below SLA")
        status = "exceeding target" if gold_today > gold_prev and (enrich_today or 0) >= 75 else ("lagging" if constraints else "on track")
        return {
            "county_id": county_id,
            "label": county_id.replace("_", " ").title(),
            "market_label": "Primary Market",
            "table_type": "primary",
            "rows": rows,
            "status_line": f"{county_id.replace('_', ' ').title()} Status: {status}",
            "constraint_line": f"Key Constraint: {', '.join(constraints) if constraints else 'none'}",
        }

    h_leads_today = _count_distress_scores(session, "hillsborough", run_date, run_date)
    density_pct = (leads_today / h_leads_today * 100) if h_leads_today else None
    prospects = _fetch_dbpr_prospects(session, county_id, month_start, run_date)
    scraper_row = session.execute(
        text("""
            SELECT COUNT(*) AS total_runs,
                   SUM(CASE WHEN run_success THEN 1 ELSE 0 END) AS ok_runs
            FROM scraper_run_stats
            WHERE run_date = :run_date AND county_id = :county_id
        """),
        {"run_date": str(run_date), "county_id": county_id},
    ).fetchone()
    scraper_total = int(scraper_row[0] or 0) if scraper_row else 0
    scraper_ok = int(scraper_row[1] or 0) if scraper_row else 0
    scraper_stale = max(0, scraper_total - scraper_ok)
    scraper_status = "healthy" if scraper_total and scraper_ok == scraper_total else ("early" if scraper_ok else "pending")
    lead_density_status = _market_status(density_pct, 60, 40)
    rows = [
        _county_metric_row("New Leads Generated", _num_fmt(leads_today), _num_fmt(leads_week), f"{leads_7d / 7:.0f}/day", _num_fmt(leads_month), status=_market_status(density_pct, 60, 40), notes=f"{density_pct:.1f}% of Hillsborough density" if density_pct is not None else "No Hillsborough baseline"),
        _county_metric_row("Gold+ Leads", _num_fmt(gold_today), _num_fmt(gold_week), f"{gold_7d / 7:.0f}/day", _num_fmt(gold_month), status=_market_status((gold_today / gold_prev * 100) if gold_prev else density_pct, 60, 40), notes="Expected at 60-70% of HCSB"),
        _county_metric_row("Scraper Status", "All live" if scraper_stale == 0 and scraper_total else f"{scraper_stale} stale", "—", "—", "—", status=scraper_status, notes=f"{scraper_ok}/{scraper_total} sources OK"),
        _county_metric_row("Enrichment % Available", f"{enrich_today:.1f}%" if enrich_today is not None else NA, f"{enrich_week:.1f}%" if enrich_week is not None else NA, "—", f"{enrich_month:.1f}%" if enrich_month is not None else NA, status=_market_status(enrich_today, 75, 50), notes="Targeting 75%+"),
        _county_metric_row("Lead Density (Signals/Week)", _num_fmt(leads_today), _num_fmt(leads_week), "—", _num_fmt(leads_month), status=lead_density_status, notes="vs Hillsborough baseline"),
        _county_metric_row("Pre-Launch Prospects Found", _num_fmt(prospects["total"]), _num_fmt(prospects["total"]), "—", _num_fmt(prospects["total"]), status=_market_status(prospects["total"], 100, 25), notes=f"Roofing {prospects['roofing']} / Remediation {prospects['remediation']} / Investors {prospects['investors']}"),
        _county_metric_row("Active Subscribers", _num_fmt(active_now), _num_fmt(active_now - active_week_start), "—", _num_fmt(active_now), status="pending" if active_now == 0 else "early", notes="Pre-revenue" if active_now == 0 else "Revenue started"),
        _county_metric_row("Subscriber MRR", mrr_today, "—", "—", mrr_today, status="pending" if mrr_today == NA else "early", notes="Pre-revenue" if mrr_today == NA else "Launch revenue started"),
        _county_metric_row("Expansion Status", "Soft Launch" if active_now else "Prep", "—", "—", "—", status=scraper_status, notes="Next milestone: sales activation" if scraper_status == "healthy" else "Next milestone: scraper stability"),
    ]
    confidence = "High" if scraper_status == "healthy" and leads_week >= 0.6 * max(1, _count_distress_scores(session, "hillsborough", week_start, run_date)) else ("Medium" if scraper_ok else "Needs work")
    return {
        "county_id": county_id,
        "label": county_id.replace("_", " ").title(),
        "market_label": "Expansion Market",
        "table_type": "expansion",
        "rows": rows,
        "status_line": f"{county_id.replace('_', ' ').title()} Status: {'Data healthy; ready for sales' if scraper_status == 'healthy' else 'Data still stabilizing'}",
        "constraint_line": f"Expansion Confidence: {confidence}",
    }


def _fetch_enrichment_rate(session, run_date: date, county_ids: list[str]) -> str:
    try:
        rows = session.execute(
            text("""
                WITH gp_today AS (
                    SELECT DISTINCT ON (ds.property_id) ds.property_id
                    FROM distress_scores ds
                    WHERE ds.county_id = ANY(:county_ids)
                      AND date(ds.score_date) = :today
                      AND ds.lead_tier = ANY(:tiers)
                    ORDER BY ds.property_id, ds.score_date DESC
                )
                SELECT COUNT(DISTINCT gt.property_id)  AS total_gp,
                       COUNT(DISTINCT ec.property_id)  AS enriched
                FROM gp_today gt
                LEFT JOIN enriched_contacts ec
                    ON ec.property_id = gt.property_id
                   AND ec.match_success = true AND ec.mobile_phone IS NOT NULL
            """),
            {"county_ids": county_ids, "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchone()
        return _pct(rows[1], rows[0]) if rows and rows[0] else NA
    except Exception as exc:
        logger.warning("_fetch_enrichment_rate failed: %s", exc)
        return NA


def _fetch_cora_autonomy(session, run_date: date | None = None) -> str:
    """True platform autonomy for finalized Cora decisions in the trailing 7 days.

    Numerator: completed decisions that stayed fully autonomous.
    Denominator: all finalized decisions, including approval-required,
    escalated, failed, aborted, rejected, and overridden outcomes. This replaces
    the older scorecard/card read that effectively reported a "not overridden"
    slice and overstated platform autonomy.
    """
    end_date = run_date or date.today()
    start_date = end_date - timedelta(days=6)
    try:
        row = session.execute(
            text("""
                SELECT
                    COUNT(*) AS finalized,
                    COUNT(*) FILTER (
                        WHERE terminal_status = 'completed'
                          AND autonomy_class = 'autonomous'
                          AND COALESCE(requires_approval, false) = false
                          AND approved_at IS NULL
                          AND overridden_at IS NULL
                    ) AS autonomous_completed
                FROM agent_decisions
                WHERE date(started_at) BETWEEN :start_date AND :end_date
                  AND terminal_status IS NOT NULL
            """),
            {"start_date": str(start_date), "end_date": str(end_date)},
        ).fetchone()
        if row and row[0]:
            return _pct(row[1], row[0])
        return NA
    except Exception as exc:
        logger.warning("_fetch_cora_autonomy failed: %s", exc)
        return NA


# ---------------------------------------------------------------------------
# NEW: Additional query functions for missing model columns
# ---------------------------------------------------------------------------

def _fetch_trial_conversion(session, county_ids: list[str]) -> str:
    """Trial→Paid conversion rate.

    Of subscribers who started a trial whose trial window has now closed
    (``is_trial = True`` AND ``trial_ends_at < now()``), the percentage that
    are currently ``status = 'active'`` (i.e. converted to paid).
    """
    try:
        row = session.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE is_trial = true
                          AND trial_ends_at IS NOT NULL
                          AND trial_ends_at < now()
                    ) AS ended,
                    COUNT(*) FILTER (
                        WHERE is_trial = true
                          AND trial_ends_at IS NOT NULL
                          AND trial_ends_at < now()
                          AND status = 'active'
                    ) AS converted
                FROM subscribers
                WHERE county_id = ANY(:cids)
            """),
            {"cids": county_ids},
        ).fetchone()
        if not row or not row[0]:
            return NA
        return _pct(row[1], row[0])
    except Exception as exc:
        logger.warning("_fetch_trial_conversion failed: %s", exc)
        return NA


def _fetch_enrichment_pct_by_vertical(session, run_date: date, county_ids: list[str]) -> dict:
    """Phone+email enrichment % keyed by best vertical for Gold+ leads (7-day).

    Joins the latest Gold+ distress_scores to enriched_contacts, derives each
    property's best vertical from the ``vertical_scores`` JSONB, and reports the
    share of each vertical's leads that have both a mobile phone and email.
    """
    seven_start = run_date - timedelta(days=6)
    try:
        rows = session.execute(
            text("""
                WITH latest AS (
                    SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.vertical_scores
                    FROM distress_scores ds
                    WHERE ds.county_id = ANY(:cids)
                      AND date(ds.score_date) BETWEEN :ss AND :today
                      AND ds.lead_tier = ANY(:tiers)
                      AND ds.vertical_scores IS NOT NULL
                    ORDER BY ds.property_id, ds.score_date DESC
                )
                SELECT l.vertical_scores,
                       (ec.property_id IS NOT NULL
                        AND ec.mobile_phone IS NOT NULL
                        AND ec.email IS NOT NULL) AS enriched
                FROM latest l
                LEFT JOIN enriched_contacts ec
                    ON ec.property_id = l.property_id AND ec.match_success = true
            """),
            {"cids": county_ids, "ss": str(seven_start), "today": str(run_date),
             "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchall()
    except Exception as exc:
        logger.warning("_fetch_enrichment_pct_by_vertical failed: %s", exc)
        return {}
    totals: dict = defaultdict(int)
    enriched_counts: dict = defaultdict(int)
    for vertical_scores, is_enriched in rows:
        if not vertical_scores:
            continue
        best = max(vertical_scores, key=lambda k: vertical_scores.get(k, 0))
        if best in VERTICAL_DISPLAY:
            totals[best] += 1
            if is_enriched:
                enriched_counts[best] += 1
    return {
        key: (_pct(enriched_counts[key], totals[key]) if totals[key] else NA)
        for key in VERTICAL_DISPLAY
    }


def _fetch_exec_summary_rows(session, run_date: date, county_ids: list[str]) -> list[dict]:
    """Build executive summary table rows: Metric | Today | This Week | Monthly Pace | Target | Status."""
    week_start = run_date - timedelta(days=run_date.weekday())
    month_start = run_date.replace(day=1)
    prev_month_end = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    results = []

    def daily(n, d):
        return f"{n / d:.0f}/day" if d else NA

    # Total Leads
    t_today = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date)=:today AND county_id=ANY(:cids)"), {"today": str(run_date), "cids": county_ids}).scalar() or 0)
    t_week = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date) BETWEEN :ws AND :today AND county_id=ANY(:cids)"), {"ws": str(week_start), "today": str(run_date), "cids": county_ids}).scalar() or 0)
    t_month = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date) BETWEEN :ms AND :today AND county_id=ANY(:cids)"), {"ms": str(month_start), "today": str(run_date), "cids": county_ids}).scalar() or 0)
    t_prev_month = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date) BETWEEN :pms AND :pme AND county_id=ANY(:cids)"), {"pms": str(prev_month_start), "pme": str(prev_month_end), "cids": county_ids}).scalar() or 0)
    days_in_month = (run_date - month_start).days + 1
    monthly_pace = t_month / days_in_month * 30 if days_in_month else NA
    results.append(("Total Leads Generated", _num_fmt(t_today), _num_fmt(t_week), daily(t_month, days_in_month), _num_fmt(int(monthly_pace)) if isinstance(monthly_pace, float) else NA, _trend(float(t_today), float(t_prev_month / 30)), "", ""))

    # Gold+ Leads
    gp_today = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date)=:today AND lead_tier=ANY(:tiers) AND county_id=ANY(:cids)"), {"today": str(run_date), "tiers": list(GOLD_PLUS_TIERS), "cids": county_ids}).scalar() or 0)
    gp_week = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date) BETWEEN :ws AND :today AND lead_tier=ANY(:tiers) AND county_id=ANY(:cids)"), {"ws": str(week_start), "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS), "cids": county_ids}).scalar() or 0)
    gp_month = int(session.execute(text("SELECT COUNT(*) FROM distress_scores WHERE date(score_date) BETWEEN :ms AND :today AND lead_tier=ANY(:tiers) AND county_id=ANY(:cids)"), {"ms": str(month_start), "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS), "cids": county_ids}).scalar() or 0)
    gp_pace = gp_month / days_in_month * 30 if days_in_month else NA
    results.append(("Gold+ Leads (Monetizable)", _num_fmt(gp_today), _num_fmt(gp_week), daily(gp_month, days_in_month), _num_fmt(int(gp_pace)) if isinstance(gp_pace, float) else NA, "—", "", ""))

    # MRR
    mrr_val = session.execute(text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar()
    mrr = _currency(mrr_val) if mrr_val else NA
    results.append(("MRR (if live subscribers)", mrr, mrr, mrr, mrr, "—", "", ""))

    # Active Subscribers
    active = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar() or 0)
    active_last_month = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND date(created_at) <= :pme AND (churned_at IS NULL OR date(churned_at) > :pme) AND county_id=ANY(:cids)"), {"pme": str(prev_month_end), "cids": county_ids}).scalar() or 0)
    results.append(("Active Subscribers", _num_fmt(active), f"{active - active_last_month:+d}", "—", _num_fmt(active), _trend(float(active), float(active_last_month)), "", ""))

    # Lead Enrichment Rate
    enrich = _fetch_enrichment_rate(session, run_date, county_ids)
    results.append(("Lead Enrichment Rate", enrich, enrich, enrich, enrich, "75%", "", "" if enrich != NA and float(enrich.rstrip("%")) >= 75 else "⚠️"))

    # Avg CDS Score
    cds = _fetch_avg_cds(session, run_date, county_ids)
    results.append(("Avg CDS Score", cds, cds, "—", cds, "70+", "", "" if cds != NA and float(cds) >= 70 else "⚠️"))

    # Trial→Paid Conversion (computed from is_trial / trial_ends_at)
    trial_conv = _fetch_trial_conversion(session, county_ids)
    results.append(("Trial→Paid Conversion", trial_conv, trial_conv, "—", trial_conv, "15%+", "", ""))

    # 30-Day Churn Rate
    churn_val = session.execute(text("SELECT COUNT(*) FROM subscribers WHERE churned_at >= now() - INTERVAL '30 days' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar() or 0
    churn_rate = f"{int(churn_val or 0) / max(1, active + int(churn_val or 0)) * 100:.1f}%" if active + int(churn_val or 0) > 0 else NA
    results.append(("Churn Rate (30-day)", churn_rate, "—", churn_rate, churn_rate, "<5%", "", ""))

    # Cora Autonomy
    auto = _fetch_cora_autonomy(session, run_date)
    results.append(("Cora Platform Autonomy", auto, "7d window", "—", auto, "95%+", "", ""))

    return [
        {"metric": r[0], "today": r[1], "this_week": r[2], "monthly_pace": r[3], "target": r[4], "trend": r[5], "status": r[6], "notes": r[7]}
        for r in results
    ]


def _fetch_scraper_ingest_details(session, run_date: date, county_ids: list[str]) -> list[dict]:
    """Build scraper rows with Last Update, Days Stale, Quality, Action columns."""
    rows = session.execute(
        text("""
            SELECT source_type,
                   MAX(run_date) AS last_update,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN run_success THEN 1 ELSE 0 END) AS ok_runs,
                   BOOL_AND(run_success) AS all_ok
            FROM scraper_run_stats
            WHERE county_id = ANY(:cids)
            GROUP BY source_type
            ORDER BY source_type
        """),
        {"cids": county_ids},
    ).fetchall()
    def _row(source_type: str, last_update, total_runs, ok_runs, all_ok) -> dict:
        days_stale = (run_date - last_update).days if last_update else None
        health = _staleness_badge(days_stale)
        action = "—"
        if days_stale is not None and days_stale > 3:
            action = "ESCALATE" if days_stale > 5 else "Check"
        return {
            "source": source_type.replace("_", " ").title(),
            "records_per_day": int(total_runs or 0),
            "updated": str(last_update) if last_update else "Never",
            "days_stale": days_stale if days_stale is not None else "Never",
            "status": "ok" if bool(all_ok) else ("caution" if ok_runs and ok_runs > 0 else "fail"),
            "quality": _quality_label(health),
            "action": action,
        }

    seen: set[str] = set()
    result = []
    for source_type, last_update, total_runs, ok_runs, all_ok in rows:
        seen.add(source_type)
        result.append(_row(source_type, last_update, total_runs, ok_runs, all_ok))

    # Code Enforcement / Divorce Filings: valid scraper_run_stats source_types
    # that aren't in the shared SCRAPER_ORDER — surface a row even with no runs.
    for extra in DASHBOARD_EXTRA_SOURCES:
        if extra in seen:
            continue
        er = session.execute(
            text("""
                SELECT MAX(run_date) AS last_update, COUNT(*) AS total_runs,
                       SUM(CASE WHEN run_success THEN 1 ELSE 0 END) AS ok_runs,
                       BOOL_AND(run_success) AS all_ok
                FROM scraper_run_stats
                WHERE county_id = ANY(:cids) AND source_type = :st
            """),
            {"cids": county_ids, "st": extra},
        ).fetchone()
        result.append(_row(extra, er[0] if er else None, er[1] if er else 0,
                           er[2] if er else 0, er[3] if er else None))

    # Stop Work Orders: derived from building_permits (enforcement permits),
    # not a scraper_run_stats source_type.
    swo = session.execute(
        text("""
            SELECT MAX(date_added) AS last_update, COUNT(*) AS cnt
            FROM building_permits
            WHERE is_enforcement_permit = true AND county_id = ANY(:cids)
        """),
        {"cids": county_ids},
    ).fetchone()
    swo_last = swo[0] if swo else None
    swo_cnt = int(swo[1] or 0) if swo else 0
    swo_days = (run_date - swo_last).days if swo_last else None
    swo_health = _staleness_badge(swo_days)
    result.append({
        "source": "Stop Work Orders",
        "records_per_day": swo_cnt,
        "updated": str(swo_last) if swo_last else "Never",
        "days_stale": swo_days if swo_days is not None else "Never",
        "status": swo_health,
        "quality": _quality_label(swo_health),
        "action": ("ESCALATE" if swo_days is not None and swo_days > 5
                   else "Check" if swo_days is not None and swo_days > 3 else "—"),
    })
    return result


def _fetch_enrichment_both_pct_by_tier(session, run_date: date, county_ids: list[str]) -> list[dict]:
    """Enrichment by tier with Both%, Impact, Action columns — all 5 tiers."""
    rows = session.execute(
        text("""
            WITH gp AS (
                SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.lead_tier
                FROM distress_scores ds
                WHERE ds.county_id = ANY(:cids)
                  AND date(ds.score_date) = :today AND ds.lead_tier = ANY(:tiers)
                ORDER BY ds.property_id, ds.score_date DESC
            )
            SELECT gp.lead_tier,
                COUNT(DISTINCT gp.property_id) AS total,
                COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NOT NULL AND ec.email IS NOT NULL THEN gp.property_id END) AS both_,
                COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NOT NULL THEN gp.property_id END) AS phone,
                COUNT(DISTINCT CASE WHEN ec.email IS NOT NULL THEN gp.property_id END) AS email
            FROM gp
            LEFT JOIN enriched_contacts ec ON ec.property_id = gp.property_id AND ec.match_success = true
            GROUP BY gp.lead_tier
            ORDER BY CASE gp.lead_tier
                WHEN 'Ultra Platinum' THEN 1 WHEN 'Platinum' THEN 2 WHEN 'Gold' THEN 3
                WHEN 'Silver' THEN 4 ELSE 5 END
        """),
        {"cids": county_ids, "today": str(run_date), "tiers": ALL_TIERS},
    ).fetchall()
    result = []
    for tier, total, both_, phone, email in rows:
        total, both_, phone, email = int(total or 0), int(both_ or 0), int(phone or 0), int(email or 0)
        both_pct = (both_ / total * 100) if total else 0
        phone_pct = (phone / total * 100) if total else 0
        email_pct = (email / total * 100) if total else 0
        impact = {"Ultra Platinum": "Highest ROI — must be enriched", "Platinum": "Very high ROI", "Gold": "Good ROI", "Silver": "Moderate ROI", "Bronze": "Lower ROI"}.get(tier, "")
        action = ""
        if tier in ("Ultra Platinum", "Platinum") and both_pct < 80:
            action = "Escalate if <80%"
        elif tier == "Gold" and both_pct < 70:
            action = "Monitor if <70%"
        elif tier == "Silver" and both_pct < 60:
            action = "Monitor if <60%"
        elif tier == "Bronze" and both_pct < 50:
            action = "Monitor if <50%"
        result.append({
            "tier": tier, "total": total,
            "phone_pct": f"{phone_pct:.1f}%",
            "email_pct": f"{email_pct:.1f}%",
            "both_pct": f"{both_pct:.1f}%",
            "impact": impact,
            "action": action,
        })
    return result


def _fetch_enrichment_source_detailed(session, county_ids: list[str]) -> list[dict]:
    """Enrichment source performance with Cost/Lead, Volume/Day, API Health."""
    since = date.today() - timedelta(days=7)
    rows = session.execute(
        text("""
            SELECT source, COUNT(*) AS total,
                   SUM(CASE WHEN match_success THEN 1 ELSE 0 END) AS matched,
                   SUM(CASE WHEN mobile_phone IS NOT NULL THEN 1 ELSE 0 END) AS with_phone
            FROM enriched_contacts
            WHERE county_id = ANY(:cids) AND enriched_at >= :since
            GROUP BY source ORDER BY total DESC
        """),
        {"cids": county_ids, "since": str(since)},
    ).fetchall()

    # Real cost/lead from enrichment_usage_logs (cost_cents per vendor call).
    # Cost per *successful* lead = total spend / successful lookups, all-time.
    vendor_cost: dict = {}
    try:
        for vendor, cents, ok in session.execute(text("""
            SELECT vendor, SUM(cost_cents) AS cents,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) AS ok
            FROM enrichment_usage_logs GROUP BY vendor
        """)).fetchall():
            ok = int(ok or 0)
            if ok:
                vendor_cost[vendor] = (int(cents or 0) / 100.0) / ok
    except Exception as exc:
        logger.warning("_fetch_enrichment_source_detailed cost query failed: %s", exc)

    result = []
    for source, total, matched, with_phone in rows:
        total, matched = int(total or 0), int(matched or 0)
        success_pct = _pct(matched, total)
        cpl = vendor_cost.get(SOURCE_TO_VENDOR.get(source))
        result.append({
            "source": SOURCE_LABELS.get(source, source),
            "success_rate": success_pct,
            "cost_per_lead": f"${cpl:.2f}" if cpl is not None else "N/A",  # N/A where vendor logs no cost
            "volume_per_day": f"{total / 7:.0f}/day" if total else "0/day",
            "api_health": "✅" if matched > 0 else "⚠️",
            "status": f"Match: {success_pct}",
        })
    return result


def _fetch_vertical_performance_extended(session, run_date: date, county_ids: list[str]) -> list[dict]:
    """Vertical performance with 7-Day Avg, Status, Enrichment %, Monetization Status."""
    seven_start = run_date - timedelta(days=6)
    week_start = run_date - timedelta(days=run_date.weekday())

    gp_7d = session.execute(
        text("""
            WITH latest AS (
                SELECT DISTINCT ON (property_id) property_id, lead_tier, vertical_scores
                FROM distress_scores
                WHERE county_id = ANY(:cids) AND date(score_date) BETWEEN :ss AND :today
                  AND lead_tier = ANY(:tiers)
                ORDER BY property_id, score_date DESC
            )
            SELECT COUNT(*) FROM latest
        """),
        {"cids": county_ids, "ss": str(seven_start), "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS)},
    ).scalar() or 0
    gp_7d_avg = round(int(gp_7d) / 7, 1)

    gp_week = session.execute(
        text("""
            WITH latest AS (
                SELECT DISTINCT ON (property_id) property_id, lead_tier, vertical_scores
                FROM distress_scores
                WHERE county_id = ANY(:cids) AND date(score_date) BETWEEN :ws AND :today
                  AND lead_tier = ANY(:tiers)
                ORDER BY property_id, score_date DESC
            )
            SELECT COUNT(*) FROM latest
        """),
        {"cids": county_ids, "ws": str(week_start), "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS)},
    ).scalar() or 0
    gp_week_avg = round(int(gp_week) / max(1, (run_date - week_start).days + 1), 1)

    enrichment_by_vertical = _fetch_enrichment_pct_by_vertical(session, run_date, county_ids)

    # Best-vertical attribution: each Gold+ property counts toward only its
    # dominant vertical (the max score in vertical_scores), not every vertical it
    # was scored on. distress_scores stores all six vertical scores per property,
    # so a "key present" check counted every property in every vertical (all 100%).
    best_counts: dict = defaultdict(int)
    try:
        vs_rows = session.execute(
            text("""
                SELECT vertical_scores FROM (
                    SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.vertical_scores
                    FROM distress_scores ds
                    WHERE ds.county_id = ANY(:cids)
                      AND date(ds.score_date) BETWEEN :ss AND :today
                      AND ds.lead_tier = ANY(:tiers)
                    ORDER BY ds.property_id, ds.score_date DESC
                ) latest
                WHERE vertical_scores IS NOT NULL
            """),
            {"cids": county_ids, "ss": str(seven_start), "today": str(run_date),
             "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchall()
        for (vertical_scores,) in vs_rows:
            if not vertical_scores:
                continue
            best = max(vertical_scores, key=lambda k: vertical_scores.get(k, 0))
            if best in VERTICAL_DISPLAY:
                best_counts[best] += 1
    except Exception as exc:
        logger.warning("_fetch_vertical_performance_extended best-vertical query failed: %s", exc)

    rows = []
    for key, label in VERTICAL_DISPLAY.items():
        count = best_counts.get(key, 0)

        sub_count = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND vertical=:vert AND county_id=ANY(:cids)"),
            {"vert": key, "cids": county_ids},
        ).scalar() or 0)

        status = "✅" if count > 10 else ("🟡" if count > 5 else "⏳")
        rows.append({
            "vertical": label,
            "gold_plus": count,
            "pct": _pct(count, gp_7d),
            "avg_7d": f"{count / 7:.0f}/day" if count else "0/day",
            "status": status,
            "enrichment_pct": enrichment_by_vertical.get(key, NA),
            "monetization": f"Active subs: {sub_count}",
        })
    return rows


def _fetch_revenue_table_extended(
    session, run_date: date, county_ids: list[str],
    subscriber_metrics: dict | None = None,
) -> list[dict]:
    """Revenue & subscriber metric rows with This Month, Monthly Pace, Target, Status columns."""
    subscriber_metrics = subscriber_metrics or {}
    month_start = run_date.replace(day=1)
    prev_day = run_date - timedelta(days=1)
    week_start = run_date - timedelta(days=run_date.weekday())

    active = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar() or 0)
    mrr_val = session.execute(text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar()
    mrr = _currency(mrr_val) if mrr_val else NA
    # MRR from Hillsborough — filtered on county_id='hillsborough' specifically.
    hcsb_mrr_val = session.execute(text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL AND county_id='hillsborough'")).scalar()
    hcsb_mrr = _currency(hcsb_mrr_val) if hcsb_mrr_val else NA
    new_week = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND created_at >= :ws AND county_id=ANY(:cids)"), {"ws": str(week_start), "cids": county_ids}).scalar() or 0)
    new_month = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND created_at >= :ms AND county_id=ANY(:cids)"), {"ms": str(month_start), "cids": county_ids}).scalar() or 0)
    churned_month = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE churned_at >= :ms AND county_id=ANY(:cids)"), {"ms": str(month_start), "cids": county_ids}).scalar() or 0)

    # Pulled from subscriber_metrics so we don't re-query the same aggregates.
    trial_signups = subscriber_metrics.get("trial_signups")
    trial_signups_fmt = _num_fmt(trial_signups) if trial_signups is not None else NA
    avg_ltv = subscriber_metrics.get("avg_ltv", NA)
    trial_conv = _fetch_trial_conversion(session, county_ids)

    rows = [
        ("MRR Total", mrr, mrr, mrr, mrr, "—", ""),
        ("MRR from Hillsborough", hcsb_mrr, hcsb_mrr, hcsb_mrr, hcsb_mrr, "—", ""),
        ("MRR from Pinellas", "$0 (Pre-revenue)", "$0", "$0", "$0", "$0 (Pre-revenue)", "⏳"),
        ("MRR from Other Counties", "$0", "$0", "$0", "$0", "$0", "⏳"),
        ("Active Subscribers", _num_fmt(active), f"+{new_week}", f"+{new_month}", _num_fmt(active), "—", ""),
        ("New Subs This Week", _num_fmt(new_week), "", "", "", "", ""),
        ("Churned Subs This Week", _num_fmt(churned_month), "", "", "", "", ""),
        ("Trial Signups", trial_signups_fmt, trial_signups_fmt, trial_signups_fmt, trial_signups_fmt, "—", ""),
        ("Trial→Paid Conversion", trial_conv, trial_conv, trial_conv, trial_conv, "15%+", ""),
        ("Avg Subscriber LTV", avg_ltv, "—", "—", avg_ltv, "—", ""),
        ("Churn Rate (30-day)", NA, NA, NA, NA, "<5%", ""),
    ]

    # Compute churn for display
    churned_30d = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE churned_at >= now() - INTERVAL '30 days' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar() or 0)
    churn_rate = f"{churned_30d / max(1, active + churned_30d) * 100:.1f}%" if active + churned_30d > 0 else NA
    rows[10] = ("Churn Rate (30-day)", churn_rate, churn_rate, churn_rate, churn_rate, "<5%", "")

    return [
        {"metric": r[0], "today": r[1], "this_week": r[2], "this_month": r[3], "monthly_pace": r[4], "target": r[5], "status": r[6]}
        for r in rows
    ]


def _fetch_conversion_funnel(session, run_date: date, county_ids: list[str]) -> list[dict]:
    """Cross-entity conversion funnel over the last 30 days, county-scoped.

    Stages blend lead-flow (scored -> delivered -> opened -> clicked -> demo)
    with subscriber-flow (trial -> new paid -> closed deal), so the conversion
    percentages are DIRECTIONAL — not a single-cohort funnel. Each stage reads an
    existing table; mid-funnel sales stages (Demo/Closed) fill in as Synthflow
    webhooks and GHL stage callbacks land.

    Returns ordered rows: {stage, count, pct_of_top, conv_to_next}.
    """
    start = run_date - timedelta(days=30)
    zips = sorted({z for cid in county_ids for z in _county_zip_list(cid)})

    def scalar(sql: str, params: dict) -> int:
        try:
            return int(session.execute(text(sql), params).scalar() or 0)
        except Exception as exc:
            logger.warning("_fetch_conversion_funnel stage failed: %s", exc)
            return 0

    common = {"cids": county_ids, "start": str(start), "today": str(run_date)}

    scored = scalar(
        "SELECT COUNT(DISTINCT property_id) FROM distress_scores "
        "WHERE county_id = ANY(:cids) AND date(score_date) BETWEEN :start AND :today "
        "AND lead_tier = ANY(:tiers)",
        {**common, "tiers": list(GOLD_PLUS_TIERS)},
    )
    delivered = scalar(
        "SELECT COUNT(*) FROM sent_leads sl JOIN subscribers s ON s.id = sl.subscriber_id "
        "WHERE s.county_id = ANY(:cids) AND date(sl.sent_at) BETWEEN :start AND :today",
        common,
    )
    opened = scalar(
        "SELECT COUNT(*) FROM message_outcomes mo JOIN subscribers s ON s.id = mo.subscriber_id "
        "WHERE s.county_id = ANY(:cids) AND mo.opened_at IS NOT NULL "
        "AND date(mo.sent_at) BETWEEN :start AND :today",
        common,
    )
    clicked = scalar(
        "SELECT COUNT(*) FROM message_outcomes mo JOIN subscribers s ON s.id = mo.subscriber_id "
        "WHERE s.county_id = ANY(:cids) AND mo.clicked_at IS NOT NULL "
        "AND date(mo.sent_at) BETWEEN :start AND :today",
        common,
    )
    demo = scalar(
        "SELECT COUNT(*) FROM synthflow_calls WHERE outcome = 'demo_requested' "
        "AND call_date BETWEEN :start AND :today AND zip_code = ANY(:zips)",
        {"start": str(start), "today": str(run_date), "zips": zips},
    ) if zips else 0
    trial = scalar(
        "SELECT COUNT(*) FROM subscribers WHERE is_trial = true AND county_id = ANY(:cids) "
        "AND created_at >= now() - INTERVAL '30 days'",
        {"cids": county_ids},
    )
    paid_new = scalar(
        "SELECT COUNT(*) FROM subscribers WHERE status = 'active' AND county_id = ANY(:cids) "
        "AND created_at >= now() - INTERVAL '30 days'",
        {"cids": county_ids},
    )
    closed = scalar(
        "SELECT COUNT(*) FROM deal_outcomes d JOIN subscribers s ON s.id = d.subscriber_id "
        "WHERE s.county_id = ANY(:cids) AND d.pipeline_stage = 'closed_won' "
        "AND d.created_at >= now() - INTERVAL '30 days'",
        {"cids": county_ids},
    )

    stages = [
        ("Leads Scored (Gold+)", scored),
        ("Delivered to Subscribers", delivered),
        ("Opened", opened),
        ("Clicked / Engaged", clicked),
        ("Demo Booked", demo),
        ("Trial Started", trial),
        ("New Paid Subscribers", paid_new),
        ("Deals Closed (won)", closed),
    ]
    top = stages[0][1] or 0
    out = []
    for i, (label, count) in enumerate(stages):
        nxt = stages[i + 1][1] if i + 1 < len(stages) else None
        out.append({
            "stage": label,
            "count": _num_fmt(count),
            "pct_of_top": _pct(count, top) if top else NA,
            "conv_to_next": (_pct(nxt, count) if (nxt is not None and count) else ("—" if nxt is None else NA)),
        })
    return out


def _fetch_engagement_by_cohort(session, county_ids: list[str]) -> dict:
    """Engagement band per active-subscriber cohort over the last 30 days.

    Signals (existing tables — no new infra): message_outcomes
    (opened/clicked/replied/delivered, populated by Cora SMS + any email-event
    capture), sent_leads (delivery), deal_outcomes (deals reported).

    Banding per subscriber:
        High   — clicked OR replied OR reported a deal
        Medium — opened, OR delivered >= 3 (engaged with volume)
        Low    — received >= 1 delivery, no opens
        (none) — no signal at all

    Returns {cohort_label: "High 🟢" | "Medium 🟡" | "Low 🔴" | "—"} using the
    dominant band per cohort (ties resolve to the higher band). Cohort labels
    match _fetch_cohort_extended so the rows line up.
    """
    try:
        rows = session.execute(
            text("""
                WITH sub AS (
                    SELECT s.id,
                        CASE
                            WHEN s.founding_member = true THEN 'Founding (1st month)'
                            WHEN s.created_at >= now() - INTERVAL '30 days' THEN 'New (30d)'
                            WHEN s.created_at >= now() - INTERVAL '90 days' THEN 'Early (2-3 months)'
                            ELSE 'Established (3+ months)'
                        END AS cohort
                    FROM subscribers s
                    WHERE s.status = 'active' AND s.county_id = ANY(:cids)
                ),
                sl AS (
                    SELECT subscriber_id, COUNT(*) AS delivered
                    FROM sent_leads
                    WHERE sent_at >= now() - INTERVAL '30 days'
                    GROUP BY subscriber_id
                ),
                mo AS (
                    SELECT subscriber_id,
                        COUNT(*) FILTER (WHERE clicked_at   IS NOT NULL) AS clicked,
                        COUNT(*) FILTER (WHERE opened_at    IS NOT NULL) AS opened,
                        COUNT(*) FILTER (WHERE replied_at   IS NOT NULL) AS replied,
                        COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) AS msg_delivered
                    FROM message_outcomes
                    WHERE sent_at >= now() - INTERVAL '30 days'
                    GROUP BY subscriber_id
                ),
                dl AS (
                    SELECT subscriber_id, COUNT(*) AS deals
                    FROM deal_outcomes
                    WHERE created_at >= now() - INTERVAL '30 days'
                    GROUP BY subscriber_id
                ),
                flags AS (
                    SELECT sub.cohort,
                        (COALESCE(mo.clicked, 0) > 0 OR COALESCE(mo.replied, 0) > 0
                         OR COALESCE(dl.deals, 0) > 0) AS high_flag,
                        (COALESCE(mo.opened, 0) > 0
                         OR (COALESCE(sl.delivered, 0) + COALESCE(mo.msg_delivered, 0)) >= 3) AS med_flag,
                        ((COALESCE(sl.delivered, 0) + COALESCE(mo.msg_delivered, 0)) > 0) AS low_flag
                    FROM sub
                    LEFT JOIN sl ON sl.subscriber_id = sub.id
                    LEFT JOIN mo ON mo.subscriber_id = sub.id
                    LEFT JOIN dl ON dl.subscriber_id = sub.id
                )
                SELECT cohort,
                    COUNT(*) FILTER (WHERE high_flag) AS high,
                    COUNT(*) FILTER (WHERE NOT high_flag AND med_flag) AS med,
                    COUNT(*) FILTER (WHERE NOT high_flag AND NOT med_flag AND low_flag) AS low
                FROM flags
                GROUP BY cohort
            """),
            {"cids": county_ids},
        ).fetchall()
    except Exception as exc:
        logger.warning("_fetch_engagement_by_cohort failed: %s", exc)
        return {}

    out: dict = {}
    for cohort, high, med, low in rows:
        high, med, low = int(high or 0), int(med or 0), int(low or 0)
        if high == 0 and med == 0 and low == 0:
            out[cohort] = "—"
        elif high >= med and high >= low and high > 0:
            out[cohort] = "High 🟢"
        elif med >= low and med > 0:
            out[cohort] = "Medium 🟡"
        else:
            out[cohort] = "Low 🔴"
    return out


def _fetch_cohort_extended(session, county_ids: list[str]) -> list[dict]:
    """Cohort with Avg Age, Avg MRR/Sub, Churn Rate, Engagement, NPS."""
    today = date.today()
    engagement_by_cohort = _fetch_engagement_by_cohort(session, county_ids)
    rows = session.execute(
        text("""
            SELECT CASE
                WHEN founding_member = true THEN 'Founding (1st month)'
                WHEN created_at >= now() - INTERVAL '30 days' THEN 'New (30d)'
                WHEN created_at >= now() - INTERVAL '90 days' THEN 'Early (2-3 months)'
                ELSE 'Established (3+ months)'
            END AS cohort,
            COUNT(*) AS cnt,
            AVG(EXTRACT(EPOCH FROM (now() - created_at)) / 86400.0) AS avg_age_days,
            AVG(plan_price) AS avg_mrr
            FROM subscribers
            WHERE status = 'active' AND county_id = ANY(:cids)
            GROUP BY 1 ORDER BY 1
        """),
        {"cids": county_ids},
    ).fetchall()

    results = []
    for cohort, cnt, avg_age, avg_mrr in rows:
        cnt = int(cnt or 0)
        avg_age_str = f"{int(avg_age or 0)} days"
        avg_mrr_str = _currency(avg_mrr) if avg_mrr else NA
        churned_in_cohort = 0
        if cohort == "Founding (1st month)":
            churned_in_cohort = int(session.execute(text("SELECT COUNT(*) FROM subscribers WHERE founding_member=true AND churned_at >= now() - INTERVAL '30 days' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar() or 0)
        churn_str = f"{churned_in_cohort / max(1, cnt) * 100:.1f}%" if cnt else NA
        results.append({
            "cohort": cohort,
            "count": cnt,
            "avg_age": avg_age_str,
            "avg_mrr": avg_mrr_str,
            "churn_rate": churn_str,
            "engagement": engagement_by_cohort.get(cohort, "—"),
            "nps": "N/A",
        })
    # Add At-Risk row
    at_risk = int(session.execute(text("SELECT COUNT(DISTINCT us.subscriber_id) FROM user_segments us JOIN subscribers s ON s.id=us.subscriber_id WHERE us.segment='at_risk' AND s.county_id=ANY(:cids) AND s.status='active'"), {"cids": county_ids}).scalar() or 0)
    at_risk_mrr = session.execute(text("SELECT SUM(plan_price) FROM subscribers WHERE id IN (SELECT subscriber_id FROM user_segments WHERE segment='at_risk') AND status='active' AND county_id=ANY(:cids)"), {"cids": county_ids}).scalar()
    if at_risk > 0:
        results.append({
            "cohort": "At-Risk (Inactive 5+ days)",
            "count": at_risk,
            "avg_age": "—",
            "avg_mrr": _currency(at_risk_mrr) if at_risk_mrr else NA,
            "churn_rate": "—",
            "engagement": "Low 🔴",
            "nps": "N/A",
        })
    return results


# ---------------------------------------------------------------------------
# County-specific
# ---------------------------------------------------------------------------

def _fetch_county_snapshot(session, run_date: date, county_id: str) -> dict:
    try:
        tier_counts = _fetch_tier_counts(session, run_date, [county_id])
        active_subs = _fetch_active_subscriber_count(session, [county_id])
        avg_cds = _fetch_avg_cds(session, run_date, [county_id])
        enrich_rate = _fetch_enrichment_rate(session, run_date, [county_id])
        revenue_kpis = _fetch_county_revenue_kpis(session, run_date, county_id, active_subs)
        tiers = _query_tier_snapshot(session, run_date, county_id)
        gp_total = sum(tiers.get(t, 0) for t in GOLD_PLUS_TIERS)

        scraper_row = session.execute(
            text("""
                SELECT COUNT(*) AS total_runs,
                       SUM(CASE WHEN run_success THEN 1 ELSE 0 END) AS ok_runs
                FROM scraper_run_stats WHERE run_date = :today AND county_id = :cid
            """),
            {"today": str(run_date), "cid": county_id},
        ).fetchone()
        total_runs = int(scraper_row[0] or 0)
        ok_runs = int(scraper_row[1] or 0)
        scraper_health = (
            "ok" if total_runs > 0 and ok_runs == total_runs
            else ("caution" if ok_runs > 0 else "fail") if total_runs > 0
            else "na"
        )
        return {
            "county_id": county_id,
            "label": county_id.replace("_", " ").title(),
            "new_leads_today": tier_counts["total"],
            "gold_plus_today": tier_counts["gold_plus"],
            "gold_plus_portfolio": gp_total,
            "ultra_plat_portfolio": tiers.get("Ultra Platinum", 0),
            "plat_portfolio": tiers.get("Platinum", 0),
            "avg_cds": avg_cds,
            "enrichment_pct": enrich_rate,
            "active_subs": active_subs,
            "scraper_health": scraper_health,
            **revenue_kpis,
        }
    except Exception as exc:
        logger.warning("_fetch_county_snapshot failed for %s: %s", county_id, exc)
        return {
            "county_id": county_id, "label": county_id.replace("_", " ").title(),
            "new_leads_today": NA, "gold_plus_today": NA, "gold_plus_portfolio": NA,
            "ultra_plat_portfolio": NA, "plat_portfolio": NA, "avg_cds": NA,
            "enrichment_pct": NA, "active_subs": NA, "scraper_health": "na",
            "subscriber_mrr": NA, "subscriber_mrr_trend": "flat",
            "subscriber_churn": NA, "subscriber_churn_trend": "flat",
            "lead_delivery_sla": NA, "lead_delivery_sla_ok": False, "lead_delivery_sla_trend": "flat",
            "synthflow_booking_rate": NA, "synthflow_booking_rate_trend": "flat",
        }


# ---------------------------------------------------------------------------
# More unified query functions
# ---------------------------------------------------------------------------

def _fetch_enrichment_breakdown(session, run_date: date, county_ids: list[str]) -> dict:
    try:
        rows = session.execute(
            text("""
                WITH gp AS (
                    SELECT DISTINCT ON (ds.property_id) ds.property_id
                    FROM distress_scores ds
                    WHERE ds.county_id = ANY(:county_ids)
                      AND date(ds.score_date) = :today AND ds.lead_tier = ANY(:tiers)
                    ORDER BY ds.property_id, ds.score_date DESC
                )
                SELECT
                    COUNT(DISTINCT gp.property_id) AS total,
                    COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NOT NULL AND ec.email IS NOT NULL THEN gp.property_id END) AS both,
                    COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NOT NULL AND ec.email IS NULL     THEN gp.property_id END) AS phone_only,
                    COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NULL     AND ec.email IS NOT NULL THEN gp.property_id END) AS email_only
                FROM gp LEFT JOIN enriched_contacts ec
                    ON ec.property_id = gp.property_id AND ec.match_success = true
            """),
            {"county_ids": county_ids, "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchone()
        if not rows or not rows[0]:
            return {"total": 0, "both": 0, "phone_only": 0, "email_only": 0, "neither": 0}
        total, both, phone_only, email_only = int(rows[0]), int(rows[1] or 0), int(rows[2] or 0), int(rows[3] or 0)
        neither = max(0, total - both - phone_only - email_only)
        return {
            "total": total, "both": both, "phone_only": phone_only,
            "email_only": email_only, "neither": neither,
            "both_pct": _pct(both, total), "phone_pct": _pct(phone_only, total),
            "email_pct": _pct(email_only, total), "neither_pct": _pct(neither, total),
        }
    except Exception as exc:
        logger.warning("_fetch_enrichment_breakdown failed: %s", exc)
        return {"total": 0, "both": 0, "phone_only": 0, "email_only": 0, "neither": 0, "error": True}


def _fetch_enrichment_by_tier(session, run_date: date, county_ids: list[str]) -> list:
    try:
        rows = session.execute(
            text("""
                WITH gp AS (
                    SELECT DISTINCT ON (ds.property_id) ds.property_id, ds.lead_tier
                    FROM distress_scores ds
                    WHERE ds.county_id = ANY(:county_ids)
                      AND date(ds.score_date) = :today AND ds.lead_tier = ANY(:tiers)
                    ORDER BY ds.property_id, ds.score_date DESC
                )
                SELECT gp.lead_tier,
                    COUNT(DISTINCT gp.property_id) AS total,
                    COUNT(DISTINCT CASE WHEN ec.mobile_phone IS NOT NULL THEN gp.property_id END) AS with_phone,
                    COUNT(DISTINCT CASE WHEN ec.email IS NOT NULL        THEN gp.property_id END) AS with_email
                FROM gp LEFT JOIN enriched_contacts ec
                    ON ec.property_id = gp.property_id AND ec.match_success = true
                GROUP BY gp.lead_tier
                ORDER BY CASE gp.lead_tier
                    WHEN 'Ultra Platinum' THEN 1 WHEN 'Platinum' THEN 2 WHEN 'Gold' THEN 3 ELSE 4 END
            """),
            {"county_ids": county_ids, "today": str(run_date), "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchall()
        result = []
        for tier, total, with_phone, with_email in rows:
            total, with_phone, with_email = int(total or 0), int(with_phone or 0), int(with_email or 0)
            result.append({"tier": tier, "total": total, "with_phone": with_phone,
                           "phone_pct": _pct(with_phone, total), "with_email": with_email,
                           "email_pct": _pct(with_email, total)})
        return result
    except Exception as exc:
        logger.warning("_fetch_enrichment_by_tier failed: %s", exc)
        return []


def _fetch_enrichment_by_source(session, county_ids: list[str]) -> list:
    since = date.today() - timedelta(days=7)
    try:
        rows = session.execute(
            text("""
                SELECT source, COUNT(*) AS total,
                       SUM(CASE WHEN match_success THEN 1 ELSE 0 END) AS matched,
                       SUM(CASE WHEN mobile_phone IS NOT NULL THEN 1 ELSE 0 END) AS with_phone,
                       SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS with_email
                FROM enriched_contacts
                WHERE county_id = ANY(:county_ids) AND enriched_at >= :since
                GROUP BY source ORDER BY total DESC
            """),
            {"county_ids": county_ids, "since": str(since)},
        ).fetchall()
        result = []
        for source, total, matched, with_phone, with_email in rows:
            total, matched = int(total or 0), int(matched or 0)
            with_phone, with_email = int(with_phone or 0), int(with_email or 0)
            result.append({
                "source": SOURCE_LABELS.get(source, source), "total": total,
                "matched": matched, "match_pct": _pct(matched, total),
                "with_phone": with_phone,
                "phone_pct": _pct(with_phone, matched) if matched else NA,
                "with_email": with_email,
                "email_pct": _pct(with_email, matched) if matched else NA,
            })
        return result
    except Exception as exc:
        logger.warning("_fetch_enrichment_by_source failed: %s", exc)
        return []


def _fetch_subscriber_metrics(session, county_ids: list[str]) -> dict:
    week_start = date.today() - timedelta(days=date.today().weekday())
    try:
        rows = session.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'active')                              AS active,
                    COUNT(*) FILTER (WHERE status = 'grace')                               AS grace,
                    COUNT(*) FILTER (WHERE status IN ('churned','cancelled'))               AS churned_total,
                    COUNT(*) FILTER (WHERE status = 'paused')                              AS paused,
                    COUNT(*) FILTER (WHERE status = 'disputed')                            AS disputed,
                    COUNT(*) FILTER (WHERE founding_member = true AND status = 'active')   AS founding_active,
                    COUNT(*) FILTER (WHERE created_at >= :week_start AND status = 'active') AS new_this_week,
                    COUNT(*) FILTER (WHERE auto_mode_enabled = true AND status = 'active') AS auto_mode
                FROM subscribers WHERE county_id = ANY(:county_ids)
            """),
            {"county_ids": county_ids, "week_start": str(week_start)},
        ).fetchone()
        if not rows:
            return {"error": True}
        active, grace, churned_total, paused, disputed, founding_active, new_this_week, auto_mode = (
            int(x or 0) for x in rows
        )
        at_risk = int(session.execute(
            text("""
                SELECT COUNT(DISTINCT us.subscriber_id)
                FROM user_segments us JOIN subscribers s ON s.id = us.subscriber_id
                WHERE us.segment = 'at_risk' AND s.county_id = ANY(:county_ids) AND s.status = 'active'
            """),
            {"county_ids": county_ids},
        ).scalar() or 0)

        mrr_val = session.execute(
            text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar()
        mrr = _currency(mrr_val) if mrr_val else NA

        churned_30d = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE churned_at >= now() - INTERVAL '30 days' AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar() or 0)
        churn_denom = active + churned_30d
        churn_rate_30d = f"{churned_30d / churn_denom * 100:.1f}%" if churn_denom else NA

        trial_signups = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE is_trial = true AND created_at >= now() - INTERVAL '30 days' AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar() or 0)

        ltv_val = session.execute(
            text("""
                SELECT AVG(plan_price * GREATEST(1, EXTRACT(EPOCH FROM (now() - created_at)) / 2592000.0))
                FROM subscribers
                WHERE status = 'active' AND plan_price IS NOT NULL AND plan_price > 0
                  AND county_id = ANY(:cids)
            """),
            {"cids": county_ids},
        ).scalar()
        avg_ltv = _currency(ltv_val) if ltv_val else NA

        synthflow_zips = sorted({
            zip_code
            for county_id in county_ids
            for zip_code in _county_zip_list(county_id)
        })
        if synthflow_zips:
            sf_row = session.execute(
                text("""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE outcome = 'demo_requested') AS bookings
                    FROM synthflow_calls
                    WHERE call_date >= current_date - 30
                      AND zip_code = ANY(:zips)
                """),
                {"zips": synthflow_zips},
            ).fetchone()
        else:
            sf_row = None
        sf_total = int(sf_row[0] or 0) if sf_row else 0
        sf_bookings = int(sf_row[1] or 0) if sf_row else 0
        synthflow_booking_rate = _pct(sf_bookings, sf_total) if sf_total else NA

        return {
            "active": active, "grace": grace, "churned_total": churned_total,
            "paused": paused, "disputed": disputed, "founding_active": founding_active,
            "new_this_week": new_this_week, "auto_mode": auto_mode, "at_risk": at_risk,
            "mrr": mrr, "trial_signups": trial_signups,
            "churn_rate_30d": churn_rate_30d, "avg_ltv": avg_ltv,
            "synthflow_booking_rate": synthflow_booking_rate,
            "synthflow_calls_30d": sf_total, "synthflow_bookings_30d": sf_bookings,
        }
    except Exception as exc:
        logger.warning("_fetch_subscriber_metrics failed: %s", exc)
        return {"error": True, "active": 0, "mrr": NA}


def _fetch_cohort_breakdown(session, county_ids: list[str]) -> list:
    try:
        rows = session.execute(
            text("""
                SELECT CASE WHEN founding_member = true THEN 'Founding'
                            WHEN created_at >= now() - INTERVAL '30 days' THEN 'New (30d)'
                            ELSE 'Established' END AS cohort,
                       COUNT(*) AS cnt
                FROM subscribers WHERE status = 'active' AND county_id = ANY(:county_ids)
                GROUP BY 1 ORDER BY cnt DESC
            """),
            {"county_ids": county_ids},
        ).fetchall()
        return [{"cohort": cohort, "count": int(cnt)} for cohort, cnt in rows]
    except Exception as exc:
        logger.warning("_fetch_cohort_breakdown failed: %s", exc)
        return []


def _fetch_subs_by_vertical(session, county_ids: list[str]) -> list:
    try:
        rows = session.execute(
            text("""
                SELECT vertical, COUNT(*) AS cnt FROM subscribers
                WHERE status = 'active' AND county_id = ANY(:county_ids)
                GROUP BY vertical ORDER BY cnt DESC
            """),
            {"county_ids": county_ids},
        ).fetchall()
        return [{"vertical": v, "count": int(cnt)} for v, cnt in rows]
    except Exception as exc:
        logger.warning("_fetch_subs_by_vertical failed: %s", exc)
        return []


def _fetch_cora_decision_stats(session, run_date: date) -> list:
    try:
        rows = session.execute(
            text("""
                SELECT
                    graph_name,
                    COUNT(*)                                                        AS total,
                    SUM(CASE WHEN terminal_status = 'completed'  THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN terminal_status = 'aborted'    THEN 1 ELSE 0 END) AS aborted,
                    SUM(CASE WHEN terminal_status = 'failed'     THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE
                        WHEN terminal_status = 'completed'
                         AND autonomy_class = 'autonomous'
                         AND COALESCE(requires_approval, false) = false
                         AND approved_at IS NULL
                         AND overridden_at IS NULL
                        THEN 1 ELSE 0
                    END) AS autonomous_completed,
                    SUM(tokens_used)  AS tokens,
                    SUM(cost_usd)     AS cost
                FROM agent_decisions
                WHERE date(started_at) = :today
                  AND terminal_status IS NOT NULL
                GROUP BY graph_name
                ORDER BY total DESC
            """),
            {"today": str(run_date)},
        ).fetchall()
        result = []
        for graph_name, total, completed, aborted, failed, autonomous_completed, tokens, cost in rows:
            total = int(total or 0)
            result.append({
                "graph": graph_name,
                "total": total,
                "completed": int(completed or 0),
                "aborted": int(aborted or 0),
                "failed": int(failed or 0),
                "autonomous_pct": _pct(int(autonomous_completed or 0), total),
                "tokens": int(tokens or 0),
                "cost": f"${float(cost or 0):.4f}",
            })
        return result
    except Exception as exc:
        logger.warning("_fetch_cora_decision_stats failed: %s", exc)
        return []


def _fetch_cora_metrics(session, run_date: date) -> dict:
    week_start = run_date - timedelta(days=run_date.weekday())
    result: dict = {
        "autonomy_pct": _fetch_cora_autonomy(session, run_date),
        "api_cost_today": NA, "api_cost_by_service": [], "api_cost_week": NA,
        "ab_active_count": NA, "ab_completed_recent": [],
        "open_incidents_red": 0, "open_incidents_yellow": 0,
        "decision_stats": _fetch_cora_decision_stats(session, run_date),
    }
    try:
        cost_rows = session.execute(
            text("SELECT service, SUM(cost_usd) AS cost FROM api_usage_logs WHERE date(created_at) = :today GROUP BY service ORDER BY cost DESC"),
            {"today": str(run_date)},
        ).fetchall()
        result["api_cost_today"] = f"${sum(float(r[1] or 0) for r in cost_rows):.4f}"
        result["api_cost_by_service"] = [{"service": r[0], "cost": f"${float(r[1] or 0):.4f}"} for r in cost_rows]
        result["api_cost_week"] = f"${float(session.execute(text('SELECT SUM(cost_usd) FROM api_usage_logs WHERE date(created_at) >= :ws'), {'ws': str(week_start)}).scalar() or 0):.4f}"
    except Exception as exc:
        logger.warning("_fetch_cora_metrics api_cost failed: %s", exc)
    try:
        result["ab_active_count"] = int(session.execute(text("SELECT COUNT(*) FROM ab_tests WHERE status = 'active'")).scalar() or 0)
        ab_recent = session.execute(
            text("SELECT test_name, winner, ended_at FROM ab_tests WHERE status = 'completed' AND ended_at >= now() - INTERVAL '30 days' ORDER BY ended_at DESC LIMIT 5"),
        ).fetchall()
        result["ab_completed_recent"] = [{"name": r[0], "winner": r[1] or NA, "ended": str(r[2])[:10] if r[2] else NA} for r in ab_recent]
    except Exception as exc:
        logger.warning("_fetch_cora_metrics ab_tests failed: %s", exc)
    try:
        for sev, cnt in session.execute(text("SELECT severity, COUNT(*) FROM cora_incident WHERE breach_resolved IS NULL GROUP BY severity")).fetchall():
            if sev == "red":
                result["open_incidents_red"] = int(cnt)
            elif sev == "yellow":
                result["open_incidents_yellow"] = int(cnt)
    except Exception as exc:
        logger.warning("_fetch_cora_metrics incidents failed: %s", exc)
    return result


def _human_root_cause(metric_name: str | None, county_id: str | None, stored: str | None) -> str:
    """Human-readable root cause: stored value if present, else a derived fallback."""
    if stored:
        return stored
    metric = (metric_name or "metric").replace("_", " ").title()
    where = f" in {county_id.replace('_', ' ').title()}" if county_id else ""
    return f"{metric} breached threshold{where}"


def _fetch_open_incidents(session) -> list:
    # Prefer the root_cause column (fa050); fall back gracefully if the
    # migration has not yet been applied on this database. The first attempt
    # runs inside a savepoint so a missing-column error doesn't abort the
    # outer transaction (mirrors the begin_nested pattern used in loaders).
    base_cols = "id, metric_name, severity, observed_value, threshold_value, breach_started, action_taken, county_id"

    def _build(rows, has_root):
        return [{
            "id": r[0], "metric": r[1], "severity": r[2],
            "observed": _score_fmt(r[3]), "threshold": _score_fmt(r[4]),
            "started": str(r[5])[:19] if r[5] else NA,
            "action": r[6], "county": r[7] or "—",
            "root_cause": _human_root_cause(r[1], r[7], r[8] if has_root else None),
            "badge": "fail" if r[2] == "red" else "warn",
        } for r in rows]

    order = "ORDER BY CASE severity WHEN 'red' THEN 1 WHEN 'yellow' THEN 2 ELSE 3 END, breach_started"
    try:
        with session.begin_nested():
            rows = session.execute(
                text(f"SELECT {base_cols}, root_cause FROM cora_incident WHERE breach_resolved IS NULL {order}")
            ).fetchall()
        return _build(rows, has_root=True)
    except Exception as exc:
        logger.info("_fetch_open_incidents: root_cause column unavailable, deriving fallback (%s)", exc)
    try:
        rows = session.execute(
            text(f"SELECT {base_cols} FROM cora_incident WHERE breach_resolved IS NULL {order}")
        ).fetchall()
        return _build(rows, has_root=False)
    except Exception as exc:
        logger.warning("_fetch_open_incidents failed: %s", exc)
        return []


def _fetch_zip_hotspots(session, run_date: date, county_ids: list[str], top_n: int = 15) -> list:
    all_zips = []
    for cid in county_ids:
        zips = _build_zip_breakdown(session, run_date, cid, top_n=top_n)
        if not zips:
            continue
        try:
            territory_map = {r[0]: r[1] for r in session.execute(
                text("SELECT DISTINCT ON (zip_code) zip_code, status FROM zip_territories WHERE zip_code = ANY(:zips) AND county_id = :cid ORDER BY zip_code, id DESC"),
                {"zips": [z["zip"] for z in zips], "cid": cid},
            ).fetchall()}
        except Exception:
            territory_map = {}
        for z in zips:
            z["territory_status"] = territory_map.get(z["zip"], "open")
        all_zips.extend(zips)
    all_zips.sort(key=lambda z: z.get("total", 0), reverse=True)
    return all_zips[:top_n]


def _fetch_future_counties(session, active_counties: list[str]) -> list[dict]:
    """Pipeline rows for queued future counties.

    Per county: Signals Density vs Hillsborough (distress_scores COUNT(*) as % of
    Hillsborough), Contractor Count (dbpr_contacts), Addressable ZIPs
    (zip_territories). A final "Other FL Counties" row aggregates any
    non-default, non-active counties present in the data.
    """
    try:
        h_count = int(session.execute(
            text("SELECT COUNT(*) FROM distress_scores WHERE county_id = 'hillsborough'")
        ).scalar() or 0)

        future = [c for c in DEFAULT_FUTURE_COUNTIES if c not in active_counties]
        rows: list[dict] = []
        for cid in future:
            signals = int(session.execute(
                text("SELECT COUNT(*) FROM distress_scores WHERE county_id = :cid"),
                {"cid": cid},
            ).scalar() or 0)
            contractors = int(session.execute(
                text("SELECT COUNT(*) FROM dbpr_contacts WHERE county_id = :cid"),
                {"cid": cid},
            ).scalar() or 0)
            zips = int(session.execute(
                text("SELECT COUNT(*) FROM zip_territories WHERE county_id = :cid"),
                {"cid": cid},
            ).scalar() or 0)
            rows.append({
                "county": cid.replace("_", " ").title(),
                "signals_density": _pct(signals, h_count) if h_count else NA,
                "contractor_count": _num_fmt(contractors),
                "addressable_zips": _num_fmt(zips),
                "est_mrr": NA,
                "status": NA,
                "target_launch": NA,
            })

        # Other FL Counties — aggregate of counties not active and not default future.
        exclude = list({*active_counties, *DEFAULT_FUTURE_COUNTIES})
        other_signals = int(session.execute(
            text("SELECT COUNT(*) FROM distress_scores WHERE NOT (county_id = ANY(:ex))"),
            {"ex": exclude},
        ).scalar() or 0)
        other_contractors = int(session.execute(
            text("SELECT COUNT(*) FROM dbpr_contacts WHERE NOT (county_id = ANY(:ex))"),
            {"ex": exclude},
        ).scalar() or 0)
        other_zips = int(session.execute(
            text("SELECT COUNT(*) FROM zip_territories WHERE NOT (county_id = ANY(:ex))"),
            {"ex": exclude},
        ).scalar() or 0)
        rows.append({
            "county": "Other FL Counties",
            "signals_density": _pct(other_signals, h_count) if h_count else NA,
            "contractor_count": _num_fmt(other_contractors),
            "addressable_zips": _num_fmt(other_zips),
            "est_mrr": NA,
            "status": NA,
            "target_launch": NA,
        })
        return rows
    except Exception as exc:
        logger.warning("_fetch_future_counties failed: %s", exc)
        return [
            {"county": cid.replace("_", " ").title(), "signals_density": NA,
             "contractor_count": NA, "addressable_zips": NA, "est_mrr": NA,
             "status": NA, "target_launch": NA}
            for cid in DEFAULT_FUTURE_COUNTIES if cid not in active_counties
        ]


def _fetch_vertical_avg_cds(session, run_date: date, county_ids: list[str]) -> list:
    try:
        rows = session.execute(
            text("""
                SELECT vertical_scores, final_cds_score
                FROM (
                    SELECT DISTINCT ON (property_id) property_id, lead_tier, vertical_scores, final_cds_score
                    FROM distress_scores
                    WHERE county_id = ANY(:county_ids) AND date(score_date) <= :run_date
                    ORDER BY property_id, score_date DESC
                ) latest
                WHERE lead_tier = ANY(:tiers) AND vertical_scores IS NOT NULL AND final_cds_score IS NOT NULL
            """),
            {"county_ids": county_ids, "run_date": run_date, "tiers": list(GOLD_PLUS_TIERS)},
        ).fetchall()
    except Exception as exc:
        logger.warning("_fetch_vertical_avg_cds failed: %s", exc)
        return []
    vert_scores: dict = defaultdict(list)
    for vs, cds in rows:
        if not vs:
            continue
        best = max(vs, key=lambda k: vs.get(k, 0))
        if best in VERTICAL_DISPLAY:
            vert_scores[best].append(float(cds))
    return [
        {"vertical": label, "count": len(vert_scores.get(key, [])),
         "avg_cds": _score_fmt(sum(vert_scores[key]) / len(vert_scores[key])) if vert_scores.get(key) else NA}
        for key, label in VERTICAL_DISPLAY.items()
    ]


def _signal_impact(source_type: str) -> str:
    """Human-readable impact of a stale/missing signal, from SIGNAL_TO_VERTICALS.

    Each scraper feeds specific buyer verticals' CDS; if it's stale, those
    verticals' scoring degrades. Pure code-level lookup — no DB, no assumptions.
    """
    verticals = SIGNAL_TO_VERTICALS.get(source_type)
    if not verticals:
        return "—"
    labels = [VERTICAL_DISPLAY.get(v, v.replace("_", " ").title()) for v in verticals]
    return "Degrades CDS: " + ", ".join(labels)


def _fetch_data_quality_signals(session, run_date: date, county_ids: list[str]) -> list:
    merged = _merge_signal_freshness_dicts([_build_signal_freshness(session, cid) for cid in county_ids])

    # SCRAPER_ORDER is shared with daily_report; surface divorce_filings here
    # (valid source_type, absent from that list) without mutating the import.
    # Staleness comes from scraper_run_stats freshness.
    extras = [s for s in DASHBOARD_EXTRA_SOURCES if s not in SCRAPER_ORDER]
    for src in extras:
        if src in merged:
            continue
        try:
            last = session.execute(
                text("SELECT MAX(run_date) FROM scraper_run_stats WHERE source_type = :st AND county_id = ANY(:cids)"),
                {"st": src, "cids": county_ids},
            ).scalar()
            merged[src] = (run_date - last).days if last else None
        except Exception:
            merged[src] = None

    return [
        {
            "source": source_type.replace("_", " ").title(),
            "days_stale": merged.get(source_type) if merged.get(source_type) is not None else "Never",
            "health": _staleness_badge(merged.get(source_type)),
            "impact": _signal_impact(source_type),
        }
        for source_type in [*SCRAPER_ORDER, *extras]
        if source_type not in ENRICHMENT_ONLY
    ]


# ---------------------------------------------------------------------------
# Combined liens & judgements
# ---------------------------------------------------------------------------

def _fetch_liens_judgements_combined(
    session, run_date: date, county_ids: list[str]
) -> tuple[list[dict], int, int]:
    try:
        db_rows = session.execute(
            text("""
                SELECT source_type,
                       SUM(total_scraped)  AS scraped,
                       SUM(matched)        AS matched,
                       BOOL_AND(run_success)         AS all_ok,
                       BOOL_OR(NOT run_success)       AS any_fail,
                       STRING_AGG(
                           CASE WHEN NOT run_success THEN
                               county_id || ': ' || COALESCE(error_message, 'error')
                           END, '; '
                       ) AS error_detail
                FROM scraper_run_stats
                WHERE run_date = :run_date
                  AND county_id = ANY(:county_ids)
                  AND source_type = ANY(:lien_types)
                GROUP BY source_type
            """),
            {
                "run_date": str(run_date),
                "county_ids": county_ids,
                "lien_types": list(LIEN_SUBTYPES),
            },
        ).fetchall()
    except Exception as exc:
        logger.warning("_fetch_liens_judgements_combined failed: %s", exc)
        return [], 0, 0

    by_type = {r[0]: r for r in db_rows}
    result = []
    for st in LIEN_SUBTYPE_ORDER:
        r = by_type.get(st)
        if r:
            scraped = int(r[1] or 0)
            matched = int(r[2] or 0)
            ok = True if r[3] else (False if r[4] else None)
        else:
            scraped, matched, ok = 0, 0, None
        result.append({
            "label":   st.replace("_", " ").title(),
            "scraped": scraped,
            "matched": matched if scraped > 0 else None,
            "ok":      ok,
        })
    for st, r in by_type.items():
        if st not in LIEN_SUBTYPE_ORDER:
            scraped = int(r[1] or 0)
            result.append({
                "label":   st.replace("_", " ").title(),
                "scraped": scraped,
                "matched": int(r[2] or 0) if scraped > 0 else None,
                "ok":      True if r[3] else (False if r[4] else None),
            })

    total_scraped = sum(r["scraped"] for r in result)
    total_matched = sum(r["matched"] or 0 for r in result)
    return result, total_scraped, total_matched


# ---------------------------------------------------------------------------
# Signal-table ingest (replaces scraper_run_stats while stats table is stale)
# ---------------------------------------------------------------------------

# (label, src_key, table, filter_clause, unmatched_srcs, is_event, is_critical)
# src_key=None  → no signal table (show N/A, still check unmatched_srcs)
# is_event=True → zero records today is expected (fires, floods…)
# is_critical=True → stale data escalates to ESCALATE instead of Check
_INGEST_SOURCES: list[tuple] = [
    ("Permits (Building/Elec.)", "permits",         "building_permits",  "is_enforcement_permit = false", ["permits", "roofing_permits"],       False, False),
    ("Stop Work Orders",         "stop_work",       "building_permits",  "is_enforcement_permit = true",  [],                                   False, False),
    ("Judgments & Liens",        "legal_and_liens", "legal_and_liens",   None,                            ["judgments", "liens", "lis_pendens"], False, True),
    ("Lis Pendens / Foreclosures","foreclosures",   "foreclosures",      None,                            ["foreclosures"],                     False, False),
    ("Tax Delinquencies",        "tax_delinq",      "tax_delinquencies", None,                            ["tax_delinquencies"],                False, True),
    ("Code Violations",          "code_viols",      "code_violations",   None,                            ["violations"],                       False, False),
    ("Insurance Claims",         "ins_claims",      "incidents",         "incident_type = 'insurance_claim'", ["insurance_claims"],             True,  True),
    ("Fire Incidents",           "fire_inc",        "incidents",         "incident_type = 'Fire'",            ["fire_incidents"],               True,  False),
    ("Flood / Water Damage",     "flood",           "incidents",         "incident_type = 'flood_damage'",    ["flood_damage"],                 True,  False),
    ("Storm Damage",             "storm",           "incidents",         "incident_type = 'storm_damage'",    ["storm_damage"],                 True,  False),
    ("Probate Filings",          "probate",         "legal_proceedings", "record_type = 'Probate'",           ["probate"],                     False, False),
    ("Evictions",                "evictions",       "legal_proceedings", "record_type = 'Eviction'",          ["evictions"],                   False, False),
    ("Bankruptcy",               "bankruptcy",      "legal_proceedings", "record_type = 'Bankruptcy'",        ["bankruptcies"],                False, False),
    ("Divorce Filings",          "divorce",         "legal_proceedings", "record_type = 'Divorce'",           ["divorce_filings"],             False, False),
    ("Deeds",                    "deeds",           "deeds",             None,                                ["deeds"],                       False, False),
]


def _ingest_status(days_stale: int | None, scraped: int, is_event: bool) -> str:
    if days_stale is None:
        return "fail"
    if days_stale > 5:
        return "fail"
    if days_stale > 2:
        return "caution"
    if is_event and scraped == 0:
        return "ok"   # event-based, no events today but scraper ran recently
    return "ok"


def _ingest_quality(days_stale: int | None, match_pct: float | None, is_event: bool, scraped: int) -> str:
    if days_stale is None:
        return "Error"
    if days_stale > 5:
        return "Stale"
    if days_stale > 2:
        return "Moderate"
    if is_event and scraped == 0:
        return "Clean"   # event-based, no events today but fresh
    if match_pct is not None and scraped > 0 and match_pct < 30:
        return "Moderate"
    return "Clean"


def _ingest_action(label: str, days_stale: int | None, scraped: int, is_event: bool, is_critical: bool) -> str:
    if days_stale is None:
        return "ESCALATE" if is_critical else "Check"
    if days_stale > 5:
        return "ESCALATE" if is_critical else "Check"  # stale always beats event-based
    if days_stale > 2:
        return "Check"
    if is_event and scraped == 0:
        return "Source check"   # fresh scraper, but zero events — verify feed is live
    return "—"


def _build_scraper_section_from_signals(
    session, run_date: date, county_id: str,
) -> dict:
    """Signal-table replacement for daily_report._build_scraper_section.

    Returns a dict with rows (all 8 display columns), totals, and a summary block.
    Each source in _INGEST_SOURCES gets today's count + last_update from signal tables.
    Unmatched counts come from unmatched_records.
    """
    params = {"county_id": county_id, "run_date": run_date}

    # Single UNION ALL: today's count + last ever date_added per source
    union_legs = []
    for _label, src_key, table, filt, _um, _ev, _cr in _INGEST_SOURCES:
        if table is None or src_key is None:
            continue
        where = f"county_id = :county_id{' AND ' + filt if filt else ''}"
        union_legs.append(
            f"SELECT '{src_key}' AS src,"
            f" COUNT(*) FILTER (WHERE date_added = :run_date) AS today_cnt,"
            f" MAX(date_added) AS last_upd"
            f" FROM {table} WHERE {where}"
        )

    today_by_src: dict[str, int] = {}
    last_upd_by_src: dict[str, date | None] = {}
    if union_legs:
        try:
            for src, cnt, lu in session.execute(
                text("\nUNION ALL\n".join(union_legs)), params
            ).fetchall():
                today_by_src[src] = int(cnt or 0)
                last_upd_by_src[src] = lu.date() if lu and hasattr(lu, "date") else lu
        except Exception as exc:
            logger.warning("_build_scraper_section_from_signals signal query failed %s: %s", county_id, exc)

    # Unmatched counts for today
    raw_unmatched: dict[str, int] = {}
    try:
        for st, cnt in session.execute(text("""
            SELECT source_type, COUNT(*) FROM unmatched_records
             WHERE county_id = :county_id
               AND date(date_added AT TIME ZONE 'UTC') = :run_date
               AND match_status IN ('unmatched', 'pending_review')
             GROUP BY source_type
        """), params).fetchall():
            raw_unmatched[st] = int(cnt or 0)
    except Exception as exc:
        logger.warning("_build_scraper_section_from_signals unmatched query failed %s: %s", county_id, exc)

    rows: list[dict] = []
    for label, src_key, _table, _filt, unmatched_srcs, is_event, is_critical in _INGEST_SOURCES:
        if src_key is not None:
            matched   = today_by_src.get(src_key, 0)
            last_upd  = last_upd_by_src.get(src_key)
            days_stale: int | None = (run_date - last_upd).days if last_upd else None
        else:
            matched, last_upd, days_stale = 0, None, None

        unmatched  = sum(raw_unmatched.get(st, 0) for st in unmatched_srcs)
        scraped    = matched + unmatched
        match_pct_row = (matched / scraped * 100) if scraped > 0 else None
        scraped_cell = scraped if (scraped > 0 or src_key is not None) else None
        days_stale_cell = days_stale if days_stale is not None else ("N/A" if src_key is None else "Never")
        quality = _ingest_quality(days_stale, match_pct_row, is_event, scraped)
        action = _ingest_action(label, days_stale, scraped, is_event, is_critical)

        rows.append({
            "label":       label,
            "scraped":     scraped_cell,
            "scraped_display": "-" if scraped_cell is None else scraped_cell,
            "scraped_class": "na" if scraped_cell in (None, 0) else "",
            "matched":     matched   if scraped > 0 else None,
            "unmatched":   unmatched if scraped > 0 else None,
            "match_rate":  f"{match_pct_row:.1f}%" if match_pct_row is not None else "—",
            "last_update": str(last_upd) if last_upd else ("N/A" if src_key is None else "Never"),
            "days_stale":  days_stale_cell,
            "days_stale_class": "na" if days_stale_cell in ("Never", "N/A") else "",
            "status":      _ingest_status(days_stale, scraped, is_event),
            "quality":     quality,
            "quality_class": {
                "Error": "q-error",
                "Stale": "q-stale",
                "Moderate": "q-moderate",
            }.get(quality, "q-clean"),
            "action":      action,
            "action_class": {
                "ESCALATE": "a-escalate",
                "Check": "a-check",
                "Source check": "a-source-check",
            }.get(action, ""),
            "is_event":    is_event,
        })

    total_matched   = sum(r["matched"]   or 0 for r in rows)
    total_unmatched = sum(r["unmatched"] or 0 for r in rows)
    total_scraped   = total_matched + total_unmatched
    match_pct       = (total_matched / total_scraped * 100) if total_scraped else 0.0

    # Data quality score: fraction of trackable sources that are "Clean" × 10
    trackable = [r for r in rows if r["quality"] not in ("N/A",) and r["last_update"] not in ("N/A", "Never")]
    clean     = [r for r in trackable if r["quality"] == "Clean"]
    dq_score  = round(len(clean) / max(len(trackable), 1) * 10, 1) if trackable else 0.0

    return {
        "rows":          rows,
        "total_scraped": total_scraped,
        "total_matched": total_matched,
        "match_pct":     match_pct,
        "errors":        [],
        "summary": {
            "data_quality_score": dq_score,
            "unmatched_count":    total_unmatched,
            "unmatched_pct":      f"{total_unmatched / total_scraped * 100:.1f}%" if total_scraped else "—",
            "match_target_ok":    match_pct >= 85,
        },
    }


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def collect_dashboard_data(session, run_date: date) -> dict:
    """Build the full Jinja2 context — unified across all active counties."""
    active_counties = _fetch_active_counties(session)

    # Collect and merge county-specific imported functions
    tier_snapshots, tier_histories, vert_bds, vert_cts = [], [], [], []
    phone_covs, sig_comps, sig_freshs = [], [], []
    for cid in active_counties:
        tier_snapshots.append(_query_tier_snapshot(session, run_date, cid))
        tier_histories.append(_build_tier_history(session, run_date, cid))
        vert_bds.append(_build_vertical_breakdown(session, run_date, cid))
        vert_cts.append(_build_vertical_tier_crosstab(session, run_date, cid))
        phone_covs.append(_build_phone_coverage(session, run_date, cid))
        sig_comps.append(_build_signal_composition(session, run_date, cid))
        sig_freshs.append(_build_signal_freshness(session, cid))

    tiers = _merge_tier_snapshots(tier_snapshots)
    tier_history = _merge_tier_histories(tier_histories)
    vertical_breakdown = _merge_vertical_breakdowns(vert_bds)
    vertical_tier_crosstab = _merge_vertical_crosstabs(vert_cts)
    phone_coverage = _merge_phone_coverages(phone_covs)
    signal_composition = _merge_signal_compositions(sig_comps)
    signal_freshness = _merge_signal_freshness_dicts(sig_freshs)

    # Section 4 — per-county scraper groups (signal tables, not scraper_run_stats)
    scraper_groups = []
    for cid in active_counties:
        result = _build_scraper_section_from_signals(session, run_date, cid)
        scraper_groups.append({
            "county_id": cid,
            "display":   cid.replace("_", " ").title(),
            "rows":      result["rows"],
            "total":     result["total_scraped"],
            "matched":   result["total_matched"],
            "match_pct": f"{result['match_pct']:.1f}%",
            "errors":    result["errors"],
            "summary":   result["summary"],
        })
    total_scraped = sum(g["total"] for g in scraper_groups)
    total_matched = sum(g["matched"] for g in scraper_groups)
    combined_match_pct = (total_matched / total_scraped * 100) if total_scraped else 0.0
    all_errors = [e for g in scraper_groups for e in g["errors"]]

    # Section 1 — Executive Summary
    tier_counts_today = _fetch_tier_counts(session, run_date, active_counties)
    avg_cds_today = _fetch_avg_cds(session, run_date, active_counties)
    active_subs = _fetch_active_subscriber_count(session, active_counties)
    enrichment_rate = _fetch_enrichment_rate(session, run_date, active_counties)
    cora_autonomy = _fetch_cora_autonomy(session, run_date)
    subscriber_metrics = _fetch_subscriber_metrics(session, active_counties)

    exec_summary = {
        "leads_today": tier_counts_today["total"],
        "gold_plus_today": tier_counts_today["gold_plus"],
        "gold_plus_pct": tier_counts_today["gold_plus_pct"],
        "gold_plus_portfolio": sum(tiers.get(t, 0) for t in GOLD_PLUS_TIERS),
        "ultra_plat_portfolio": tiers.get("Ultra Platinum", 0),
        "plat_portfolio": tiers.get("Platinum", 0),
        "avg_cds": avg_cds_today,
        "active_subscribers": active_subs,
        "enrichment_rate": enrichment_rate,
        "cora_autonomy": cora_autonomy,
        "mrr": subscriber_metrics.get("mrr", NA),
        "churn_rate_30d": subscriber_metrics.get("churn_rate_30d", NA),
        "total_scraped": total_scraped,
        "total_matched": total_matched,
        "match_pct": f"{combined_match_pct:.1f}%",
        "scraper_errors": all_errors,
    }

    # NEW: Exec summary table rows for model layout
    exec_summary_rows = _fetch_exec_summary_rows(session, run_date, active_counties)

    # NEW: Tier 7-day avg and this week total
    tier_7day_avg = _fetch_tier_7day_avg(session, run_date, active_counties)
    tier_this_week = _fetch_tier_this_week_total(session, run_date, active_counties)

    # Section 2 — per-county snapshots
    county_snapshots = [_fetch_county_snapshot(session, run_date, c) for c in active_counties]
    primary_county = "hillsborough" if "hillsborough" in active_counties else active_counties[0]
    county_performance_snapshots = [
        _fetch_county_performance_table(session, run_date, c, "primary" if c == primary_county else "expansion")
        for c in active_counties
    ]

    # Enrichment leverage
    try:
        enrich_float = float(enrichment_rate.rstrip("%")) if enrichment_rate != NA else None
        enrich_gap_pp = round(75.0 - enrich_float, 1) if enrich_float is not None else None
        enrich_gap = _score_fmt(enrich_gap_pp) if enrich_gap_pp is not None else NA
    except (ValueError, TypeError):
        enrich_gap_pp = None
        enrich_gap = NA

    # Real cost to close the enrichment gap (no assumptions): the Gold+ leads
    # without a phone × actual cost per successful enrichment (enrichment_usage_logs).
    try:
        crow = session.execute(
            text("SELECT SUM(cost_cents), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM enrichment_usage_logs")
        ).fetchone()
        cost_per_lead = (int(crow[0] or 0) / 100.0) / int(crow[1]) if crow and crow[1] else None
    except Exception as exc:
        logger.warning("enrichment gap cost query failed: %s", exc)
        cost_per_lead = None
    gap_leads = int(phone_coverage.get("without_phone", 0) or 0)
    enrichment_cost_per_lead = f"${cost_per_lead:.2f}" if cost_per_lead else NA
    enrichment_gap_cost = _currency(gap_leads * cost_per_lead) if cost_per_lead else NA

    # NEW data
    scraper_ingest_details = _fetch_scraper_ingest_details(session, run_date, active_counties)
    enrichment_by_tier_ext = _fetch_enrichment_both_pct_by_tier(session, run_date, active_counties)
    enrichment_source_detail = _fetch_enrichment_source_detailed(session, active_counties)
    vertical_performance_ext = _fetch_vertical_performance_extended(session, run_date, active_counties)
    revenue_table_ext = _fetch_revenue_table_extended(session, run_date, active_counties, subscriber_metrics)
    cohort_ext = _fetch_cohort_extended(session, active_counties)
    conversion_funnel = _fetch_conversion_funnel(session, run_date, active_counties)
    future_counties = _fetch_future_counties(session, active_counties)

    # Demo-data guard: seeded subscribers carry a 'seed_demo_' stripe_customer_id
    # prefix (scripts/seed_demo_metrics.py). When present, the report renders a
    # TEST-DATA banner so subscriber / revenue / Cora figures are never mistaken
    # for production — there are no real subscribers yet.
    try:
        demo_subscriber_count = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE stripe_customer_id LIKE 'seed_demo_%'")
        ).scalar() or 0)
        # Total active subscribers (the figure the report's numbers derive from).
        # While seed data is present there are no production subscribers, so ALL
        # active subs are non-production — the banner cites this total, not just
        # the seed-tagged count, so a reader can't infer the rest are real.
        demo_active_total = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND county_id=ANY(:cids)"),
            {"cids": active_counties},
        ).scalar() or 0)
    except Exception as exc:
        logger.warning("demo-data guard query failed: %s", exc)
        demo_subscriber_count = 0
        demo_active_total = 0
    demo_data_active = demo_subscriber_count > 0

    
    return {
        "run_date": str(run_date),
        "active_counties": active_counties,
        "county_label": " + ".join(c.replace("_", " ").title() for c in active_counties),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "exec_summary": exec_summary,
        "exec_summary_rows": exec_summary_rows,
        "demo_data_active": demo_data_active,
        "demo_subscriber_count": demo_subscriber_count,
        "demo_active_total": demo_active_total,
        "county_snapshots": county_snapshots,
        "county_performance_snapshots": county_performance_snapshots,
        "future_counties": future_counties,
        "tiers": tiers,
        "tier_counts_today": tier_counts_today["counts"],
        "tier_history": tier_history,
        "weekly_tier_history": _fetch_weekly_tier_history(session, run_date, active_counties),
        "tier_7day_avg": tier_7day_avg,
        "tier_this_week": tier_this_week,
        "scraper_groups": scraper_groups,
        "scraper_ingest_details": scraper_ingest_details,
        "scraper_errors": all_errors,
        "total_scraped": total_scraped,
        "total_matched": total_matched,
        "match_pct": f"{combined_match_pct:.1f}%",
        "enrichment_breakdown": _fetch_enrichment_breakdown(session, run_date, active_counties),
        "enrichment_by_tier": _fetch_enrichment_by_tier(session, run_date, active_counties),
        "enrichment_by_tier_ext": enrichment_by_tier_ext,
        "enrichment_by_source": _fetch_enrichment_by_source(session, active_counties),
        "enrichment_source_detail": enrichment_source_detail,
        "phone_coverage": phone_coverage,
        "vertical_breakdown": vertical_breakdown,
        "vertical_tier_crosstab": vertical_tier_crosstab,
        "vertical_avg_cds": _fetch_vertical_avg_cds(session, run_date, active_counties),
        "vertical_performance_ext": vertical_performance_ext,
        "signal_composition": signal_composition,
        "subs_by_vertical": _fetch_subs_by_vertical(session, active_counties),
        "subscriber_metrics": subscriber_metrics,
        "revenue_table_ext": revenue_table_ext,
        "conversion_funnel": conversion_funnel,
        "cohort_breakdown": _fetch_cohort_breakdown(session, active_counties),
        "cohort_ext": cohort_ext,
        "cora_metrics": _fetch_cora_metrics(session, run_date),
        "open_incidents": _fetch_open_incidents(session),
        "quality_signals": _fetch_data_quality_signals(session, run_date, active_counties),
        "signal_freshness": signal_freshness,
        "zip_hotspots": _fetch_zip_hotspots(session, run_date, active_counties),
        "enrichment_rate": enrichment_rate,
        "enrichment_gap": enrich_gap,
        "enrichment_gap_float": enrich_gap_pp,
        "enrichment_gap_cost": enrichment_gap_cost,
        "enrichment_cost_per_lead": enrichment_cost_per_lead,
        "NA": NA,
    }
        

# ---------------------------------------------------------------------------
# Render + Export
# ---------------------------------------------------------------------------

def render_html(context: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    env.globals.update(pct=_pct, badge=_badge, staleness_badge=_staleness_badge,
                       score_fmt=_score_fmt, NA=NA, currency=_currency, num_fmt=_num_fmt,
                       quality_label=_quality_label, status_emoji=_status_emoji,
                       trend_arrow=_trend_arrow)
    return env.get_template("daily_dashboard.html").render(**context)


def html_to_pdf(html: str, output_path: Path) -> None:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html, wait_until="domcontentloaded")
        page.pdf(
            path=str(output_path), format="A4", landscape=True,
            print_background=True,
            margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
        )
        browser.close()


def prune_old_dashboards(directory: Path, keep_days: int = RETENTION_DAYS) -> int:
    if not directory.exists():
        return 0
    cutoff = date.today() - timedelta(days=keep_days)
    removed = 0
    for f in directory.glob("*.pdf"):
        try:
            if date.fromisoformat(f.name[:10]) < cutoff:
                f.unlink()
                removed += 1
        except (ValueError, OSError):
            pass
    return removed


def send_dashboard_email(pdf_path: Path, run_date: date) -> bool:
    """Email the dashboard PDF to every address in REPORT_RECIPIENTS.

    Returns True if at least one send succeeded.
    No-ops (returns False) when SMTP or REPORT_RECIPIENTS are not configured.
    """
    settings = get_settings()
    raw = settings.report_recipients or ""
    recipients = [r.strip() for r in raw.split(",") if r.strip()]
    if not recipients:
        logger.info("[daily_dashboard] REPORT_RECIPIENTS not set — skipping email")
        return False

    subject = f"Forced Action Daily Dashboard — {run_date.strftime('%b %d, %Y')}"
    body_text = (
        f"Daily operations dashboard for {run_date.strftime('%B %d, %Y')}.\n"
        f"Full 10-section PDF is attached.\n\n"
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    body_html = f"""
    <div style="font-family:sans-serif;background:#0f172a;padding:32px;border-radius:8px">
      <h2 style="color:#f59e0b;margin:0 0 8px">Forced Action Daily Dashboard</h2>
      <p style="color:#94a3b8;margin:0 0 16px;font-size:15px">
        {run_date.strftime("%B %d, %Y")}
      </p>
      <p style="color:#e2e8f0;font-size:14px">
        The full 10-section operations dashboard is attached as a PDF.
      </p>
      <p style="color:#475569;font-size:12px;margin-top:24px">
        Generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
      </p>
    </div>
    """

    cc_raw = settings.report_cc_recipients or ""
    cc_recipients = [r.strip() for r in cc_raw.split(",") if r.strip()]
    cc_set = {e.lower() for e in cc_recipients}

    any_ok = False
    for recipient in recipients:
        cc = cc_recipients if recipient.lower() not in cc_set else None
        ok = send_email(
            to=recipient,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachments=[pdf_path],
            cc=cc,
        )
        if ok:
            logger.info("[daily_dashboard] emailed dashboard to %s", recipient)
            any_ok = True
        else:
            logger.warning("[daily_dashboard] failed to email dashboard to %s", recipient)

    return any_ok


def generate_dashboard_pdf(
    run_date: date | None = None,
    wl_client: dict | None = None,
) -> Path:
    """
    Generate the daily dashboard PDF.

    Args:
        run_date:  Date for the report (defaults to today).
        wl_client: Optional white-label branding dict:
                   {company_name, logo_url, primary_color, secondary_color}
                   When provided the PDF substitutes WL branding for Forced Action
                   branding and writes to a WL-specific output path.
    """
    run_date = run_date or date.today()
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    if wl_client:
        slug = wl_client.get("company_slug", "wl")
        output_path = DASHBOARD_DIR / f"{run_date}_{slug}_daily_dashboard.pdf"
    else:
        output_path = DASHBOARD_DIR / f"{run_date}_forced_action_daily_dashboard.pdf"

    logger.info("[daily_dashboard] generating for %s (wl=%s)", run_date, bool(wl_client))
    with get_db_context() as session:
        context = collect_dashboard_data(session, run_date)

    if wl_client:
        context["wl_branding"] = {
            "company_name":   wl_client.get("display_name") or wl_client.get("company_name", ""),
            "logo_url":       wl_client.get("logo_url"),
            "primary_color":  wl_client.get("primary_color") or "#fbbf24",
            "secondary_color": wl_client.get("secondary_color") or "#a855f7",
        }
    else:
        context["wl_branding"] = None

    html = render_html(context)
    html_to_pdf(html, output_path)
    pruned = prune_old_dashboards(DASHBOARD_DIR)
    logger.info("[daily_dashboard] saved → %s (pruned %d old files)", output_path, pruned)
    return output_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate daily operations dashboard PDF")
    parser.add_argument("--date", help="YYYY-MM-DD (defaults to today)")
    parser.add_argument(
        "--send", action="store_true",
        help="Email the PDF to REPORT_RECIPIENTS after generating",
    )
    args = parser.parse_args()
    run_date = date.fromisoformat(args.date) if args.date else date.today()
    path = generate_dashboard_pdf(run_date=run_date)
    if args.send:
        send_dashboard_email(path, run_date)
    print(f"Dashboard written to: {path}")
