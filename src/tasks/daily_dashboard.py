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

from src.core.database import get_db_context
from src.tasks.daily_report import (
    ENRICHMENT_ONLY,
    GOLD_PLUS_TIERS,
    SCRAPER_ORDER,
    VERTICAL_DISPLAY,
    _build_phone_coverage,
    _build_scraper_section,
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

# Lien and judgment subtypes that are common to every county.
# These are stripped from per-county scraper rows and merged into one
# unified "Liens & Judgements" section so the county tables don't repeat
# the same subtype list for Hillsborough, Pinellas, etc.
LIEN_SUBTYPES: frozenset[str] = frozenset({
    "judgments", "lis_pendens",
    "lien_ml", "lien_tcl", "lien_hoa", "lien_ccl", "lien_tl", "lien_unknown",
})
LIEN_SUBTYPE_ORDER = [
    "judgments", "lis_pendens",
    "lien_ml", "lien_tcl", "lien_hoa", "lien_ccl", "lien_tl", "lien_unknown",
]
# Pre-computed label set for fast membership tests against row["label"]
LIEN_LABELS: frozenset[str] = frozenset(st.replace("_", " ").title() for st in LIEN_SUBTYPES)

SOURCE_LABELS = {
    "batch_skip_tracing": "BatchData (BST)",
    "idi": "IDI Fallback",
    "pdl": "PDL",
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


# ---------------------------------------------------------------------------
# County discovery
# ---------------------------------------------------------------------------

def _fetch_active_counties(session) -> list[str]:
    """Return county_ids where is_active=true. Falls back to ['hillsborough']."""
    try:
        rows = session.execute(
            text("SELECT county_id FROM counties WHERE is_active = true ORDER BY county_id")
        ).fetchall()
        return [r[0] for r in rows] if rows else ["hillsborough"]
    except Exception as exc:
        logger.warning("_fetch_active_counties failed: %s", exc)
        return ["hillsborough"]


# ---------------------------------------------------------------------------
# Merge helpers for results from county-specific imported functions
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
            if not isinstance(data, dict):  # skip _total and other scalar sentinels
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
                merged[src] = None  # never run → worst case
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
        while d <= run_date:
            key = str(d)
            counts = by_day.get(key, {t: 0 for t in ("Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze")})
            gp = sum(counts.get(t, 0) for t in GOLD_PLUS_TIERS)
            result.append({"date": key, "counts": counts, "gold_plus": gp})
            d += timedelta(days=1)
        return result
    except Exception as exc:
        logger.warning("_fetch_weekly_tier_history failed: %s", exc)
        return []


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
    """County-scoped commercial KPIs for the Section 2 county tiles."""
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
            "subscriber_mrr": NA,
            "subscriber_mrr_trend": "flat",
            "subscriber_churn": NA,
            "subscriber_churn_trend": "flat",
            "lead_delivery_sla": NA,
            "lead_delivery_sla_ok": False,
            "lead_delivery_sla_trend": "flat",
            "synthflow_booking_rate": NA,
            "synthflow_booking_rate_trend": "flat",
        }


def _count_distress_scores(
    session,
    county_id: str,
    start_date: date,
    end_date: date,
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
    metric: str,
    today: str,
    this_week: str,
    avg_7d: str,
    monthly: str,
    target: str = "—",
    trend: str = "flat",
    status: str | None = None,
    notes: str | None = None,
) -> dict:
    return {
        "metric": metric,
        "today": today,
        "this_week": this_week,
        "avg_7d": avg_7d,
        "monthly": monthly,
        "target": target,
        "trend": trend,
        "status": status,
        "notes": notes,
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

    leads_today, leads_week, leads_7d, leads_month, leads_prev = counts()
    gold_today, gold_week, gold_7d, gold_month, gold_prev = counts("gold_plus")
    up_today, up_week, up_7d, up_month, up_prev = counts("up_plat")

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


def _fetch_cora_autonomy(session) -> str:
    try:
        row = session.execute(
            text("""
                SELECT data_json FROM learning_cards
                WHERE card_type = 'autonomy_summary'
                ORDER BY card_date DESC LIMIT 1
            """),
        ).fetchone()
        if row and row[0] and row[0].get("autonomous_pct") is not None:
            return f"{float(row[0]['autonomous_pct']):.1f}%"
        return NA
    except Exception as exc:
        logger.warning("_fetch_cora_autonomy failed: %s", exc)
        return NA


# ---------------------------------------------------------------------------
# County-specific (called once per county for Section 2)
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

        # MRR — sum of plan_price for active subscribers
        mrr_val = session.execute(
            text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar()
        mrr = _currency(mrr_val) if mrr_val else NA

        # 30-day churn rate: churned in window / (active now + churned in window)
        churned_30d = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE churned_at >= now() - INTERVAL '30 days' AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar() or 0)
        churn_denom = active + churned_30d
        churn_rate_30d = f"{churned_30d / churn_denom * 100:.1f}%" if churn_denom else NA

        # Trial signups (last 30 days)
        trial_signups = int(session.execute(
            text("SELECT COUNT(*) FROM subscribers WHERE is_trial = true AND created_at >= now() - INTERVAL '30 days' AND county_id = ANY(:cids)"),
            {"cids": county_ids},
        ).scalar() or 0)

        # Avg LTV = avg(plan_price × months subscribed) for active subscribers with a price
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

        # Synthflow booking rate (last 30 days) — demo_requested / total calls
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
                    SUM(CASE WHEN was_autonomous = true          THEN 1 ELSE 0 END) AS autonomous,
                    SUM(tokens_used)  AS tokens,
                    SUM(cost_usd)     AS cost
                FROM agent_decisions
                WHERE date(started_at) = :today
                GROUP BY graph_name
                ORDER BY total DESC
            """),
            {"today": str(run_date)},
        ).fetchall()
        result = []
        for graph_name, total, completed, aborted, failed, autonomous, tokens, cost in rows:
            total = int(total or 0)
            result.append({
                "graph": graph_name,
                "total": total,
                "completed": int(completed or 0),
                "aborted": int(aborted or 0),
                "failed": int(failed or 0),
                "autonomous_pct": _pct(int(autonomous or 0), total),
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
        "autonomy_pct": _fetch_cora_autonomy(session),
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


def _fetch_open_incidents(session) -> list:
    try:
        rows = session.execute(
            text("""
                SELECT id, metric_name, severity, observed_value, threshold_value,
                       breach_started, action_taken, county_id
                FROM cora_incident WHERE breach_resolved IS NULL
                ORDER BY CASE severity WHEN 'red' THEN 1 WHEN 'yellow' THEN 2 ELSE 3 END, breach_started
            """),
        ).fetchall()
        return [{
            "id": r[0], "metric": r[1], "severity": r[2],
            "observed": _score_fmt(r[3]), "threshold": _score_fmt(r[4]),
            "started": str(r[5])[:19] if r[5] else NA,
            "action": r[6], "county": r[7] or "—",
            "badge": "fail" if r[2] == "red" else "warn",
        } for r in rows]
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


def _fetch_data_quality_signals(session, run_date: date, county_ids: list[str]) -> list:
    merged = _merge_signal_freshness_dicts([_build_signal_freshness(session, cid) for cid in county_ids])
    return [
        {
            "source": source_type.replace("_", " ").title(),
            "days_stale": merged.get(source_type) if merged.get(source_type) is not None else "Never",
            "health": _staleness_badge(merged.get(source_type)),
        }
        for source_type in SCRAPER_ORDER
        if source_type not in ENRICHMENT_ONLY
    ]


# ---------------------------------------------------------------------------
# Combined liens & judgements (unified across all active counties)
# ---------------------------------------------------------------------------

def _fetch_liens_judgements_combined(
    session, run_date: date, county_ids: list[str]
) -> tuple[list[dict], int, int]:
    """Return (rows, total_scraped, total_matched) for lien/judgment subtypes
    summed across all active counties.  Used for the unified S4 sub-table."""
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
    # Catch any unknown lien subtypes that arrived (e.g. new portal categories)
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

    # Section 4 — per-county scraper groups
    scraper_groups = []
    for cid in active_counties:
        rows, total, matched, pct_val, errors = _build_scraper_section(session, run_date, cid)
        scraper_groups.append({
            "county_id": cid,
            "display": cid.replace("_", " ").title(),
            "rows": rows,
            "total": total,
            "matched": matched,
            "match_pct": f"{pct_val:.1f}%",
            "errors": errors or [],
        })
    total_scraped = sum(g["total"] for g in scraper_groups)
    total_matched = sum(g["matched"] for g in scraper_groups)
    combined_match_pct = (total_matched / total_scraped * 100) if total_scraped else 0.0
    all_errors = [e for g in scraper_groups for e in g["errors"]]

    # Strip lien/judgment subtype rows from each county's table, then append
    # one aggregated "Liens & Judgements" row with that county's summed totals.
    for grp in scraper_groups:
        grp["rows"] = [r for r in grp["rows"] if r["label"] not in LIEN_LABELS]
        lien_rows_cty, lien_total_cty, lien_matched_cty = _fetch_liens_judgements_combined(
            session, run_date, [grp["county_id"]]
        )
        any_fail = any(r["ok"] is False for r in lien_rows_cty)
        all_ok = lien_rows_cty and all(r["ok"] is True for r in lien_rows_cty if r["ok"] is not None)
        grp["rows"].append({
            "label": "Liens & Judgements",
            "scraped": lien_total_cty,
            "matched": lien_matched_cty if lien_total_cty > 0 else None,
            "ok": False if any_fail else (True if all_ok else None),
        })

    # Section 1 — Executive Summary
    tier_counts_today = _fetch_tier_counts(session, run_date, active_counties)
    avg_cds_today = _fetch_avg_cds(session, run_date, active_counties)
    active_subs = _fetch_active_subscriber_count(session, active_counties)
    enrichment_rate = _fetch_enrichment_rate(session, run_date, active_counties)
    cora_autonomy = _fetch_cora_autonomy(session)
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

    # Section 2 — per-county snapshots
    county_snapshots = [_fetch_county_snapshot(session, run_date, c) for c in active_counties]
    primary_county = "hillsborough" if "hillsborough" in active_counties else active_counties[0]
    county_performance_snapshots = [
        _fetch_county_performance_table(
            session,
            run_date,
            c,
            "primary" if c == primary_county else "expansion",
        )
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

    return {
        "run_date": str(run_date),
        "active_counties": active_counties,
        "county_label": " + ".join(c.replace("_", " ").title() for c in active_counties),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "exec_summary": exec_summary,
        "county_snapshots": county_snapshots,
        "county_performance_snapshots": county_performance_snapshots,
        "tiers": tiers,
        "tier_history": tier_history,
        "weekly_tier_history": _fetch_weekly_tier_history(session, run_date, active_counties),
        "scraper_groups": scraper_groups,
        "scraper_errors": all_errors,
        "total_scraped": total_scraped,
        "total_matched": total_matched,
        "match_pct": f"{combined_match_pct:.1f}%",
        "enrichment_breakdown": _fetch_enrichment_breakdown(session, run_date, active_counties),
        "enrichment_by_tier": _fetch_enrichment_by_tier(session, run_date, active_counties),
        "enrichment_by_source": _fetch_enrichment_by_source(session, active_counties),
        "phone_coverage": phone_coverage,
        "vertical_breakdown": vertical_breakdown,
        "vertical_tier_crosstab": vertical_tier_crosstab,
        "vertical_avg_cds": _fetch_vertical_avg_cds(session, run_date, active_counties),
        "signal_composition": signal_composition,
        "subs_by_vertical": _fetch_subs_by_vertical(session, active_counties),
        "subscriber_metrics": subscriber_metrics,
        "cohort_breakdown": _fetch_cohort_breakdown(session, active_counties),
        "cora_metrics": _fetch_cora_metrics(session, run_date),
        "open_incidents": _fetch_open_incidents(session),
        "quality_signals": _fetch_data_quality_signals(session, run_date, active_counties),
        "signal_freshness": signal_freshness,
        "zip_hotspots": _fetch_zip_hotspots(session, run_date, active_counties),
        "enrichment_rate": enrichment_rate,
        "enrichment_gap": enrich_gap,
        "enrichment_gap_float": enrich_gap_pp,
        "NA": NA,
    }


# ---------------------------------------------------------------------------
# Render + Export
# ---------------------------------------------------------------------------

def render_html(context: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    env.globals.update(pct=_pct, badge=_badge, staleness_badge=_staleness_badge,
                       score_fmt=_score_fmt, NA=NA)
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


def generate_dashboard_pdf(run_date: date | None = None) -> Path:
    run_date = run_date or date.today()
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DASHBOARD_DIR / f"{run_date}_forced_action_daily_dashboard.pdf"
    logger.info("[daily_dashboard] generating for %s", run_date)
    with get_db_context() as session:
        context = collect_dashboard_data(session, run_date)
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
    args = parser.parse_args()
    path = generate_dashboard_pdf(run_date=date.fromisoformat(args.date) if args.date else None)
    print(f"Dashboard written to: {path}")
