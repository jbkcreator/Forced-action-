"""
Supplier Intelligence Foundation — report generation engine (fa067).

Generates per-section data for a supplier report. Safe sections always return
data. Gated sections check DATA_READINESS_THRESHOLDS before generating; if the
threshold is not met they return {"status": "insufficient_data", ...} instead
of fake or unreliable numbers.

Phase 2 sections (recommendations) are always N/A in Phase 1.

All DB I/O via sa_text (repo convention).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.supplier_intel_config import (
    DATA_READINESS_THRESHOLDS,
    GATED_SECTIONS,
    PHASE2_SECTIONS,
    REPORT_SECTIONS,
    SAFE_SECTIONS,
)

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _period_start(days: int = 30) -> date:
    return (datetime.now(timezone.utc) - timedelta(days=days)).date()


def _insufficient(section_key: str, current: int, minimum: int) -> dict:
    return {
        "status": "insufficient_data",
        "section": section_key,
        "current_count": current,
        "minimum_required": minimum,
        "message": (
            f"This section requires at least {minimum} data points. "
            f"Currently {current} available. Check back when more activity is recorded."
        ),
    }


# ── Data readiness check ───────────────────────────────────────────────────────

def check_data_readiness(
    counties: list[str],
    verticals: list[str],
    db: Session,
) -> dict:
    """Return per-section readiness: {'section_key': {'ready': bool, 'current': N, 'min': M}}."""
    result: dict[str, dict] = {}
    min_deals = DATA_READINESS_THRESHOLDS["min_deals_for_benchmarks"]
    min_subs = DATA_READINESS_THRESHOLDS["min_subs_for_trend"]

    # Count closed_won deals in territory
    deal_row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM deal_outcomes
        WHERE pipeline_stage = 'closed_won'
          AND (:counties IS NULL OR county_id = ANY(CAST(:counties AS VARCHAR[])))
          AND (:verticals IS NULL OR trade_vertical = ANY(CAST(:verticals AS VARCHAR[])))
    """), {
        "counties": counties or None,
        "verticals": verticals or None,
    }).first()
    deal_count = int(deal_row.c or 0)

    # Count active subscribers in territory
    sub_row = db.execute(sa_text("""
        SELECT COUNT(DISTINCT id) AS c FROM subscribers
        WHERE status = 'active'
          AND tier NOT IN ('free','data_only')
          AND (:counties IS NULL OR county_id = ANY(CAST(:counties AS VARCHAR[])))
          AND (:verticals IS NULL OR vertical = ANY(CAST(:verticals AS VARCHAR[])))
    """), {
        "counties": counties or None,
        "verticals": verticals or None,
    }).first()
    sub_count = int(sub_row.c or 0)

    for section in REPORT_SECTIONS:
        key = section["key"]
        if section["availability"] == "safe":
            result[key] = {"ready": True, "current": None, "min": None}
        elif section["availability"] == "phase2":
            result[key] = {"ready": False, "reason": "phase2", "current": None, "min": None}
        elif key == "closed_deal_benchmarks":
            result[key] = {"ready": deal_count >= min_deals, "current": deal_count, "min": min_deals}
        elif key == "contractor_demand":
            result[key] = {"ready": sub_count >= min_subs, "current": sub_count, "min": min_subs}
        else:
            result[key] = {"ready": False, "reason": "unknown_gate"}

    return result


# ── Safe sections ──────────────────────────────────────────────────────────────

def _section_market_activity(counties: list[str], verticals: list[str], db: Session) -> dict:
    since_30 = _period_start(30)
    since_60 = _period_start(60)

    row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE ds.score_date::date >= :since_30)  AS leads_30d,
            COUNT(*) FILTER (WHERE ds.score_date::date >= :since_60)  AS leads_60d,
            COUNT(DISTINCT ds.property_id)                             AS unique_properties
        FROM distress_scores ds
        WHERE ds.qualified = true
          AND (:counties IS NULL OR ds.county_id = ANY(CAST(:counties AS VARCHAR[])))
    """), {"since_30": since_30, "since_60": since_60, "counties": counties or None}).first()

    # Signal breakdown
    signal_rows = db.execute(sa_text("""
        SELECT
            SUM(CASE WHEN f.id IS NOT NULL THEN 1 ELSE 0 END) AS foreclosures,
            SUM(CASE WHEN t.id IS NOT NULL THEN 1 ELSE 0 END) AS tax_delinquencies,
            SUM(CASE WHEN c.id IS NOT NULL THEN 1 ELSE 0 END) AS code_violations
        FROM properties p
        LEFT JOIN foreclosures f ON f.property_id = p.id
        LEFT JOIN tax_delinquencies t ON t.property_id = p.id
        LEFT JOIN code_violations c ON c.property_id = p.id
        WHERE (:counties IS NULL OR p.county_id = ANY(CAST(:counties AS VARCHAR[])))
        LIMIT 1
    """), {"counties": counties or None}).first()

    leads_30 = int(row.leads_30d or 0)
    leads_60 = int(row.leads_60d or 0)
    period_change_pct = (
        round((leads_30 - leads_60) / leads_60 * 100, 1) if leads_60 > 0 else None
    )

    return {
        "status": "ok",
        "leads_last_30d": leads_30,
        "leads_last_60d": leads_60,
        "period_change_pct": period_change_pct,
        "unique_properties": int(row.unique_properties or 0),
        "signals": {
            "foreclosures": int(signal_rows.foreclosures or 0) if signal_rows else 0,
            "tax_delinquencies": int(signal_rows.tax_delinquencies or 0) if signal_rows else 0,
            "code_violations": int(signal_rows.code_violations or 0) if signal_rows else 0,
        },
    }


def _section_top_zips(counties: list[str], verticals: list[str], db: Session) -> dict:
    min_leads = DATA_READINESS_THRESHOLDS["min_leads_for_zip_map"]
    rows = db.execute(sa_text("""
        SELECT p.zip, COUNT(*) AS lead_count,
               COUNT(*) FILTER (WHERE ds.lead_tier IN ('Ultra Platinum','Platinum')) AS premium_count
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE ds.qualified = true
          AND (:counties IS NULL OR ds.county_id = ANY(CAST(:counties AS VARCHAR[])))
        GROUP BY p.zip
        HAVING COUNT(*) >= :min_leads
        ORDER BY lead_count DESC
        LIMIT 10
    """), {"counties": counties or None, "min_leads": min_leads}).fetchall()

    return {
        "status": "ok",
        "top_zips": [
            {"zip": r.zip, "lead_count": r.lead_count, "premium_count": r.premium_count}
            for r in rows
        ],
    }


def _section_signal_movement(counties: list[str], verticals: list[str], db: Session) -> dict:
    periods = {"30d": _period_start(30), "60d": _period_start(60), "90d": _period_start(90)}

    # Foreclosure trend — use date_added (the load date); filing_date is a Date column
    # without time component so FILTER works directly.
    fc_rows = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE f.date_added >= :d30)  AS fc_30d,
            COUNT(*) FILTER (WHERE f.date_added >= :d60)  AS fc_60d,
            COUNT(*) FILTER (WHERE f.date_added >= :d90)  AS fc_90d
        FROM foreclosures f
        JOIN properties p ON p.id = f.property_id
        WHERE (:counties IS NULL OR p.county_id = ANY(CAST(:counties AS VARCHAR[])))
    """), {
        "d30": periods["30d"], "d60": periods["60d"], "d90": periods["90d"],
        "counties": counties or None,
    }).first()

    return {
        "status": "ok",
        "foreclosures": {
            "30d": int(fc_rows.fc_30d or 0) if fc_rows else 0,
            "60d": int(fc_rows.fc_60d or 0) if fc_rows else 0,
            "90d": int(fc_rows.fc_90d or 0) if fc_rows else 0,
        },
        "note": "Signal counts are raw activity counts, not de-duplicated properties.",
    }


def _section_property_tier_dist(counties: list[str], verticals: list[str], db: Session) -> dict:
    rows = db.execute(sa_text("""
        SELECT lead_tier, COUNT(*) AS c
        FROM distress_scores ds
        WHERE ds.qualified = true
          AND ds.lead_tier IS NOT NULL
          AND (:counties IS NULL OR ds.county_id = ANY(CAST(:counties AS VARCHAR[])))
        GROUP BY lead_tier
        ORDER BY c DESC
    """), {"counties": counties or None}).fetchall()

    total = sum(r.c for r in rows)
    tiers = {r.lead_tier: r.c for r in rows}
    return {
        "status": "ok",
        "total": total,
        "distribution": {
            tier: {"count": tiers.get(tier, 0), "pct": round(tiers.get(tier, 0) / total * 100, 1) if total else 0}
            for tier in ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
        },
    }


def _section_trade_coverage(counties: list[str], verticals: list[str], db: Session) -> dict:
    rows = db.execute(sa_text("""
        SELECT s.vertical, COUNT(DISTINCT sl.id) AS lead_count
        FROM subscribers s
        JOIN sent_leads sl ON sl.subscriber_id = s.id
        WHERE s.status = 'active'
          AND s.tier NOT IN ('free','data_only')
          AND (:counties IS NULL OR s.county_id = ANY(CAST(:counties AS VARCHAR[])))
          AND (:verticals IS NULL OR s.vertical = ANY(CAST(:verticals AS VARCHAR[])))
        GROUP BY s.vertical
        ORDER BY lead_count DESC
    """), {"counties": counties or None, "verticals": verticals or None}).fetchall()

    return {
        "status": "ok",
        "verticals": [{"vertical": r.vertical, "lead_count": r.lead_count} for r in rows],
    }


# ── Gated sections ─────────────────────────────────────────────────────────────

def _section_closed_deal_benchmarks(
    counties: list[str], verticals: list[str], db: Session
) -> dict:
    min_deals = DATA_READINESS_THRESHOLDS["min_deals_for_benchmarks"]
    count_row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM deal_outcomes
        WHERE pipeline_stage = 'closed_won'
          AND (:counties IS NULL OR county_id = ANY(CAST(:counties AS VARCHAR[])))
    """), {"counties": counties or None}).first()
    count = int(count_row.c or 0)
    if count < min_deals:
        return _insufficient("closed_deal_benchmarks", count, min_deals)

    row = db.execute(sa_text("""
        SELECT
            AVG(deal_amount)      AS avg_deal_size,
            AVG(days_to_close)    AS avg_days_to_close,
            COUNT(*) FILTER (WHERE pipeline_stage = 'closed_won') AS won,
            COUNT(*) AS total
        FROM deal_outcomes
        WHERE (:counties IS NULL OR county_id = ANY(CAST(:counties AS VARCHAR[])))
          AND (:verticals IS NULL OR trade_vertical = ANY(CAST(:verticals AS VARCHAR[])))
    """), {"counties": counties or None, "verticals": verticals or None}).first()

    total = int(row.total or 0)
    won = int(row.won or 0)
    return {
        "status": "ok",
        "avg_deal_size_usd": float(row.avg_deal_size or 0),
        "avg_days_to_close": float(row.avg_days_to_close or 0),
        "close_rate_pct": round(won / total * 100, 1) if total > 0 else 0,
        "sample_size": count,
    }


def _section_contractor_demand(
    counties: list[str], verticals: list[str], db: Session
) -> dict:
    min_subs = DATA_READINESS_THRESHOLDS["min_subs_for_trend"]
    count_row = db.execute(sa_text("""
        SELECT COUNT(DISTINCT id) AS c FROM subscribers
        WHERE status = 'active' AND tier NOT IN ('free','data_only')
          AND (:counties IS NULL OR county_id = ANY(CAST(:counties AS VARCHAR[])))
    """), {"counties": counties or None}).first()
    count = int(count_row.c or 0)
    if count < min_subs:
        return _insufficient("contractor_demand", count, min_subs)

    row = db.execute(sa_text("""
        SELECT COUNT(DISTINCT sl.subscriber_id) AS active_buyers,
               COUNT(sl.id) AS total_leads_consumed
        FROM sent_leads sl
        JOIN subscribers s ON s.id = sl.subscriber_id
        WHERE s.status = 'active'
          AND sl.sent_at >= NOW() - INTERVAL '30 days'
          AND (:counties IS NULL OR s.county_id = ANY(CAST(:counties AS VARCHAR[])))
    """), {"counties": counties or None}).first()

    return {
        "status": "ok",
        "active_subscriber_count": count,
        "active_buyers_30d": int(row.active_buyers or 0),
        "leads_consumed_30d": int(row.total_leads_consumed or 0),
    }


# ── Phase 2 placeholder ────────────────────────────────────────────────────────

def _section_recommendations() -> dict:
    return {
        "status": "phase2",
        "message": (
            "AI-driven supplier opportunity recommendations are coming in Phase 2. "
            "This section will activate when sufficient deal outcome data is validated."
        ),
    }


# ── Main generate function ────────────────────────────────────────────────────

SECTION_GENERATORS = {
    "market_activity":       _section_market_activity,
    "top_zips":              _section_top_zips,
    "signal_movement":       _section_signal_movement,
    "property_tier_dist":    _section_property_tier_dist,
    "trade_coverage":        _section_trade_coverage,
    "closed_deal_benchmarks": _section_closed_deal_benchmarks,
    "contractor_demand":     _section_contractor_demand,
}


def generate_report(
    account_id: int,
    county_id: str,
    counties: list[str],
    verticals: list[str],
    db: Session,
) -> dict:
    """Generate all report sections. Returns {section_key: data_or_insufficient}."""
    readiness = check_data_readiness(counties, verticals, db)
    sections: dict[str, Any] = {}

    for section_meta in REPORT_SECTIONS:
        key = section_meta["key"]
        try:
            if key == "recommendations":
                sections[key] = _section_recommendations()
            elif section_meta["availability"] == "phase2":
                sections[key] = {"status": "phase2", "message": "Coming in Phase 2."}
            elif section_meta["availability"] == "safe":
                gen = SECTION_GENERATORS.get(key)
                sections[key] = gen(counties, verticals, db) if gen else {"status": "ok"}
            else:
                # gated — check readiness first
                r = readiness.get(key, {})
                if r.get("ready"):
                    gen = SECTION_GENERATORS.get(key)
                    sections[key] = gen(counties, verticals, db) if gen else {"status": "ok"}
                else:
                    sections[key] = _insufficient(key, r.get("current", 0), r.get("min", 0))
        except Exception as exc:
            logger.warning("[supplier-report] section %s failed: %s", key, exc, exc_info=True)
            sections[key] = {"status": "error", "message": "Section generation failed."}

    return {
        "sections": sections,
        "data_readiness_snapshot": readiness,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_start": _period_start(30).isoformat(),
        "period_end": date.today().isoformat(),
        "county_id": county_id,
    }
