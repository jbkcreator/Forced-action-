"""
Lead report data assembly (Sprint 4.9).

build_report_data: single entry point → dict consumed by pdf_export.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


class ReportDataError(Exception):
    pass


def build_report_data(property_id: int, db: Session, *, full: bool) -> dict:
    """Assemble report context for one property.

    full=True  → report  (sections 1-5)
    full=False → brief   (sections 1-3)

    Raises ReportDataError if distress row is absent.
    """
    row = db.execute(text("""
        SELECT
            p.id, p.address, p.city, p.state, p.zip, p.parcel_id,
            o.owner_name,
            o.contact_info_confidence, o.contact_info_confidence_score,
            o.contactability_detail,
            f.assessed_value_mkt, f.est_equity, f.equity_pct,
            f.total_lien_amount, f.est_mortgage_bal,
            ds.final_cds_score, ds.lead_tier, ds.vertical_scores,
            ds.distress_types, ds.urgency_level, ds.factor_scores
        FROM properties p
        LEFT JOIN owners o ON o.property_id = p.id
        LEFT JOIN financials f ON f.property_id = p.id
        LEFT JOIN LATERAL (
            SELECT final_cds_score, lead_tier, vertical_scores,
                   distress_types, urgency_level, factor_scores
            FROM distress_scores
            WHERE property_id = p.id
            ORDER BY score_date DESC LIMIT 1
        ) ds ON true
        WHERE p.id = :property_id
    """), {"property_id": property_id}).mappings().first()

    if row is None:
        raise ReportDataError(f"Property {property_id} not found")
    if row["final_cds_score"] is None:
        raise ReportDataError(
            f"No distress score for property {property_id} — cannot generate report"
        )

    result: dict = {
        "header": {
            "address": row["address"],
            "city": row["city"],
            "state": row["state"],
            "zip": row["zip"],
            "parcel_id": row["parcel_id"],
            "owner_name": row["owner_name"],
            "generated_date": datetime.now(timezone.utc).strftime("%B %d, %Y"),
        },
        "distress": {
            "status": "ok",
            "final_cds_score": row["final_cds_score"],
            "lead_tier": row["lead_tier"],
            "distress_types": row["distress_types"] or {},
            "urgency_level": row["urgency_level"],
            "vertical_scores": row["vertical_scores"] or {},
            "factor_scores": row["factor_scores"] or {},
        },
        "equity": _build_equity_section(row),
    }

    if full:
        permits = db.execute(text("""
            SELECT permit_type, issue_date, status, is_enforcement_permit, description
            FROM building_permits
            WHERE property_id = :property_id
            ORDER BY issue_date DESC NULLS LAST
            LIMIT 20
        """), {"property_id": property_id}).mappings().all()

        result["permits"] = {
            "status": "ok" if permits else "empty",
            "rows": [dict(r) for r in permits],
        }
        result["contact_validity"] = _build_contact_section(row)

    return result


def _build_equity_section(row) -> dict:
    caveat = row["est_mortgage_bal"] is None
    assessed = row["assessed_value_mkt"]
    return {
        "status": "ok" if assessed is not None else "empty",
        "assessed_value_mkt": assessed,
        "est_equity": row["est_equity"],
        "equity_pct": row["equity_pct"],
        "total_lien_amount": row["total_lien_amount"],
        "est_mortgage_bal": row["est_mortgage_bal"],
        "caveat": caveat,
    }


def _build_contact_section(row) -> dict:
    detail = row["contactability_detail"] or {}
    score = row["contact_info_confidence_score"]
    label = row["contact_info_confidence"]
    return {
        "status": "ok" if label is not None else "empty",
        "label": label,
        "score_pct": round(score * 100) if score is not None else None,
        "source_count": len(detail.get("sources", [])),
        "rule_fired": detail.get("rule_fired"),
        "corroboration": detail.get("corroboration"),
    }
