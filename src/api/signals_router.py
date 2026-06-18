"""
Signal-Chaining API (Stream H).

Exposes the complete relational distress data for a single property to
external partners and lenders as a structured "distress stack" payload.

Prefix: /api/v1/signals
Authentication: Bearer JWT OR X-API-Key (reuses the white-label data-client
gate, which also enforces an active subscription).

The parser traverses the raw relational tables directly (foreclosures,
code_violations, tax_delinquencies, building_permits) rather than reading the
composite scoring model — the persisted composite score is echoed for context
only and is never re-computed here.
"""

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.white_label_router import _get_data_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------

class SignalChainResponse(BaseModel):
    property_id: int
    county: Optional[str]
    composite_score: Optional[float]
    distress_stack: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Distress stack parser
# ---------------------------------------------------------------------------

def _foreclosure_stage(auction_date, lis_pendens_date, case_status) -> Optional[str]:
    """Derive a coarse foreclosure stage from the available filing dates."""
    if auction_date is not None:
        return "auction_scheduled"
    if lis_pendens_date is not None:
        return "early_lis_pendens"
    return case_status


def build_signal_chain(property_id: int, db: Session) -> Optional[dict]:
    """
    Traverse the raw distress tables for a single property and assemble the
    distress stack. Returns None if the property does not exist.

    Each lookup is a point query on the indexed property_id column.
    """
    prop = db.execute(
        sa_text("""
            SELECT id, county_id, year_built, building_condition
              FROM properties WHERE id = :pid
        """),
        {"pid": property_id},
    ).mappings().first()
    if prop is None:
        return None

    composite_score = db.execute(
        sa_text("""
            SELECT final_cds_score
              FROM distress_scores
             WHERE property_id = :pid
             ORDER BY score_date DESC
             LIMIT 1
        """),
        {"pid": property_id},
    ).scalar()

    distress_stack: list[dict[str, Any]] = []

    foreclosures = db.execute(
        sa_text("""
            SELECT case_number, filing_date, lis_pendens_date,
                   auction_date, case_status
              FROM foreclosures
             WHERE property_id = :pid
             ORDER BY filing_date DESC NULLS LAST
        """),
        {"pid": property_id},
    ).mappings().all()
    for f in foreclosures:
        distress_stack.append({
            "signal_type": "foreclosure",
            "filing_date": f["filing_date"],
            "stage": _foreclosure_stage(
                f["auction_date"], f["lis_pendens_date"], f["case_status"]
            ),
            "details": f["case_number"],
        })

    violations = db.execute(
        sa_text("""
            SELECT violation_type, description, opened_date
              FROM code_violations
             WHERE property_id = :pid
             ORDER BY opened_date DESC NULLS LAST
        """),
        {"pid": property_id},
    ).mappings().all()
    for v in violations:
        distress_stack.append({
            "signal_type": "code_violation",
            "violation_date": v["opened_date"],
            "type": v["violation_type"],
            "details": v["description"],
        })

    tax_rows = db.execute(
        sa_text("""
            SELECT tax_year, total_amount_due
              FROM tax_delinquencies
             WHERE property_id = :pid
             ORDER BY tax_year DESC
        """),
        {"pid": property_id},
    ).mappings().all()
    if tax_rows:
        outstanding_years = [r["tax_year"] for r in tax_rows if r["tax_year"] is not None]
        amounts = [float(r["total_amount_due"]) for r in tax_rows if r["total_amount_due"] is not None]
        distress_stack.append({
            "signal_type": "tax_delinquency",
            "outstanding_years": outstanding_years,
            "balance_due": round(sum(amounts), 2) if amounts else None,
        })

    permits = db.execute(
        sa_text("""
            SELECT permit_type, description, issue_date
              FROM building_permits
             WHERE property_id = :pid
               AND is_enforcement_permit = TRUE
             ORDER BY issue_date DESC NULLS LAST
        """),
        {"pid": property_id},
    ).mappings().all()
    for p in permits:
        distress_stack.append({
            "signal_type": "permit_enforcement",
            "issue_date": p["issue_date"],
            "type": p["permit_type"],
            "details": p["description"],
        })

    fin = db.execute(
        sa_text("""
            SELECT assessed_value_mkt, est_equity, equity_pct
              FROM financials WHERE property_id = :pid
        """),
        {"pid": property_id},
    ).mappings().first()
    appraiser = {
        "assessed_value": float(fin["assessed_value_mkt"]) if fin and fin["assessed_value_mkt"] is not None else None,
        "est_equity": float(fin["est_equity"]) if fin and fin["est_equity"] is not None else None,
        "equity_pct": float(fin["equity_pct"]) if fin and fin["equity_pct"] is not None else None,
        "year_built": prop["year_built"],
        "building_condition": prop["building_condition"],
    }
    if any(v is not None for v in appraiser.values()):
        distress_stack.append({"signal_type": "appraiser", **appraiser})

    return {
        "property_id": prop["id"],
        "county": prop["county_id"],
        "composite_score": float(composite_score) if composite_score is not None else None,
        "distress_stack": distress_stack,
    }


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/chain/{property_id}", response_model=SignalChainResponse)
def get_signal_chain(
    property_id: int,
    client=Depends(_get_data_client),
    db: Session = Depends(get_db),
):
    """Return the full relational distress stack for a single property."""
    result = build_signal_chain(property_id, db)
    if result is None:
        raise HTTPException(404, "Property not found")

    counties = client.get("counties_enabled") or []
    if counties and result["county"] not in counties:
        raise HTTPException(403, "Property not in your enabled counties")

    return result
