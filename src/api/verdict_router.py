"""
M6 — Truth Engine verdict read API (spec §8B getVerdict).

Admin-gated: verdicts are an internal grading artefact. Account-scoped lead
access is the Lead Delivery (M10) concern, not this surface.
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.truth_engine import grade_prospect

router = APIRouter(prefix="/api/admin", tags=["truth_engine"])


def _parse_prospect_id(prospect_id: str) -> str:
    try:
        return str(uuid.UUID(prospect_id))
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail="prospect_id must be a UUID")


@router.get("/verdicts/{prospect_id}")
def get_verdict(
    prospect_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Return the latest verdict for a prospect (spec §8B)."""
    pid = _parse_prospect_id(prospect_id)
    row = db.execute(sa_text("""
        SELECT grade, contributing_factors, routed_channel, contactability_flag
        FROM verdicts
        WHERE prospect_id = CAST(:pid AS uuid)
        ORDER BY created_at DESC
        LIMIT 1
    """), {"pid": pid}).mappings().first()

    if row is None:
        raise HTTPException(status_code=404, detail="verdict not found")

    return {
        "grade": row["grade"],
        "contributing_factors": row["contributing_factors"],
        "routed_channel": row["routed_channel"],
        "contactability_flag": row["contactability_flag"],
    }


@router.post("/verdicts/{prospect_id}/grade")
def post_grade(
    prospect_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Force an on-demand grade for a prospect and persist the verdict."""
    pid = _parse_prospect_id(prospect_id)
    result = grade_prospect(db, pid, actor=f"admin:{_admin.get('sub', 'unknown')}")

    if result is None:
        raise HTTPException(status_code=404, detail="prospect not found")
    if result.get("held"):
        raise HTTPException(status_code=409, detail="held: no CDS score for this prospect")

    db.commit()
    return result
