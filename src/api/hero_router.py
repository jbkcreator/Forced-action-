"""
Hero router — T-B12-01.

Public, unauthenticated endpoint powering the landing-page hero: visitor
enters a ZIP, sees one real blurred scored deal (or an honest fallback).
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import OperationalError

from src.api.deps import VALID_VERTICALS, ZIP_RE, get_db
from src.services.hero_deal import get_hero_deal

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/hero/scored-deal")
def hero_scored_deal(zip: str, vertical: str = "roofing", db=Depends(get_db)):
    """
    Returns one real, blurred, scored deal for a visitor-entered ZIP.

    Response: {"status": "zip_match"|"nearest"|"empty", "zip_code", "vertical",
               "deal": {...}|None, "nearest_label": str|None, "message": str|None}
    """
    if not ZIP_RE.match(zip):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_zip", "message": "ZIP code must be exactly 5 digits"},
        )
    if vertical not in VALID_VERTICALS:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_vertical", "message": f"vertical must be one of: {sorted(VALID_VERTICALS)}"},
        )

    try:
        return get_hero_deal(zip_code=zip, vertical=vertical, db=db)
    except OperationalError:
        logger.error("DB error in hero scored-deal", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail={"error": "service_unavailable", "message": "Database temporarily unavailable"},
        )
