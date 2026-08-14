"""
Territory scarcity API (Block 12 — conversion & retention).

Exposes county-level ZIP inventory pressure so the dashboard can render a
truthful scarcity bar ("34619 is one of 12 open ZIPs in Pinellas; 35 locked")
instead of a hard-coded number. Counts are derived from the existing
`ZipTerritory.status` — no seat capacity, no new schema.

Prefix: /api/scarcity
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.api.deps import ZIP_RE, VALID_VERTICALS, get_db
from src.services.territory_scarcity import county_scarcity, zip_vertical_status

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scarcity", tags=["scarcity"])


class CountyScarcityResponse(BaseModel):
    zip_code: str
    zip_status: Optional[str]
    county_id: str
    county_name: str
    vertical: Optional[str]
    open_count: int
    locked_count: int
    grace_count: int
    total_count: int


class ZipScarcityResponse(BaseModel):
    zip_code: str
    vertical: str
    status: str


@router.get("/county", response_model=CountyScarcityResponse)
def get_county_scarcity(
    zip: str = Query(..., description="5-digit ZIP to derive the county from"),
    vertical: Optional[str] = Query(None, description="Optional vertical to scope inventory to"),
    db: Session = Depends(get_db),
):
    """Return open/locked ZIP counts for the county the given ZIP belongs to."""
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="Invalid ZIP: must be 5 digits")
    if vertical is not None and vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail="Invalid vertical")

    result = county_scarcity(db, zip, vertical)
    if result is None:
        raise HTTPException(status_code=404, detail="ZIP not found in any territory")
    return result


@router.get("/zip", response_model=ZipScarcityResponse)
def get_zip_scarcity(
    zip: str = Query(..., description="5-digit ZIP to check"),
    vertical: str = Query(..., description="Vertical to scope the ZIP to"),
    db: Session = Depends(get_db),
):
    """Return availability for one ZIP x vertical territory."""
    if not ZIP_RE.match(zip):
        raise HTTPException(status_code=400, detail="Invalid ZIP: must be 5 digits")
    if vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail="Invalid vertical")

    result = zip_vertical_status(db, zip, vertical)
    if result is None:
        raise HTTPException(status_code=404, detail="ZIP x vertical not found in territory inventory")
    return {
        "zip_code": result["zip_code"],
        "vertical": result["vertical"],
        "status": result["status"],
    }
