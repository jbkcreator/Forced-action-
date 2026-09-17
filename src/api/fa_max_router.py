"""FA Max borrower intelligence API.

Routes:
  GET /api/fa-max/persons/{person_id}/profile — return the stored buy-box /
      velocity / next-need profile for a person, or 404 if not yet computed.

Auth: admin JWT required (same HS256 token issued by POST /api/admin/login).
This endpoint exposes internal borrower intelligence (predicted need, intent
evidence, property preferences) and must never be public.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.borrower_profile_service import get_person_profile

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fa-max", tags=["fa-max"])


@router.get("/persons/{person_id}/profile")
def get_borrower_profile(
    person_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
) -> dict:
    """Return the stored FA Max borrower profile for *person_id*.

    Requires an admin JWT. 404 if no profile has been computed yet.
    """
    profile = get_person_profile(db, person_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found for this person.")
    return profile
