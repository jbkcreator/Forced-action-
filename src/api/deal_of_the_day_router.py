"""
Deal-of-the-day router — T-B12-07.

Public endpoint powering the daily exclusive-unlock surface: today's
top-CDS undelivered lead, 24h scarcity window, standard price (not a
discount). Admin endpoint lets ops trigger/backfill the daily pick.
"""
import logging

from fastapi import APIRouter, Depends
from sqlalchemy.exc import OperationalError
from fastapi import HTTPException

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.deal_of_the_day import get_current_deal, select_deal_of_the_day

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/deal-of-the-day")
def deal_of_the_day(db=Depends(get_db)):
    """
    Returns today's exclusive deal-of-the-day.

    Response: {"status": "live"|"expired"|"empty", "deal": {...}|None,
               "window_start", "window_end", "message": str|None}
    """
    try:
        return get_current_deal(db)
    except OperationalError:
        logger.error("DB error in deal-of-the-day", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail={"error": "service_unavailable", "message": "Database temporarily unavailable"},
        )


@router.post("/api/admin/deal-of-the-day/run")
def run_deal_of_the_day(db=Depends(get_db), _admin: dict = Depends(get_current_admin)):
    """Manually trigger (or backfill) today's deal-of-the-day pick. Idempotent."""
    result = select_deal_of_the_day(db)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail={"error": "no_lead_available", "message": "No qualified undelivered lead available"},
        )
    return result
