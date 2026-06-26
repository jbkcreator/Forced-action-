"""Competitor Benchmark API (Task 4.8).

- GET  /api/competitor-benchmark/targets    high-margin targets (computed live)
- GET  /api/competitor-benchmark/rate-card   Forced Action's own terms
- PUT  /api/competitor-benchmark/rate-card/{product}   edit our terms (admin)

Auth: rate-card edits require admin JWT (same guard as admin_router).
All data access via text() with named binds (no ORM query API).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.competitor_benchmark import (
    compute_benchmark_report,
    load_competitor_rows,
    load_our_terms,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/competitor-benchmark", tags=["competitor-benchmark"])

_VALID_PRODUCTS = ("dscr", "private")


class RateCardUpdate(BaseModel):
    rate: float = Field(..., gt=0, lt=100)
    max_ltv: float = Field(..., gt=0, le=100)
    points: Optional[float] = Field(None, ge=0)
    prepay: Optional[str] = None


@router.get("/targets")
def get_targets(db: Session = Depends(get_db)) -> dict:
    rows = load_competitor_rows(db)
    report = compute_benchmark_report(rows, load_our_terms(db), as_of=date.today())
    return {
        "scanned": report.scanned,
        "targets": [
            {
                "lender": t.row.lender_name,
                "product": t.row.product,
                "region": t.row.region,
                "status": t.status,
                "stale": t.stale,
                "rate_delta_bps": t.rate_delta_bps,
                "ltv_delta_pts": t.ltv_delta_pts,
                "term_delta_months": t.term_delta_months,
                "competitor_rate": t.row.rate_low,
                "competitor_max_ltv": t.row.max_ltv,
                "competitor_term_months": t.row.term_months,
            }
            for t in report.targets
        ],
    }


@router.get("/rate-card")
def get_rate_card(db: Session = Depends(get_db)) -> dict:
    rows = db.execute(
        sa_text(
            "SELECT product, rate, max_ltv, points, prepay, updated_at "
            "FROM forced_action_lender_terms ORDER BY product"
        )
    ).mappings().all()
    return {"rate_card": [dict(r) for r in rows]}


@router.put("/rate-card/{product}")
def update_rate_card(
    product: str,
    body: RateCardUpdate,
    db: Session = Depends(get_db),
    _admin=Depends(get_current_admin),
) -> dict:
    if product not in _VALID_PRODUCTS:
        raise HTTPException(status_code=400, detail="Unknown product")
    result = db.execute(
        sa_text(
            """
            UPDATE forced_action_lender_terms
               SET rate = :rate, max_ltv = :max_ltv, points = :points,
                   prepay = :prepay, updated_at = NOW()
             WHERE product = :product
            """
        ),
        {
            "product": product,
            "rate": body.rate,
            "max_ltv": body.max_ltv,
            "points": body.points,
            "prepay": body.prepay,
        },
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Product not in rate card")
    db.commit()
    return {"product": product, "updated": True}
