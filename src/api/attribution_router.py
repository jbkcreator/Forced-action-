"""
Attribution Router — Stage 8: admin dashboard API for conversion attribution.

All endpoints are JWT-protected via `get_current_admin` from admin_router.
All DB access is raw SQL (text()/session.execute).

Mounted at /api/admin/attribution/ by main.py.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text as sa_text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.core.database import get_db_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/attribution", tags=["attribution"])

_VALID_GROUP_DIMS: frozenset[str] = frozenset({
    "conversion_type", "zip_code", "trade", "wallet_tier",
    "lock_status", "autopilot_tier", "bundle_type", "deal_size_bucket",
})

# Columns that support simple equality filtering in list_conversions.
# Kept as a tuple (not set) to preserve a stable iteration order.
_EQUALITY_FILTER_COLS: tuple[str, ...] = (
    "conversion_type", "zip_code", "trade", "wallet_tier",
    "lock_status", "autopilot_tier", "bundle_type", "deal_size_bucket",
)

# Columns selected for the attribution_event portion of the trace response.
_TRACE_ATTR_COLS: frozenset[str] = frozenset({
    "id", "conversion_type", "source_table", "source_event_id",
    "subscriber_id", "lead_id", "property_id",
    "zip_code", "trade", "wallet_tier", "lock_status", "lock_zip",
    "autopilot_tier", "bundle_id", "bundle_type", "deal_size_bucket",
    "revenue_amount", "currency", "occurred_at",
    "attribution_status", "attribution_confidence", "attribution_metadata", "created_at",
})


def _get_db():
    with get_db_context() as db:
        yield db


def _apply_date_range(
    conditions: list[str],
    params: dict[str, Any],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> None:
    """Append occurred_at range conditions in-place; raises 422 if range is inverted."""
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must not be after date_to")
    if date_from:
        conditions.append("occurred_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("occurred_at <= :date_to")
        params["date_to"] = date_to


# ── GET /conversions ─────────────────────────────────────────────────────────

@router.get("/conversions")
def list_conversions(
    conversion_type: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None),
    trade: Optional[str] = Query(None),
    wallet_tier: Optional[str] = Query(None),
    lock_status: Optional[str] = Query(None),
    autopilot_tier: Optional[str] = Query(None),
    bundle_type: Optional[str] = Query(None),
    deal_size_bucket: Optional[str] = Query(None),
    subscriber_id: Optional[int] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    page: int = Query(1, ge=1, le=10_000),
    per_page: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """List conversion attribution events with optional dimension filters."""
    conditions: list[str] = ["1=1"]
    params: dict[str, Any] = {}

    # Build equality filters from locals() rather than 8 separate if-blocks.
    filter_values: dict[str, Any] = {
        "conversion_type": conversion_type,
        "zip_code": zip_code,
        "trade": trade,
        "wallet_tier": wallet_tier,
        "lock_status": lock_status,
        "autopilot_tier": autopilot_tier,
        "bundle_type": bundle_type,
        "deal_size_bucket": deal_size_bucket,
    }
    for col in _EQUALITY_FILTER_COLS:
        if filter_values[col] is not None:
            conditions.append(f"{col} = :{col}")
            params[col] = filter_values[col]

    if subscriber_id is not None:
        conditions.append("subscriber_id = :subscriber_id")
        params["subscriber_id"] = subscriber_id

    _apply_date_range(conditions, params, date_from, date_to)

    where = " AND ".join(conditions)
    offset = (page - 1) * per_page

    try:
        # COUNT(*) OVER() computes the total in the same scan — no second round-trip.
        rows = db.execute(sa_text(f"""
            SELECT id, conversion_type, source_table, source_event_id,
                   subscriber_id, lead_id, property_id,
                   zip_code, trade, wallet_tier, lock_status, lock_zip,
                   autopilot_tier, bundle_id, bundle_type, deal_size_bucket,
                   revenue_amount, currency,
                   occurred_at, attribution_status, attribution_confidence,
                   attribution_metadata, created_at,
                   COUNT(*) OVER() AS _total
            FROM conversion_attribution_events
            WHERE {where}
            ORDER BY occurred_at DESC
            LIMIT :per_page OFFSET :offset
        """), {**params, "per_page": per_page, "offset": offset}).mappings().all()
    except SQLAlchemyError:
        logger.exception("list_conversions DB error page=%s per_page=%s", page, per_page)
        raise HTTPException(status_code=500, detail="Failed to query attribution events")

    total = int(rows[0]["_total"]) if rows else 0
    data = [{k: v for k, v in r.items() if k != "_total"} for r in rows]

    return {"data": data, "total": total, "page": page, "per_page": per_page}


# ── GET /stats ────────────────────────────────────────────────────────────────

@router.get("/stats")
def attribution_stats(
    group_by: str = Query("conversion_type"),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Aggregate conversion counts and revenue by a single dimension."""
    if group_by not in _VALID_GROUP_DIMS:
        raise HTTPException(
            status_code=422,
            detail=f"group_by must be one of {sorted(_VALID_GROUP_DIMS)}",
        )

    conditions: list[str] = ["1=1"]
    params: dict[str, Any] = {}
    _apply_date_range(conditions, params, date_from, date_to)

    where = " AND ".join(conditions)
    try:
        rows = db.execute(sa_text(f"""
            SELECT {group_by}          AS group_value,
                   COUNT(*)             AS count,
                   SUM(revenue_amount)  AS revenue_sum
            FROM conversion_attribution_events
            WHERE {where}
            GROUP BY {group_by}
            ORDER BY count DESC
        """), params).mappings().all()
    except SQLAlchemyError:
        logger.exception("attribution_stats DB error group_by=%s", group_by)
        raise HTTPException(status_code=500, detail="Failed to query attribution stats")

    return {"data": [dict(r) for r in rows]}


# ── GET /subscriber/{subscriber_id} ──────────────────────────────────────────

@router.get("/subscriber/{subscriber_id}")
def subscriber_attribution(
    subscriber_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Latest score fields + last 20 score events + last 20 attribution events."""
    try:
        sub = db.execute(sa_text("""
            SELECT id, revenue_signal_score, revenue_signal_band,
                   revenue_signal_breakdown, revenue_signal_updated_at,
                   email, tier, vertical
            FROM subscribers
            WHERE id = :sub_id
        """), {"sub_id": subscriber_id}).mappings().first()

        if sub is None:
            raise HTTPException(status_code=404, detail=f"Subscriber {subscriber_id} not found")

        score_events = db.execute(sa_text("""
            SELECT id, action_type, old_score, new_score, delta, band,
                   breakdown, metadata, created_at
            FROM revenue_signal_score_events
            WHERE subscriber_id = :sub_id
            ORDER BY created_at DESC
            LIMIT 20
        """), {"sub_id": subscriber_id}).mappings().all()

        attr_events = db.execute(sa_text("""
            SELECT id, conversion_type, source_table, source_event_id,
                   zip_code, trade, wallet_tier, lock_status, autopilot_tier,
                   bundle_type, deal_size_bucket, revenue_amount,
                   occurred_at, attribution_status, attribution_confidence
            FROM conversion_attribution_events
            WHERE subscriber_id = :sub_id
            ORDER BY occurred_at DESC
            LIMIT 20
        """), {"sub_id": subscriber_id}).mappings().all()

    except HTTPException:
        raise
    except SQLAlchemyError:
        logger.exception("subscriber_attribution DB error subscriber_id=%s", subscriber_id)
        raise HTTPException(status_code=500, detail="Failed to query subscriber attribution")

    return {
        "subscriber": dict(sub),
        "score_events": [dict(r) for r in score_events],
        "attribution_events": [dict(r) for r in attr_events],
    }


# ── GET /trace/{attribution_event_id} ────────────────────────────────────────

@router.get("/trace/{attribution_event_id}")
def trace_attribution(
    attribution_event_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Single attribution row + linked score event in one DB round-trip."""
    # LEFT JOIN retrieves both rows in a single query.
    # Score-event columns are prefixed with rsse_ to avoid name collisions with
    # cae columns that share the same name (id, created_at, metadata).
    try:
        row = db.execute(sa_text("""
            SELECT
                cae.id,                  cae.conversion_type,     cae.source_table,
                cae.source_event_id,     cae.subscriber_id,       cae.lead_id,
                cae.property_id,         cae.zip_code,            cae.trade,
                cae.wallet_tier,         cae.lock_status,         cae.lock_zip,
                cae.autopilot_tier,      cae.bundle_id,           cae.bundle_type,
                cae.deal_size_bucket,    cae.revenue_amount,      cae.currency,
                cae.occurred_at,         cae.attribution_status,  cae.attribution_confidence,
                cae.attribution_metadata,cae.created_at,
                rsse.id          AS rsse_id,
                rsse.action_type AS rsse_action_type,
                rsse.old_score,
                rsse.new_score,
                rsse.delta,
                rsse.band,
                rsse.breakdown,
                rsse.metadata    AS rsse_metadata,
                rsse.created_at  AS rsse_created_at
            FROM conversion_attribution_events cae
            LEFT JOIN revenue_signal_score_events rsse
                ON  rsse.subscriber_id = cae.subscriber_id
                AND (rsse.metadata->>'attribution_event_id')::bigint = cae.id
            WHERE cae.id = :eid
            ORDER BY rsse.created_at DESC
            LIMIT 1
        """), {"eid": attribution_event_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("trace_attribution DB error attribution_event_id=%s", attribution_event_id)
        raise HTTPException(status_code=500, detail="Failed to query attribution trace")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Attribution event {attribution_event_id} not found")

    score_event: Optional[dict[str, Any]] = None
    if row["rsse_id"] is not None:
        score_event = {
            "id":          row["rsse_id"],
            "action_type": row["rsse_action_type"],
            "old_score":   row["old_score"],
            "new_score":   row["new_score"],
            "delta":       row["delta"],
            "band":        row["band"],
            "breakdown":   row["breakdown"],
            "metadata":    row["rsse_metadata"],
            "created_at":  row["rsse_created_at"],
        }

    return {
        "attribution_event": {k: row[k] for k in _TRACE_ATTR_COLS},
        "score_event": score_event,
    }
