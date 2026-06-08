"""
SMS Analytics Router — admin API for dynamic personalization analytics.

Endpoints:
    GET /api/admin/message-outcomes                — list (no context_snapshot)
    GET /api/admin/message-outcomes/{message_id}   — detail (with context_snapshot)
    GET /api/admin/sms-variant-performance          — aggregate by dimension

All endpoints are JWT-protected via get_current_admin.
All DB access uses sa_text() / session.execute — no ORM chains.
context_snapshot is PII-adjacent; the list endpoint returns only
has_context_snapshot (bool), the detail endpoint returns the full payload.
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
from src.api.deps import get_db as _get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

# Allowlisted group_by dimensions — never interpolate an untrusted string into SQL.
_VALID_GROUP_BY_DIMS: frozenset[str] = frozenset({
    "variant_id",
    "template_id",
    "prompt_version",
    "trade_vertical",
    "county_id",
    "behavioral_segment",
    "revenue_signal_score_band",
    "last_action_recency_band",
})

_VALID_COMPOUND_GROUP_BY: frozenset[str] = frozenset({
    "template_id,variant_id",
    "trade_vertical,behavioral_segment",
})

# Equality-filterable columns for the list endpoint.
# All names must exactly match message_outcomes column names.
_EQUALITY_FILTER_COLS: tuple[str, ...] = (
    "trade_vertical",
    "county_id",
    "behavioral_segment",
    "revenue_signal_score_band",
    "last_action_recency_band",
    "prompt_version",
    "conversion_type",
    "template_id",
    "variant_id",
    "channel",
    "message_type",
)


# ── GET /message-outcomes ────────────────────────────────────────────────────

@router.get("/message-outcomes")
def list_message_outcomes(
    trade_vertical: Optional[str] = Query(None),
    county_id: Optional[str] = Query(None),
    behavioral_segment: Optional[str] = Query(None),
    revenue_signal_score_band: Optional[str] = Query(None),
    last_action_recency_band: Optional[str] = Query(None),
    prompt_version: Optional[str] = Query(None),
    conversion_type: Optional[str] = Query(None),
    template_id: Optional[str] = Query(None),
    variant_id: Optional[str] = Query(None),
    channel: Optional[str] = Query(None),
    message_type: Optional[str] = Query(None),
    subscriber_id: Optional[int] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    page: int = Query(1, ge=1, le=10_000),
    per_page: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """
    List message outcome rows. context_snapshot is not returned here;
    use the detail endpoint to fetch it for a single row.
    """
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must not be after date_to")

    conditions: list[str] = ["1=1"]
    params: dict[str, Any] = {}

    filter_values: dict[str, Any] = {
        "trade_vertical": trade_vertical,
        "county_id": county_id,
        "behavioral_segment": behavioral_segment,
        "revenue_signal_score_band": revenue_signal_score_band,
        "last_action_recency_band": last_action_recency_band,
        "prompt_version": prompt_version,
        "conversion_type": conversion_type,
        "template_id": template_id,
        "variant_id": variant_id,
        "channel": channel,
        "message_type": message_type,
    }
    for col in _EQUALITY_FILTER_COLS:
        if filter_values[col] is not None:
            conditions.append(f"{col} = :{col}")
            params[col] = filter_values[col]

    if subscriber_id is not None:
        conditions.append("subscriber_id = :subscriber_id")
        params["subscriber_id"] = subscriber_id

    if date_from:
        conditions.append("sent_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("sent_at <= :date_to")
        params["date_to"] = date_to

    where = " AND ".join(conditions)
    offset = (page - 1) * per_page

    try:
        rows = db.execute(sa_text(f"""
            SELECT id, subscriber_id,
                   message_type, template_id, variant_id, channel,
                   sent_at, delivered_at, replied_at, clicked_at,
                   conversion_type, conversion_within_24h, revenue_attributed,
                   trade_vertical, county_id, behavioral_segment,
                   revenue_signal_score, revenue_signal_score_band,
                   last_action_recency_band, prompt_version,
                   (context_snapshot IS NOT NULL) AS has_context_snapshot,
                   COUNT(*) OVER() AS _total
            FROM message_outcomes
            WHERE {where}
            ORDER BY sent_at DESC
            LIMIT :per_page OFFSET :offset
        """), {**params, "per_page": per_page, "offset": offset}).mappings().all()
    except SQLAlchemyError:
        logger.exception("list_message_outcomes DB error page=%s per_page=%s", page, per_page)
        raise HTTPException(status_code=500, detail="Failed to query message outcomes")

    total = int(rows[0]["_total"]) if rows else 0
    data = [{k: v for k, v in r.items() if k != "_total"} for r in rows]

    return {"data": data, "total": total, "page": page, "per_page": per_page}


# ── GET /message-outcomes/{message_id} ───────────────────────────────────────

@router.get("/message-outcomes/{message_id}")
def get_message_outcome(
    message_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Single message outcome detail, including context_snapshot."""
    try:
        row = db.execute(sa_text("""
            SELECT id, subscriber_id,
                   message_type, template_id, variant_id, channel,
                   sent_at, delivered_at, replied_at, clicked_at,
                   conversion_type,
                   conversion_within_4h, conversion_within_24h, conversion_within_48h,
                   revenue_attributed,
                   trade_vertical, county_id, behavioral_segment,
                   revenue_signal_score, revenue_signal_score_band,
                   last_action_recency_band, prompt_version,
                   context_snapshot,
                   created_at
            FROM message_outcomes
            WHERE id = :message_id
        """), {"message_id": message_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("get_message_outcome DB error message_id=%s", message_id)
        raise HTTPException(status_code=500, detail="Failed to query message outcome")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Message outcome {message_id} not found")

    return dict(row)


# ── GET /sms-variant-performance ─────────────────────────────────────────────

@router.get("/sms-variant-performance")
def sms_variant_performance(
    group_by: str = Query("prompt_version"),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """
    Aggregate reply rate and conversion rate by a single dimension or an
    allowed compound key.

    Allowed single dimensions: variant_id, template_id, prompt_version,
    trade_vertical, county_id, behavioral_segment,
    revenue_signal_score_band, last_action_recency_band.

    Allowed compound: "template_id,variant_id", "trade_vertical,behavioral_segment".

    conversion_within_24h is used as the conversion signal.
    """
    # Validate against the explicit allowlists — no untrusted column interpolation.
    if (
        group_by not in _VALID_GROUP_BY_DIMS
        and group_by not in _VALID_COMPOUND_GROUP_BY
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"group_by must be one of {sorted(_VALID_GROUP_BY_DIMS)} "
                f"or compound: {sorted(_VALID_COMPOUND_GROUP_BY)}"
            ),
        )

    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must not be after date_to")

    conditions: list[str] = ["1=1"]
    params: dict[str, Any] = {}

    if date_from:
        conditions.append("sent_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("sent_at <= :date_to")
        params["date_to"] = date_to

    where = " AND ".join(conditions)

    # group_by is validated above — safe to interpolate.
    select_cols = group_by  # e.g. "prompt_version" or "template_id, variant_id"
    group_cols = group_by

    try:
        rows = db.execute(sa_text(f"""
            SELECT {select_cols},
                   COUNT(*)                                                       AS total_sent,
                   COUNT(*) FILTER (WHERE delivered_at IS NOT NULL)               AS total_delivered,
                   COUNT(*) FILTER (WHERE replied_at IS NOT NULL)                 AS total_replied,
                   COUNT(*) FILTER (WHERE clicked_at IS NOT NULL)                 AS total_clicked,
                   COUNT(*) FILTER (WHERE conversion_within_24h = true)           AS total_converted,
                   ROUND(
                       100.0 * COUNT(*) FILTER (WHERE replied_at IS NOT NULL)
                       / NULLIF(COUNT(*), 0), 2
                   )                                                              AS reply_rate_pct,
                   ROUND(
                       100.0 * COUNT(*) FILTER (WHERE conversion_within_24h = true)
                       / NULLIF(COUNT(*), 0), 2
                   )                                                              AS conversion_rate_pct,
                   AVG(revenue_signal_score)                                      AS avg_revenue_signal_score,
                   SUM(revenue_attributed)                                        AS total_revenue_attributed
            FROM message_outcomes
            WHERE {where}
            GROUP BY {group_cols}
            ORDER BY total_sent DESC
        """), params).mappings().all()
    except SQLAlchemyError:
        logger.exception("sms_variant_performance DB error group_by=%s", group_by)
        raise HTTPException(status_code=500, detail="Failed to query SMS variant performance")

    return {"data": [dict(r) for r in rows], "group_by": group_by}
