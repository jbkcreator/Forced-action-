"""Canonical lane query helper — shared across broker and admin surfaces.

Produces the LaneObject shape the frontend contract requires.
All lane list/detail routes call _fetch_lanes() or _fetch_lane() rather than
rolling their own queries.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Core SQL — joins lanes to every related table in one round-trip
# ---------------------------------------------------------------------------

_LANE_SELECT = """
    SELECT
        l.lane_id,
        l.lane_type,
        l.current_stage,
        lsc.display_name            AS current_stage_display,
        l.outcome,
        l.assigned_broker_id,
        b.name                      AS assigned_broker_name,
        l.entered_at,
        l.fee_config_flag,
        l.lender_id,
        lnd.name                    AS lender_name,
        l.last_activity_at,
        (
            COALESCE(l.last_activity_at, l.entered_at)
            < NOW() - INTERVAL '30 days'
        )                           AS is_stale,
        (
            SELECT bt.to_state
            FROM broker_transitions bt
            WHERE bt.lane_id = l.lane_id
            ORDER BY bt.occurred_at DESC, bt.transition_id DESC
            LIMIT 1
        )                           AS current_work_state,
        p.prospect_id,
        pr.address,
        pr.city,
        pr.state,
        pr.county_id                AS county,
        pr.zip,
        o.owner_name,
        o.phone_1                   AS phone,
        o.email_1                   AS email
    FROM lanes l
    LEFT JOIN lane_stage_config lsc
           ON lsc.lane_type = l.lane_type
          AND lsc.stage_key = l.current_stage
          AND lsc.is_active = true
    LEFT JOIN brokers b   ON b.broker_id = l.assigned_broker_id
    LEFT JOIN lenders lnd ON lnd.lender_id = l.lender_id
    JOIN  prospects p     ON p.prospect_id = l.prospect_id
    JOIN  properties pr   ON pr.id = p.property_id
    LEFT JOIN owners o    ON o.property_id = p.property_id
"""


def _serialize(row: Any, *, redact_contact: bool = False) -> dict:
    """Convert a raw DB row into the canonical LaneObject dict."""
    return {
        "lane_id": str(row.lane_id),
        "lane_type": row.lane_type,
        "current_stage": row.current_stage,
        "current_stage_display": row.current_stage_display or row.current_stage,
        "current_work_state": row.current_work_state or "unassigned",
        "outcome": row.outcome,
        "assigned_broker_id": str(row.assigned_broker_id) if row.assigned_broker_id else None,
        "assigned_broker_name": row.assigned_broker_name,
        "entered_at": row.entered_at.isoformat() if row.entered_at else None,
        "is_stale": bool(row.is_stale),
        "lender_id": str(row.lender_id) if row.lender_id else None,
        "lender_name": row.lender_name,
        "fee_config_flag": bool(row.fee_config_flag),
        "prospect": {
            "prospect_id": str(row.prospect_id),
            "address": row.address,
            "city": row.city,
            "state": row.state,
            "county": row.county,
            "zip": row.zip,
            "owner_name": "masked" if redact_contact else row.owner_name,
            "phone": None if redact_contact else row.phone,
            "email": None if redact_contact else row.email,
        },
    }


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def fetch_lane(
    session: Session,
    lane_id: str,
    *,
    redact_contact: bool = False,
) -> dict | None:
    """Return a single LaneObject dict, or None if not found."""
    row = session.execute(
        sa_text(_LANE_SELECT + " WHERE l.lane_id = CAST(:lid AS uuid)"),
        {"lid": str(lane_id)},
    ).fetchone()
    return _serialize(row, redact_contact=redact_contact) if row else None


def fetch_lanes(
    session: Session,
    *,
    broker_id: str | None = None,
    assigned_only: bool = False,
    unassigned_only: bool = False,
    open_only: bool = True,
    redact_contact: bool = False,
    limit: int = 200,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Return (lanes, total_count) with optional filters.

    broker_id      — restrict to lanes assigned to this broker
    assigned_only  — only lanes with assigned_broker_id IS NOT NULL
    unassigned_only— only lanes with assigned_broker_id IS NULL (pool)
    open_only      — only outcome='open' (default True)
    redact_contact — mask phone/email/owner_name (pool view)
    """
    where_parts = []
    params: dict = {"limit": limit, "offset": offset}

    if open_only:
        where_parts.append("l.outcome = 'open'")
    if broker_id:
        where_parts.append("l.assigned_broker_id = CAST(:broker_id AS uuid)")
        params["broker_id"] = str(broker_id)
    if assigned_only:
        where_parts.append("l.assigned_broker_id IS NOT NULL")
    if unassigned_only:
        where_parts.append("l.assigned_broker_id IS NULL")

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows = session.execute(
        sa_text(
            _LANE_SELECT
            + where_sql
            + " ORDER BY l.last_activity_at DESC NULLS LAST"
            + " LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    total = session.execute(
        sa_text(
            "SELECT COUNT(*) FROM lanes l "
            + where_sql.replace("l.outcome", "l.outcome")
        ),
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    ).scalar() or 0

    return [_serialize(r, redact_contact=redact_contact) for r in rows], int(total)
