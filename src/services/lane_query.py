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
# Core SQL — joins lanes to every related table in one round-trip.
# current_work_state uses a LATERAL join (not scalar subquery) so the outer
# WHERE can filter on it efficiently.
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
        bts.to_state                AS current_work_state,
        l.property_id,
        pr.address,
        pr.city,
        pr.state,
        pr.county_id                AS county,
        pr.zip,
        o.owner_name,
        o.phone_1                   AS phone,
        o.email_1                   AS email,
        l.lane_description,
        l.description_tier,
        fi.intent_tier              AS current_intent_tier,
        fi.financing_intent_score   AS intent_score
    FROM lanes l
    LEFT JOIN lane_stage_config lsc
           ON lsc.lane_type = l.lane_type
          AND lsc.stage_key = l.current_stage
          AND lsc.is_active = true
    LEFT JOIN brokers b    ON b.broker_id = l.assigned_broker_id
    LEFT JOIN lenders lnd  ON lnd.lender_id = l.lender_id
    JOIN  properties pr    ON pr.id = l.property_id
    LEFT JOIN owners o     ON o.property_id = l.property_id
    LEFT JOIN LATERAL (
        SELECT to_state
        FROM broker_transitions
        WHERE lane_id = l.lane_id
        ORDER BY occurred_at DESC, transition_id DESC
        LIMIT 1
    ) bts ON true
    LEFT JOIN LATERAL (
        SELECT intent_tier, financing_intent_score
        FROM financing_intent_scores
        WHERE property_id = l.property_id
        ORDER BY score_date DESC
        LIMIT 1
    ) fi ON true
"""

# Minimal FROM for the COUNT(*) query — only the tables needed for WHERE.
# Lateral joins and owners are included only when their columns appear in filters.
_COUNT_BASE = "SELECT COUNT(*) FROM lanes l JOIN properties pr ON pr.id = l.property_id"
_COUNT_OWNERS = "\n    LEFT JOIN owners o ON o.property_id = l.property_id"
_COUNT_BTS  = """
    LEFT JOIN LATERAL (
        SELECT to_state
        FROM broker_transitions
        WHERE lane_id = l.lane_id
        ORDER BY occurred_at DESC, transition_id DESC
        LIMIT 1
    ) bts ON true
"""
_COUNT_FI = """
    LEFT JOIN LATERAL (
        SELECT intent_tier, financing_intent_score
        FROM financing_intent_scores
        WHERE property_id = l.property_id
        ORDER BY score_date DESC
        LIMIT 1
    ) fi ON true
"""

_CONTACT_FILTER_SQL: dict[str, str] = {
    "has_phone":  "EXISTS (SELECT 1 FROM owners _co WHERE _co.property_id = l.property_id AND _co.phone_1 IS NOT NULL)",
    "has_email":  "EXISTS (SELECT 1 FROM owners _co WHERE _co.property_id = l.property_id AND _co.email_1 IS NOT NULL)",
    "has_both":   "EXISTS (SELECT 1 FROM owners _co WHERE _co.property_id = l.property_id AND _co.phone_1 IS NOT NULL AND _co.email_1 IS NOT NULL)",
    "no_contact": "NOT EXISTS (SELECT 1 FROM owners _co WHERE _co.property_id = l.property_id AND (_co.phone_1 IS NOT NULL OR _co.email_1 IS NOT NULL))",
}

_VALID_SORT = {
    "entered_at":    ("l.entered_at",                  ""),
    "last_activity": ("l.last_activity_at",             "NULLS LAST"),
    "intent_score":  ("fi.financing_intent_score",      "NULLS LAST"),
    "address":       ("pr.address",                     ""),
}


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
        "property_id": int(row.property_id),
        "intent_score": float(row.intent_score) if row.intent_score is not None else None,
        "intent_tier": row.current_intent_tier,
        "lane_description": row.lane_description,
        "description_outdated": (
            row.lane_description is not None
            and row.description_tier is not None
            and row.current_intent_tier is not None
            and row.description_tier != row.current_intent_tier
        ),
        "property": {
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
    # access-control filters
    broker_id: str | None = None,
    assigned_only: bool = False,
    unassigned_only: bool = False,
    open_only: bool = True,
    redact_contact: bool = False,
    # contact availability filters
    has_contact: bool = False,          # system gate: phone OR email present (broker pool)
    contact_filter: str | None = None,  # admin lens: has_phone|has_email|has_both|no_contact
    # user-facing filters
    intent_tier: str | None = None,
    work_state: str | None = None,
    stage: str | None = None,
    county: str | None = None,
    lender_id: str | None = None,
    stale_only: bool = False,
    entered_from: str | None = None,
    entered_to: str | None = None,
    activity_from: str | None = None,
    activity_to: str | None = None,
    # sorting
    sort_by: str = "entered_at",
    sort_dir: str = "desc",
    # pagination
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Return (lanes, total_count) with optional filters and pagination.

    access-control:
      broker_id      — restrict to lanes assigned to this broker
      assigned_only  — only lanes with assigned_broker_id IS NOT NULL
      unassigned_only— only lanes with assigned_broker_id IS NULL (pool)
      open_only      — only outcome='open' (default True)
      redact_contact — mask phone/email/owner_name (pool view)

    filters:
      intent_tier    — high/medium/low/very_low/unknown
      work_state     — unassigned/working/quoted/…
      stage          — entered/quoted/committed/funded/dead
      county         — county_id string
      lender_id      — UUID string
      entered_from   — ISO date, lower bound on l.entered_at
      entered_to     — ISO date, upper bound on l.entered_at
      activity_from  — ISO date, lower bound on l.last_activity_at
      activity_to    — ISO date, upper bound on l.last_activity_at
    """
    where_parts: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    # --- access-control predicates ---
    if open_only:
        where_parts.append("l.outcome = 'open'")
    if broker_id:
        where_parts.append("l.assigned_broker_id = CAST(:broker_id AS uuid)")
        params["broker_id"] = str(broker_id)
    if assigned_only:
        where_parts.append("l.assigned_broker_id IS NOT NULL")
    if unassigned_only:
        where_parts.append("l.assigned_broker_id IS NULL")

    # --- filter predicates ---
    if intent_tier:
        where_parts.append("fi.intent_tier = :intent_tier")
        params["intent_tier"] = intent_tier
    if work_state:
        where_parts.append("COALESCE(bts.to_state, 'unassigned') = :work_state")
        params["work_state"] = work_state
    if stage:
        where_parts.append("l.current_stage = :stage")
        params["stage"] = stage
    if county:
        where_parts.append("pr.county_id = :county")
        params["county"] = county
    if lender_id:
        where_parts.append("l.lender_id = CAST(:lender_id AS uuid)")
        params["lender_id"] = str(lender_id)
    if stale_only:
        where_parts.append(
            "COALESCE(l.last_activity_at, l.entered_at) < NOW() - INTERVAL '30 days'"
        )
    if entered_from:
        where_parts.append("l.entered_at >= :entered_from")
        params["entered_from"] = entered_from
    if entered_to:
        where_parts.append("l.entered_at <= :entered_to")
        params["entered_to"] = entered_to
    if activity_from:
        where_parts.append("l.last_activity_at >= :activity_from")
        params["activity_from"] = activity_from
    if activity_to:
        where_parts.append("l.last_activity_at <= :activity_to")
        params["activity_to"] = activity_to
    if has_contact:
        where_parts.append("EXISTS (SELECT 1 FROM owners _co WHERE _co.property_id = l.property_id AND (_co.phone_1 IS NOT NULL OR _co.email_1 IS NOT NULL))")
    if contact_filter and contact_filter in _CONTACT_FILTER_SQL:
        where_parts.append(_CONTACT_FILTER_SQL[contact_filter])

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # --- sort ---
    sort_col, nulls = _VALID_SORT.get(sort_by, _VALID_SORT["entered_at"])
    direction = "ASC" if sort_dir.lower() == "asc" else "DESC"
    order_sql = f"ORDER BY {sort_col} {direction} {nulls}".rstrip()

    # --- data query ---
    rows = session.execute(
        sa_text(
            _LANE_SELECT
            + where_sql
            + f" {order_sql}"
            + " LIMIT :limit OFFSET :offset"
        ),
        params,
    ).fetchall()

    # --- count query — include joins only when their columns are filtered ---
    needs_bts  = work_state is not None
    needs_fi   = intent_tier is not None
    count_from = _COUNT_BASE
    if needs_bts:
        count_from += _COUNT_BTS
    if needs_fi:
        count_from += _COUNT_FI

    total = session.execute(
        sa_text(count_from + " " + where_sql),
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    ).scalar() or 0

    return [_serialize(r, redact_contact=redact_contact) for r in rows], int(total)
