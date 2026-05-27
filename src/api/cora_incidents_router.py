"""
Cora Incidents Router — admin API for self-healing incident management.

Endpoints:
    GET  /api/admin/cora-incidents                      — list with filters
    GET  /api/admin/cora-incidents/{incident_id}        — detail
    POST /api/admin/cora-incidents/{incident_id}/acknowledge — ack action
    POST /api/admin/cora-incidents/{incident_id}/resolve     — close incident

All endpoints are JWT-protected via get_current_admin.
All DB access uses sa_text() / session.execute — no ORM chains.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.core.database import get_db_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

_VALID_SEVERITIES = frozenset({"yellow", "red"})
_VALID_ACTIONS = frozenset({
    "no_op", "fallback_enabled", "auto_paused",
    "human_escalated", "feature_killed", "resolved",
})
_ACTION_STRENGTH: dict[str, int] = {
    "no_op": 0,
    "fallback_enabled": 1,
    "auto_paused": 2,
    "human_escalated": 3,
    "feature_killed": 4,
    "resolved": 5,
}


def _get_db():
    with get_db_context() as db:
        yield db


# ── GET /cora-incidents ───────────────────────────────────────────────────────

@router.get("/cora-incidents")
def list_cora_incidents(
    severity: Optional[str] = Query(None),
    metric_name: Optional[str] = Query(None),
    feature_name: Optional[str] = Query(None),
    action_taken: Optional[str] = Query(None),
    open_only: bool = Query(False),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """List cora_incident rows with optional filters."""
    if severity is not None and severity not in _VALID_SEVERITIES:
        raise HTTPException(
            status_code=422,
            detail=f"severity must be one of {sorted(_VALID_SEVERITIES)}",
        )
    if action_taken is not None and action_taken not in _VALID_ACTIONS:
        raise HTTPException(
            status_code=422,
            detail=f"action_taken must be one of {sorted(_VALID_ACTIONS)}",
        )
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must not be after date_to")

    conditions: list[str] = ["1=1"]
    params: dict[str, Any] = {}

    if severity is not None:
        conditions.append("severity = :severity")
        params["severity"] = severity
    if metric_name is not None:
        conditions.append("metric_name = :metric_name")
        params["metric_name"] = metric_name
    if feature_name is not None:
        conditions.append("feature_name = :feature_name")
        params["feature_name"] = feature_name
    if action_taken is not None:
        conditions.append("action_taken = :action_taken")
        params["action_taken"] = action_taken
    if open_only:
        conditions.append("breach_resolved IS NULL")
    if date_from:
        conditions.append("breach_started >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("breach_started <= :date_to")
        params["date_to"] = date_to

    where = " AND ".join(conditions)

    try:
        rows = db.execute(sa_text(f"""
            SELECT id, metric_name, county_id, feature_name, severity,
                   observed_value, threshold_value, baseline_value,
                   breach_started, breach_resolved, duration_hours,
                   action_taken, action_details, decision_id,
                   created_at, updated_at,
                   COUNT(*) OVER() AS _total
            FROM cora_incident
            WHERE {where}
            ORDER BY breach_started DESC
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": limit, "offset": offset}).mappings().all()
    except SQLAlchemyError:
        logger.exception("list_cora_incidents DB error")
        raise HTTPException(status_code=500, detail="Failed to query cora incidents")

    total = int(rows[0]["_total"]) if rows else 0
    data = [{k: v for k, v in r.items() if k != "_total"} for r in rows]

    return {"data": data, "total": total, "limit": limit, "offset": offset}


# ── GET /cora-incidents/{incident_id} ────────────────────────────────────────

@router.get("/cora-incidents/{incident_id}")
def get_cora_incident(
    incident_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Single incident detail."""
    try:
        row = db.execute(sa_text("""
            SELECT id, metric_name, county_id, feature_name, severity,
                   observed_value, threshold_value, baseline_value,
                   breach_started, breach_resolved, duration_hours,
                   action_taken, action_details, decision_id,
                   created_at, updated_at
            FROM cora_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("get_cora_incident DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query cora incident")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    return dict(row)


# ── POST /cora-incidents/{incident_id}/acknowledge ───────────────────────────

class AcknowledgeRequest(BaseModel):
    notes: Optional[str] = None
    acknowledged_by: Optional[str] = None


@router.post("/cora-incidents/{incident_id}/acknowledge")
def acknowledge_cora_incident(
    incident_id: int,
    body: AcknowledgeRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """
    Acknowledge an open incident.

    Upgrades action_taken to 'human_escalated' only when the current action
    is 'no_op'. Stronger actions (fallback_enabled, auto_paused, feature_killed)
    are preserved — we just append the ack metadata to action_details.
    """
    try:
        row = db.execute(sa_text("""
            SELECT id, action_taken, action_details, breach_resolved
            FROM cora_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("acknowledge_cora_incident fetch DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query cora incident")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    current_action = row["action_taken"]
    existing_details: dict = row["action_details"] or {}

    # Only escalate to human_escalated if the current action is the weakest (no_op).
    new_action = "human_escalated" if current_action == "no_op" else current_action

    ack_payload: dict[str, Any] = {
        "acknowledged_at": datetime.utcnow().isoformat(),
        "acknowledged_by": body.acknowledged_by or _admin.get("sub"),
    }
    if body.notes:
        ack_payload["notes"] = body.notes

    merged_details = {**existing_details, "acknowledgement": ack_payload}

    try:
        db.execute(sa_text("""
            UPDATE cora_incident
            SET action_taken = :action_taken,
                action_details = :action_details::jsonb,
                updated_at = NOW()
            WHERE id = :incident_id
        """), {
            "action_taken": new_action,
            "action_details": json.dumps(merged_details),
            "incident_id": incident_id,
        })
        db.commit()
    except SQLAlchemyError:
        logger.exception("acknowledge_cora_incident update DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to acknowledge cora incident")

    return {
        "id": incident_id,
        "action_taken": new_action,
        "action_details": merged_details,
        "upgraded": current_action != new_action,
    }


# ── POST /cora-incidents/{incident_id}/resolve ───────────────────────────────

class ResolveRequest(BaseModel):
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None


@router.post("/cora-incidents/{incident_id}/resolve")
def resolve_cora_incident(
    incident_id: int,
    body: ResolveRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """
    Close an incident: set breach_resolved, calculate duration_hours,
    set action_taken to 'resolved', merge resolution info into action_details.
    """
    try:
        row = db.execute(sa_text("""
            SELECT id, action_details, breach_resolved, breach_started
            FROM cora_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("resolve_cora_incident fetch DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query cora incident")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    if row["breach_resolved"] is not None:
        raise HTTPException(status_code=409, detail="Incident is already resolved")

    existing_details: dict = row["action_details"] or {}
    resolution_payload: dict[str, Any] = {
        "resolved_at": datetime.utcnow().isoformat(),
        "resolved_by": body.resolved_by or _admin.get("sub"),
    }
    if body.resolution_notes:
        resolution_payload["resolution_notes"] = body.resolution_notes

    merged_details = {**existing_details, "resolution": resolution_payload}

    try:
        updated = db.execute(sa_text("""
            UPDATE cora_incident
            SET breach_resolved = NOW(),
                duration_hours = EXTRACT(EPOCH FROM (NOW() - breach_started))::int / 3600,
                action_taken = 'resolved',
                action_details = :action_details::jsonb,
                updated_at = NOW()
            WHERE id = :incident_id
            RETURNING id, breach_resolved, duration_hours, action_taken, action_details
        """), {
            "action_details": json.dumps(merged_details),
            "incident_id": incident_id,
        }).mappings().first()
        db.commit()
    except SQLAlchemyError:
        logger.exception("resolve_cora_incident update DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to resolve cora incident")

    return dict(updated)
