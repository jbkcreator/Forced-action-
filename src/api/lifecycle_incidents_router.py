"""
Lifecycle Incidents Router — admin API for self-healing incident management.

Endpoints:
    GET  /api/admin/lifecycle-incidents                      — list with filters
    GET  /api/admin/lifecycle-incidents/{incident_id}        — detail
    POST /api/admin/lifecycle-incidents/{incident_id}/acknowledge — ack action
    POST /api/admin/lifecycle-incidents/{incident_id}/resolve     — close incident
    GET  /api/admin/lifecycle/subscribers/{subscriber_id}/timeline — Lifecycle touch timeline

All endpoints are JWT-protected via get_current_admin.
All DB access uses sa_text() / session.execute — no ORM chains.
"""

from __future__ import annotations

import base64
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
from src.api.deps import get_db as _get_db

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


# ── GET /lifecycle-incidents ───────────────────────────────────────────────────────

@router.get("/lifecycle-incidents")
def list_lifecycle_incidents(
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
    """List lifecycle_incident rows with optional filters."""
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
            FROM lifecycle_incident
            WHERE {where}
            ORDER BY breach_started DESC
            LIMIT :limit OFFSET :offset
        """), {**params, "limit": limit, "offset": offset}).mappings().all()
    except SQLAlchemyError:
        logger.exception("list_lifecycle_incidents DB error")
        raise HTTPException(status_code=500, detail="Failed to query lifecycle incidents")

    total = int(rows[0]["_total"]) if rows else 0
    data = [{k: v for k, v in r.items() if k != "_total"} for r in rows]

    return {"data": data, "total": total, "limit": limit, "offset": offset}


# ── GET /lifecycle-incidents/{incident_id} ────────────────────────────────────────

@router.get("/lifecycle-incidents/{incident_id}")
def get_lifecycle_incident(
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
            FROM lifecycle_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("get_lifecycle_incident DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query lifecycle incident")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    return dict(row)


# ── POST /lifecycle-incidents/{incident_id}/acknowledge ───────────────────────────

class AcknowledgeRequest(BaseModel):
    notes: Optional[str] = None
    acknowledged_by: Optional[str] = None


@router.post("/lifecycle-incidents/{incident_id}/acknowledge")
def acknowledge_lifecycle_incident(
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
            FROM lifecycle_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("acknowledge_lifecycle_incident fetch DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query lifecycle incident")

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
            UPDATE lifecycle_incident
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
        logger.exception("acknowledge_lifecycle_incident update DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to acknowledge lifecycle incident")

    return {
        "id": incident_id,
        "action_taken": new_action,
        "action_details": merged_details,
        "upgraded": current_action != new_action,
    }


# ── POST /lifecycle-incidents/{incident_id}/resolve ───────────────────────────────

class ResolveRequest(BaseModel):
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None


@router.post("/lifecycle-incidents/{incident_id}/resolve")
def resolve_lifecycle_incident(
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
            FROM lifecycle_incident
            WHERE id = :incident_id
        """), {"incident_id": incident_id}).mappings().first()
    except SQLAlchemyError:
        logger.exception("resolve_lifecycle_incident fetch DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to query lifecycle incident")

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
            UPDATE lifecycle_incident
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
        logger.exception("resolve_lifecycle_incident update DB error incident_id=%s", incident_id)
        raise HTTPException(status_code=500, detail="Failed to resolve lifecycle incident")

    return dict(updated)


# ── GET /lifecycle/subscribers/{subscriber_id}/timeline ───────────────────────────

_VALID_TERMINAL_STATUSES = frozenset({"completed", "aborted", "escalated", "failed"})


def _encode_cursor(started_at: datetime, decision_id: str) -> str:
    raw = f"{started_at.isoformat()}|{decision_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        started_at_iso, decision_id = raw.split("|", 1)
        return started_at_iso, decision_id
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


@router.get("/lifecycle/subscribers/{subscriber_id}/timeline")
def get_subscriber_lifecycle_timeline(
    subscriber_id: int,
    cursor: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    graph_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    since: Optional[datetime] = Query(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
) -> dict[str, Any]:
    """Per-subscriber Lifecycle touch timeline. One row per agent_decisions entry, newest first."""
    if status is not None and status not in _VALID_TERMINAL_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {sorted(_VALID_TERMINAL_STATUSES)}",
        )

    # Verify subscriber exists.
    try:
        sub_row = db.execute(
            sa_text("SELECT id FROM subscribers WHERE id = :id"),
            {"id": subscriber_id},
        ).fetchone()
    except SQLAlchemyError:
        logger.exception("timeline subscriber check DB error subscriber_id=%s", subscriber_id)
        raise HTTPException(status_code=500, detail="Database error")

    if sub_row is None:
        raise HTTPException(status_code=404, detail=f"Subscriber {subscriber_id} not found")

    conditions: list[str] = ["subscriber_id = :subscriber_id"]
    params: dict[str, Any] = {"subscriber_id": subscriber_id, "limit": limit + 1}

    if graph_name is not None:
        conditions.append("graph_name = :graph_name")
        params["graph_name"] = graph_name
    if status is not None:
        conditions.append("terminal_status = :status")
        params["status"] = status
    if since is not None:
        conditions.append("started_at >= :since")
        params["since"] = since
    if cursor is not None:
        cursor_started_at, cursor_decision_id = _decode_cursor(cursor)
        conditions.append(
            "(started_at, decision_id) < (:cursor_started_at, :cursor_decision_id)"
        )
        params["cursor_started_at"] = cursor_started_at
        params["cursor_decision_id"] = cursor_decision_id

    where = " AND ".join(conditions)

    try:
        rows = db.execute(sa_text(f"""
            SELECT
                decision_id, graph_name, event_type,
                started_at, completed_at, terminal_status,
                autonomy_class, was_autonomous, variant_id,
                requires_approval, approved_at, approved_by,
                overridden_at, overridden_by, override_reason_code, override_reason,
                tokens_used, cost_usd, summary
            FROM agent_decisions
            WHERE {where}
            ORDER BY started_at DESC, decision_id DESC
            LIMIT :limit
        """), params).mappings().all()
    except SQLAlchemyError:
        logger.exception("timeline decisions DB error subscriber_id=%s", subscriber_id)
        raise HTTPException(status_code=500, detail="Failed to query timeline")

    has_more = len(rows) > limit
    page_rows = list(rows[:limit])

    # Fetch child SMS sends for this page in one query.
    decision_ids = [r["decision_id"] for r in page_rows]
    sms_by_decision: dict[str, list[dict]] = {d: [] for d in decision_ids}
    if decision_ids:
        try:
            sms_rows = db.execute(
                sa_text("""
                    SELECT id, decision_id, outcome, message_type, vendor,
                           vendor_message_id, body_preview, created_at
                    FROM sms_send_logs
                    WHERE decision_id = ANY(:ids)
                    ORDER BY created_at ASC
                """),
                {"ids": decision_ids},
            ).mappings().all()
        except SQLAlchemyError:
            logger.exception("timeline sms_send_logs DB error subscriber_id=%s", subscriber_id)
            raise HTTPException(status_code=500, detail="Failed to query SMS sends")

        for sms in sms_rows:
            sms_by_decision[sms["decision_id"]].append(dict(sms))

    def _serialize(row: Any) -> dict[str, Any]:
        d = dict(row)
        override: Optional[dict] = None
        if d.get("overridden_at"):
            override = {
                "overridden_at": d["overridden_at"],
                "overridden_by": d.get("overridden_by"),
                "override_reason_code": d.get("override_reason_code"),
                "override_reason": d.get("override_reason"),
            }
        elif d.get("approved_at"):
            override = {
                "approved_at": d["approved_at"],
                "approved_by": d.get("approved_by"),
            }
        return {
            "decision_id": d["decision_id"],
            "graph_name": d["graph_name"],
            "event_type": d.get("event_type"),
            "started_at": d["started_at"],
            "completed_at": d.get("completed_at"),
            "terminal_status": d.get("terminal_status"),
            "autonomy_class": d.get("autonomy_class"),
            "was_autonomous": d.get("was_autonomous", False),
            "requires_approval": d.get("requires_approval", False),
            "variant_id": d.get("variant_id"),
            "tokens_used": d.get("tokens_used", 0),
            "cost_usd": float(d["cost_usd"]) if d.get("cost_usd") is not None else None,
            "override": override,
            "summary": d.get("summary"),
            "sms_sends": sms_by_decision.get(d["decision_id"], []),
        }

    items = [_serialize(r) for r in page_rows]
    next_cursor: Optional[str] = None
    if has_more and items:
        last = items[-1]
        next_cursor = _encode_cursor(last["started_at"], last["decision_id"])

    return {
        "subscriber_id": subscriber_id,
        "items": items,
        "next_cursor": next_cursor,
    }
