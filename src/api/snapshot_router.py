"""A5: Admin API for pre-decision snapshots.

Endpoints:
  GET /api/admin/snapshots       — list with filters
  GET /api/admin/snapshots/{id}  — full detail
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/snapshots", tags=["snapshots"])


@router.get("")
def list_snapshots(
    property_id: Optional[int] = None,
    selected_vertical: Optional[str] = None,
    outcome_status: Optional[str] = None,
    counterfactual_run: Optional[bool] = None,
    from_ts: Optional[str] = None,
    to_ts: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List pre-decision snapshots with optional filters."""
    limit = min(limit, 100)

    clauses = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}

    if property_id is not None:
        clauses.append("property_id = :property_id")
        params["property_id"] = property_id
    if selected_vertical:
        clauses.append("selected_vertical = :selected_vertical")
        params["selected_vertical"] = selected_vertical
    if outcome_status:
        clauses.append("outcome_status = :outcome_status")
        params["outcome_status"] = outcome_status
    if counterfactual_run is not None:
        clauses.append("counterfactual_run = :counterfactual_run")
        params["counterfactual_run"] = counterfactual_run
    if from_ts:
        clauses.append("snapshot_ts >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts:
        clauses.append("snapshot_ts <= :to_ts")
        params["to_ts"] = to_ts

    where = " AND ".join(clauses)

    rows = db.execute(
        sa_text(f"""
            SELECT id, property_id, prospect_id, deal_outcome_id,
                   snapshot_ts, selected_vertical, lead_tier, final_cds_score,
                   distress_types, all_vertical_scores, runner_up_verticals,
                   pricing_cohort_id, cora_graph, pitch_variant,
                   outcome_status, resolved_at,
                   counterfactual_run, counterfactual_run_at, created_at
            FROM pre_decision_snapshots
            WHERE {where}
            ORDER BY snapshot_ts DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).mappings().all()

    total_row = db.execute(
        sa_text(f"SELECT count(*) FROM pre_decision_snapshots WHERE {where}"),
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    ).scalar()

    items = []
    for r in rows:
        items.append({
            "id":                  str(r["id"]),
            "property_id":         r["property_id"],
            "prospect_id":         str(r["prospect_id"]) if r["prospect_id"] else None,
            "deal_outcome_id":     r["deal_outcome_id"],
            "snapshot_ts":         r["snapshot_ts"].isoformat() if r["snapshot_ts"] else None,
            "selected_vertical":   r["selected_vertical"],
            "lead_tier":           r["lead_tier"],
            "final_cds_score":     float(r["final_cds_score"]) if r["final_cds_score"] else None,
            "distress_types":      r["distress_types"],
            "all_vertical_scores": r["all_vertical_scores"],
            "runner_up_verticals": r["runner_up_verticals"],
            "pricing_cohort_id":   r["pricing_cohort_id"],
            "cora_graph":          r["cora_graph"],
            "pitch_variant":       r["pitch_variant"],
            "outcome_status":      r["outcome_status"],
            "resolved_at":         r["resolved_at"].isoformat() if r["resolved_at"] else None,
            "counterfactual_run":  r["counterfactual_run"],
            "counterfactual_run_at": r["counterfactual_run_at"].isoformat() if r["counterfactual_run_at"] else None,
            "created_at":          r["created_at"].isoformat() if r["created_at"] else None,
        })

    return {"total": total_row, "items": items}


@router.get("/{snapshot_id}")
def get_snapshot(
    snapshot_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Return full detail for a single snapshot including raw_context."""
    row = db.execute(
        sa_text("""
            SELECT id, property_id, prospect_id, deal_outcome_id,
                   snapshot_ts, selected_vertical, lead_tier, final_cds_score,
                   distress_types, all_vertical_scores, runner_up_verticals,
                   pricing_cohort_id, pricing_snapshot, cora_graph, pitch_variant,
                   raw_context, outcome_status, resolved_at,
                   broker_id, alternative_brokers,
                   counterfactual_run, counterfactual_run_at, created_at
            FROM pre_decision_snapshots
            WHERE id = :id
        """),
        {"id": snapshot_id},
    ).mappings().one_or_none()

    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    return {
        "id":                    str(row["id"]),
        "property_id":           row["property_id"],
        "prospect_id":           str(row["prospect_id"]) if row["prospect_id"] else None,
        "deal_outcome_id":       row["deal_outcome_id"],
        "snapshot_ts":           row["snapshot_ts"].isoformat() if row["snapshot_ts"] else None,
        "selected_vertical":     row["selected_vertical"],
        "lead_tier":             row["lead_tier"],
        "final_cds_score":       float(row["final_cds_score"]) if row["final_cds_score"] else None,
        "distress_types":        row["distress_types"],
        "all_vertical_scores":   row["all_vertical_scores"],
        "runner_up_verticals":   row["runner_up_verticals"],
        "pricing_cohort_id":     row["pricing_cohort_id"],
        "pricing_snapshot":      row["pricing_snapshot"],
        "cora_graph":            row["cora_graph"],
        "pitch_variant":         row["pitch_variant"],
        "raw_context":           row["raw_context"],
        "outcome_status":        row["outcome_status"],
        "resolved_at":           row["resolved_at"].isoformat() if row["resolved_at"] else None,
        "broker_id":             row["broker_id"],
        "alternative_brokers":   row["alternative_brokers"],
        "counterfactual_run":    row["counterfactual_run"],
        "counterfactual_run_at": row["counterfactual_run_at"].isoformat() if row["counterfactual_run_at"] else None,
        "created_at":            row["created_at"].isoformat() if row["created_at"] else None,
    }
