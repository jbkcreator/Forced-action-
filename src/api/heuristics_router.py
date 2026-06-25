"""A3: Admin API for scoring weight overrides and heuristic tuner.

Endpoints:
  GET  /api/admin/scoring/weight-overrides       — list overrides with base + effective weight
  POST /api/admin/scoring/heuristics/seed        — re-apply config/heuristics.json to table
  POST /api/admin/scoring/heuristics/reset       — remove feedback rows, restore seed-only state
  POST /api/admin/scoring/heuristics/run-tuner   — run tuner on-demand, return summary
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from config.scoring import VERTICAL_WEIGHTS
from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.heuristic_loader import (
    load_overrides,
    reset_feedback_rows,
    seed_from_json,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/scoring", tags=["heuristics"])


@router.get("/weight-overrides")
def list_weight_overrides(
    vertical: Optional[str] = None,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Return all scoring weight overrides with base_weight, delta, and effective_weight."""
    from sqlalchemy import text as sa_text

    where = "WHERE enabled = TRUE"
    params: dict = {}
    if vertical:
        where += " AND vertical = :vertical"
        params["vertical"] = vertical

    rows = db.execute(
        sa_text(f"""
            SELECT id, vertical, signal_type, delta, source, reason,
                   enabled, loss_sample_count, win_sample_count,
                   created_at, updated_at
            FROM scoring_weight_overrides
            {where}
            ORDER BY vertical, signal_type
        """),
        params,
    ).mappings().all()

    items = []
    for r in rows:
        base = VERTICAL_WEIGHTS.get(r["vertical"], {}).get(r["signal_type"])
        delta = float(r["delta"])
        effective = max(0, min(100, base + delta)) if base is not None else None
        items.append({
            "id":                 r["id"],
            "vertical":           r["vertical"],
            "signal_type":        r["signal_type"],
            "base_weight":        base,
            "delta":              delta,
            "effective_weight":   effective,
            "source":             r["source"],
            "reason":             r["reason"],
            "enabled":            r["enabled"],
            "loss_sample_count":  r["loss_sample_count"],
            "win_sample_count":   r["win_sample_count"],
            "updated_at":         r["updated_at"].isoformat() if r["updated_at"] else None,
        })

    return {"total": len(items), "items": items}


@router.post("/heuristics/seed", status_code=200)
def seed_heuristics(
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Re-apply config/heuristics.json seed values to scoring_weight_overrides.

    Only updates rows with source='seed'. Feedback rows are untouched.
    """
    try:
        n = seed_from_json(db)
    except Exception as exc:
        logger.error("[heuristics] seed failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to seed heuristics")
    return {"seeded_rows": n, "message": f"Seeded {n} override rows from heuristics.json"}


@router.post("/heuristics/reset", status_code=200)
def reset_heuristics(
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Delete all feedback (non-seed) override rows, restoring seed-only state."""
    try:
        deleted = reset_feedback_rows(db)
    except Exception as exc:
        logger.error("[heuristics] reset failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to reset heuristics")
    return {"deleted_rows": deleted, "message": f"Removed {deleted} feedback override rows"}


@router.post("/heuristics/run-tuner", status_code=200)
def run_tuner_on_demand(
    dry_run: bool = False,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Run the heuristic tuner synchronously and return a summary.

    Pass ?dry_run=true to compute changes without writing to DB.
    """
    try:
        from tasks.heuristic_tuner import run
        summary = run(db=db, dry_run=dry_run)
    except Exception as exc:
        logger.error("[heuristics] tuner failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Tuner run failed")
    return summary
