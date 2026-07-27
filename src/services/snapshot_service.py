"""A5: Pre-Decision Snapshot Service.

Captures the full routing context at the moment a deal_outcome is created:
- All 6 CDS vertical scores (roads not taken)
- Selected vertical + top-3 runner-ups with score delta
- Active pricing cohort
- Last Lifecycle graph decision
- Last pitch variant from closer_calls

Resolution (funded/lost) is written back via resolve_snapshot() when the deal closes.

The future A5b counterfactual comparison engine reads rows where
counterfactual_run = FALSE AND outcome_status IS NOT NULL via idx_pds_pending_cf.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.core.models import PreDecisionSnapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def capture_snapshot(
    property_id: int,
    db: Session,
    deal_outcome_id: Optional[int] = None,
    prospect_id: Optional[UUID] = None,
    selected_vertical: Optional[str] = None,
    outcome_status: Optional[str] = None,
) -> Optional[PreDecisionSnapshot]:
    """Capture a pre-decision context snapshot for this deal.

    Idempotent: if a snapshot already exists for deal_outcome_id, the existing
    row is returned without re-writing. Safe to call multiple times.

    Args:
        property_id: The property being worked.
        db: Active SQLAlchemy session.
        deal_outcome_id: FK to deal_outcomes; used for idempotency.
        prospect_id: FK to prospects (optional).
        selected_vertical: The vertical chosen for this deal (trade_vertical).
        outcome_status: "funded" | "lost" | None — set immediately for terminal deals.

    Returns:
        The persisted PreDecisionSnapshot row, or None on failure.
    """
    try:
        # Idempotency check
        if deal_outcome_id is not None:
            existing = db.execute(
                sa_text("SELECT id FROM pre_decision_snapshots WHERE deal_outcome_id = :did"),
                {"did": deal_outcome_id},
            ).scalar_one_or_none()
            if existing is not None:
                logger.debug("[snapshot] existing snapshot %s for deal_outcome %d", existing, deal_outcome_id)
                return db.get(PreDecisionSnapshot, existing)

        # Gather context
        vertical_data = _gather_vertical_scores(property_id, db)
        all_scores: dict = vertical_data.get("vertical_scores") or {}
        runner_ups = _compute_runner_ups(all_scores, selected_vertical)

        # Resolve subscriber_id for Lifecycle/closer lookups
        subscriber_id: Optional[int] = None
        county_id: Optional[str] = None
        if deal_outcome_id is not None:
            row = db.execute(
                sa_text("SELECT subscriber_id, county_id, trade_vertical FROM deal_outcomes WHERE id = :id"),
                {"id": deal_outcome_id},
            ).mappings().one_or_none()
            if row:
                subscriber_id = row["subscriber_id"]
                county_id = row["county_id"]
                if selected_vertical is None:
                    selected_vertical = row["trade_vertical"]

        pricing = _gather_pricing(county_id, selected_vertical, db)
        lifecycle_graph = _gather_lifecycle_graph(subscriber_id, db)
        closer = _gather_pitch_variant(subscriber_id, db)

        raw_context = {
            "property": {
                "property_id": property_id,
                "lead_tier": vertical_data.get("lead_tier"),
                "final_cds_score": float(vertical_data["final_cds_score"]) if vertical_data.get("final_cds_score") else None,
                "urgency_level": vertical_data.get("urgency_level"),
                "distress_types": vertical_data.get("distress_types"),
            },
            "vertical_scores": all_scores,
            "runner_up_verticals": runner_ups,
            "active_pricing": pricing,
            "lifecycle_graph": lifecycle_graph,
            "recent_closer_call": closer,
        }

        resolved_at = datetime.now(timezone.utc) if outcome_status else None

        snap = PreDecisionSnapshot(
            property_id=property_id,
            prospect_id=prospect_id,
            deal_outcome_id=deal_outcome_id,
            selected_vertical=selected_vertical,
            lead_tier=vertical_data.get("lead_tier"),
            final_cds_score=vertical_data.get("final_cds_score"),
            distress_types=vertical_data.get("distress_types"),
            all_vertical_scores=all_scores or None,
            runner_up_verticals=runner_ups or None,
            pricing_cohort_id=pricing.get("id") if pricing else None,
            pricing_snapshot=pricing,
            lifecycle_graph=lifecycle_graph,
            pitch_variant=closer.get("pitch_variant") if closer else None,
            raw_context=raw_context,
            outcome_status=outcome_status,
            resolved_at=resolved_at,
        )
        db.add(snap)
        db.flush()
        logger.info(
            "[snapshot] captured property_id=%d deal_outcome_id=%s vertical=%s outcome=%s",
            property_id, deal_outcome_id, selected_vertical, outcome_status,
        )
        return snap

    except Exception as exc:
        logger.warning("[snapshot] capture failed property_id=%d: %s", property_id, exc, exc_info=True)
        return None


def resolve_snapshot(
    deal_outcome_id: int,
    outcome_status: str,
    db: Session,
) -> None:
    """Update outcome_status and resolved_at on an existing snapshot.

    Silent no-op if no snapshot exists for this deal_outcome_id.
    """
    try:
        db.execute(
            sa_text("""
                UPDATE pre_decision_snapshots
                   SET outcome_status = :status,
                       resolved_at    = now()
                 WHERE deal_outcome_id = :did
                   AND outcome_status IS NULL
            """),
            {"status": outcome_status, "did": deal_outcome_id},
        )
    except Exception as exc:
        logger.warning("[snapshot] resolve failed deal_outcome_id=%d: %s", deal_outcome_id, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Private context gatherers
# ---------------------------------------------------------------------------


def _gather_vertical_scores(property_id: int, db: Session) -> dict:
    """Return most recent distress_scores row fields for this property."""
    row = db.execute(
        sa_text("""
            SELECT vertical_scores, final_cds_score, lead_tier, urgency_level, distress_types
            FROM distress_scores
            WHERE property_id = :pid
            ORDER BY score_date DESC
            LIMIT 1
        """),
        {"pid": property_id},
    ).mappings().one_or_none()
    if row is None:
        return {}
    return {
        "vertical_scores":  dict(row["vertical_scores"]) if row["vertical_scores"] else {},
        "final_cds_score":  row["final_cds_score"],
        "lead_tier":        row["lead_tier"],
        "urgency_level":    row["urgency_level"],
        "distress_types":   list(row["distress_types"]) if row["distress_types"] else [],
    }


def _gather_pricing(
    county_id: Optional[str],
    vertical: Optional[str],
    db: Session,
) -> Optional[dict]:
    """Return the active pricing cohort for (county_id, vertical), or None."""
    if not county_id or not vertical:
        return None
    row = db.execute(
        sa_text("""
            SELECT id, price_type, base_price_cents, adjusted_price_cents, adjustment_pct, status
            FROM pricing_cohorts
            WHERE county_id = :county AND trade_vertical = :vertical AND status = 'active'
            ORDER BY activated_at DESC
            LIMIT 1
        """),
        {"county": county_id, "vertical": vertical},
    ).mappings().one_or_none()
    if row is None:
        return None
    return {
        "id":                   row["id"],
        "price_type":           row["price_type"],
        "base_price_cents":     row["base_price_cents"],
        "adjusted_price_cents": row["adjusted_price_cents"],
        "adjustment_pct":       float(row["adjustment_pct"]) if row["adjustment_pct"] else None,
        "status":               row["status"],
    }


def _gather_lifecycle_graph(subscriber_id: Optional[int], db: Session) -> Optional[str]:
    """Return the most recently used Lifecycle graph for this subscriber."""
    if not subscriber_id:
        return None
    row = db.execute(
        sa_text("""
            SELECT graph_name FROM agent_decisions
            WHERE subscriber_id = :sid
            ORDER BY started_at DESC
            LIMIT 1
        """),
        {"sid": subscriber_id},
    ).scalar_one_or_none()
    return row


def _gather_pitch_variant(subscriber_id: Optional[int], db: Session) -> Optional[dict]:
    """Return pitch_variant + call metadata from the latest closer_call."""
    if not subscriber_id:
        return None
    row = db.execute(
        sa_text("""
            SELECT pitch_variant, sentiment, call_outcome, objection_type
            FROM closer_calls
            WHERE subscriber_id = :sid
            ORDER BY started_at DESC
            LIMIT 1
        """),
        {"sid": subscriber_id},
    ).mappings().one_or_none()
    if row is None:
        return None
    return {
        "pitch_variant":  row["pitch_variant"],
        "sentiment":      row["sentiment"],
        "call_outcome":   row["call_outcome"],
        "objection_type": row["objection_type"],
    }


def _compute_runner_ups(
    all_scores: dict,
    selected_vertical: Optional[str],
) -> list:
    """Return top-3 non-selected verticals sorted by score descending."""
    if not all_scores:
        return []
    selected_score = all_scores.get(selected_vertical, 0.0) if selected_vertical else 0.0
    others = [
        (v, float(s)) for v, s in all_scores.items()
        if v != selected_vertical
    ]
    others.sort(key=lambda x: x[1], reverse=True)
    return [
        {
            "vertical":          v,
            "score":             s,
            "delta_vs_selected": round(s - float(selected_score), 2),
        }
        for v, s in others[:3]
    ]
