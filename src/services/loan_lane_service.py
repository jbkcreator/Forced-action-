"""Loan Lane core service — lane lifecycle.

Owns lane stage/outcome. Stage legality is read from the lane_stage_config DB
table (config-as-data, no deploy to change). The lane never writes broker state.
"""
from __future__ import annotations

import logging
import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.event_bus import emit_event

logger = logging.getLogger(__name__)

SOURCE_COMPONENT = "loan_lane"


def _lowest_stage(session: Session, lane_type: str) -> str:
    row = session.execute(
        text("""
            SELECT stage_key FROM lane_stage_config
            WHERE lane_type = :lt AND is_active = true
            ORDER BY order_index
            LIMIT 1
        """),
        {"lt": lane_type},
    ).fetchone()
    if row is None:
        raise ValueError(f"no active stages configured for lane_type={lane_type!r}")
    return row.stage_key


def enter_lane(
    session: Session,
    prospect_id: str,
    lane_type: str = "distressed-payoff",
    loan_program: str | None = None,
) -> str:
    """Create a lane at the lowest-order stage / open. Idempotent per (prospect_id, lane_type)."""
    existing = session.execute(
        text("""
            SELECT lane_id FROM lanes
            WHERE prospect_id = CAST(:pid AS uuid) AND lane_type = :lt
            LIMIT 1
        """),
        {"pid": str(prospect_id), "lt": lane_type},
    ).fetchone()
    if existing is not None:
        return str(existing.lane_id)

    stage = _lowest_stage(session, lane_type)
    row = session.execute(
        text("""
            INSERT INTO lanes (prospect_id, lane_type, loan_program, current_stage)
            VALUES (CAST(:pid AS uuid), :lt, :program, :stage)
            RETURNING lane_id
        """),
        {"pid": str(prospect_id), "lt": lane_type, "program": loan_program, "stage": stage},
    ).fetchone()
    lane_id = str(row.lane_id)

    emit_event(
        session,
        event_type="lane.entry",
        actor=SOURCE_COMPONENT,
        source_component=SOURCE_COMPONENT,
        prospect_id=prospect_id,
        payload={"lane_id": lane_id, "lane_type": lane_type, "current_stage": stage},
    )
    logger.info("[LoanLane] entered lane_id=%s prospect_id=%s stage=%s", lane_id, prospect_id, stage)
    return lane_id


def _load_lane(session: Session, lane_id: str):
    return session.execute(
        text("""
            SELECT lane_id, prospect_id, lane_type, current_stage, outcome,
                   assigned_broker_id, lender_id, claimed_at, last_activity_at
            FROM lanes WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id)},
    ).fetchone()


def _touch_lane(session: Session, lane_id: str) -> None:
    session.execute(
        text("UPDATE lanes SET last_activity_at = NOW(), updated_at = NOW() WHERE lane_id = CAST(:lid AS uuid)"),
        {"lid": str(lane_id)},
    )


def advance_lane(session: Session, lane_id: str, to_stage: str, actor: str) -> None:
    """Advance the lane to `to_stage` if legal per lane_stage_config.allowed_next.

    Manual intermediate-stage progression (quoted/committed). Raises ValueError on
    an illegal move. Terminal funded/dead come via set_lane_outcome, not here.
    """
    lane = _load_lane(session, lane_id)
    if lane is None:
        raise ValueError(f"lane not found: {lane_id}")

    cfg = session.execute(
        text("""
            SELECT allowed_next FROM lane_stage_config
            WHERE lane_type = :lt AND stage_key = :stage
        """),
        {"lt": lane.lane_type, "stage": lane.current_stage},
    ).fetchone()
    allowed = (cfg.allowed_next if cfg else None) or []
    if to_stage not in allowed:
        raise ValueError(
            f"illegal lane advance: {lane.current_stage!r} → {to_stage!r} (allowed: {allowed})"
        )

    session.execute(
        text("UPDATE lanes SET current_stage = :stage, updated_at = NOW() WHERE lane_id = CAST(:lid AS uuid)"),
        {"stage": to_stage, "lid": str(lane_id)},
    )
    emit_event(
        session,
        event_type="lane.advance",
        actor=actor,
        source_component=SOURCE_COMPONENT,
        prospect_id=lane.prospect_id,
        payload={"lane_id": str(lane_id), "from_stage": lane.current_stage, "to_stage": to_stage},
    )
    logger.info("[LoanLane] advanced lane_id=%s %s→%s", lane_id, lane.current_stage, to_stage)


def set_lane_outcome(session: Session, lane_id: str, outcome: str, actor: str = "system") -> None:
    """Close the lane to a terminal outcome (funded/dead/recycled). Idempotent."""
    lane = _load_lane(session, lane_id)
    if lane is None:
        raise ValueError(f"lane not found: {lane_id}")
    if lane.outcome != "open":
        return  # already closed — idempotent

    # On a terminal close, mirror current_stage to the matching terminal stage.
    stage = "funded" if outcome == "funded" else ("dead" if outcome == "dead" else lane.current_stage)
    session.execute(
        text("""
            UPDATE lanes SET outcome = :outcome, current_stage = :stage, updated_at = NOW()
            WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"outcome": outcome, "stage": stage, "lid": str(lane_id)},
    )
    emit_event(
        session,
        event_type="lane.close",
        actor=actor,
        source_component=SOURCE_COMPONENT,
        prospect_id=lane.prospect_id,
        payload={"lane_id": str(lane_id), "outcome": outcome},
    )
    logger.info("[LoanLane] closed lane_id=%s outcome=%s", lane_id, outcome)


def claim_lane(session: Session, lane_id: str, broker_id: str) -> bool:
    """Atomically claim an unassigned lane for a broker.

    Returns True when we won the race, False if another broker already claimed it.
    """
    row = session.execute(
        text("""
            UPDATE lanes
               SET assigned_broker_id = CAST(:bid AS uuid),
                   claimed_at = NOW(),
                   last_activity_at = NOW(),
                   updated_at = NOW()
             WHERE lane_id = CAST(:lid AS uuid)
               AND assigned_broker_id IS NULL
               AND outcome = 'open'
            RETURNING lane_id
        """),
        {"lid": str(lane_id), "bid": str(broker_id)},
    ).fetchone()
    return row is not None


def reassign_lane(session: Session, lane_id: str, broker_id: str, actor: str) -> None:
    """Admin override for ownership."""
    session.execute(
        text("""
            UPDATE lanes
               SET assigned_broker_id = CAST(:bid AS uuid),
                   claimed_at = NOW(),
                   last_activity_at = NOW(),
                   updated_at = NOW()
             WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id), "bid": str(broker_id)},
    )


def release_lane(session: Session, lane_id: str, actor: str) -> None:
    """Return a lane to the pool."""
    session.execute(
        text("""
            UPDATE lanes
               SET assigned_broker_id = NULL,
                   claimed_at = NULL,
                   last_activity_at = NOW(),
                   updated_at = NOW()
             WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id)},
    )


def distress_reason(distress_types, vertical_scores) -> str:
    """Human-readable teaser for pool/review surfaces."""
    reasons: list[str] = []
    for source in (distress_types, vertical_scores):
        if not source:
            continue
        if isinstance(source, dict):
            reasons.extend([str(k).replace("_", " ") for k in source.keys() if k])
        else:
            try:
                parsed = json.loads(source)
                if isinstance(parsed, dict):
                    reasons.extend([str(k).replace("_", " ") for k in parsed.keys() if k])
            except Exception:
                reasons.append(str(source))
    if not reasons:
        return "distress signal"
    unique: list[str] = []
    for reason in reasons:
        if reason not in unique:
            unique.append(reason)
    return ", ".join(unique[:3])


def get_pool(session: Session, *, limit: int = 50) -> list[dict]:
    """Return teasers for open, unclaimed, non-guess loan lanes."""
    rows = session.execute(
        text("""
            SELECT
                l.lane_id,
                l.prospect_id,
                l.current_stage,
                l.outcome,
                l.assigned_broker_id,
                l.lender_id,
                l.claimed_at,
                l.last_activity_at,
                p.property_id,
                pr.address,
                pr.city,
                pr.state,
                pr.zip,
                ds.final_cds_score,
                ds.lead_tier,
                ds.distress_types,
                ds.vertical_scores,
                ds.is_guess_lead
            FROM lanes l
            JOIN prospects p ON p.prospect_id = l.prospect_id
            JOIN properties pr ON pr.id = p.property_id
            JOIN LATERAL (
                SELECT ds.*
                FROM distress_scores ds
                WHERE ds.property_id = p.property_id
                ORDER BY ds.score_date DESC, ds.id DESC
                LIMIT 1
            ) ds ON true
            WHERE l.outcome = 'open'
              AND l.assigned_broker_id IS NULL
              AND ds.is_guess_lead = false
            ORDER BY ds.final_cds_score DESC NULLS LAST, l.entered_at DESC
            LIMIT :limit
        """),
        {"limit": limit},
    ).fetchall()
    return [
        {
            "lane_id": str(row.lane_id),
            "prospect_id": str(row.prospect_id),
            "property_id": row.property_id,
            "address": row.address,
            "city": row.city,
            "state": row.state,
            "zip": row.zip,
            "current_stage": row.current_stage,
            "outcome": row.outcome,
            "final_cds_score": float(row.final_cds_score) if row.final_cds_score is not None else None,
            "lead_tier": row.lead_tier,
            "distress_reason": distress_reason(row.distress_types, row.vertical_scores),
            "is_guess_lead": bool(row.is_guess_lead),
            "claimed_at": row.claimed_at,
            "last_activity_at": row.last_activity_at,
        }
        for row in rows
    ]


def get_stale_lanes(session: Session, *, days: int = 30) -> list[dict]:
    """Return open lanes with no activity for the configured window."""
    rows = session.execute(
        text("""
            SELECT lane_id, prospect_id, current_stage, outcome, assigned_broker_id,
                   claimed_at, last_activity_at, entered_at
            FROM lanes
            WHERE outcome = 'open'
              AND COALESCE(last_activity_at, entered_at) < NOW() - (:days || ' days')::interval
            ORDER BY COALESCE(last_activity_at, entered_at) ASC
        """),
        {"days": days},
    ).fetchall()
    return [
        {
            "lane_id": str(row.lane_id),
            "prospect_id": str(row.prospect_id),
            "current_stage": row.current_stage,
            "outcome": row.outcome,
            "assigned_broker_id": str(row.assigned_broker_id) if row.assigned_broker_id else None,
            "claimed_at": row.claimed_at,
            "last_activity_at": row.last_activity_at,
            "entered_at": row.entered_at,
        }
        for row in rows
    ]


def set_lane_lender(session: Session, lane_id: str, lender_id: str, actor: str) -> None:
    """Attach a cleared, active lender to the lane."""
    lender = session.execute(
        text("""
            SELECT lender_id
            FROM lenders
            WHERE lender_id = CAST(:lid AS uuid)
              AND is_cleared = true
              AND is_active = true
        """),
        {"lid": str(lender_id)},
    ).fetchone()
    if lender is None:
        raise ValueError(f"lender not eligible: {lender_id}")

    session.execute(
        text("""
            UPDATE lanes
               SET lender_id = CAST(:lender_id AS uuid),
                   last_activity_at = NOW(),
                   updated_at = NOW()
             WHERE lane_id = CAST(:lane_id AS uuid)
        """),
        {"lane_id": str(lane_id), "lender_id": str(lender_id)},
    )


def fee_surfaces_enabled(session: Session, lane_id: str) -> bool:
    """RESPA gate — True only when the lane's fee_config_flag is ON.

    Single decision point for whether commission net_lines / fee amounts may be
    surfaced for this lane.
    """
    row = session.execute(
        text("SELECT fee_config_flag FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
        {"lid": str(lane_id)},
    ).fetchone()
    return bool(row and row.fee_config_flag)
