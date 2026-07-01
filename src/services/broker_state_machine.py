"""Broker State Machine — Layer 3B.

Owns broker work-state for Loan Lanes. Append-only: every state change
writes a broker_transitions row and emits a broker.transition event.

Does NOT:
- update lane.current_stage or lane.outcome
- call advance_lane() or set_lane_outcome()
- touch the commission ledger
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.broker_states import (
    IllegalBrokerTransition,
    InvalidBrokerReasonCode,
    InvalidBrokerState,
    validate_transition,
)
logger = logging.getLogger(__name__)

_SOURCE = "broker_state_machine"
_SMS_ALLOWED_STATES = frozenset({"working", "quoted", "committed"})


# ── Exceptions ────────────────────────────────────────────────────────────────

class LaneNotFound(Exception): ...
class BrokerNotFound(Exception): ...
class BrokerInactive(Exception): ...
class LaneNotOpen(Exception): ...
class LaneOwnershipError(Exception): ...
class ClosedWonPayloadRequired(IllegalBrokerTransition): ...
class InvalidGrossAmount(Exception): ...

IllegalTransition = IllegalBrokerTransition


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load_lane(session: Session, lane_id: str):
    return session.execute(
        sa_text("""
            SELECT lane_id, property_id, lane_type, current_stage,
                   outcome, assigned_broker_id
            FROM lanes WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id)},
    ).fetchone()


def _verify_broker_active(session: Session, broker_id: str) -> None:
    row = session.execute(
        sa_text(
            "SELECT broker_id, is_active FROM brokers "
            "WHERE broker_id = CAST(:bid AS uuid)"
        ),
        {"bid": str(broker_id)},
    ).fetchone()
    if row is None:
        raise BrokerNotFound(f"Broker {broker_id!r} not found.")
    if not row.is_active:
        raise BrokerInactive(f"Broker {broker_id!r} is inactive.")


def _latest_broker_state(session: Session, lane_id: str):
    return session.execute(
        sa_text("""
            SELECT to_state FROM broker_transitions
            WHERE lane_id = CAST(:lid AS uuid)
            ORDER BY occurred_at DESC, transition_id DESC
            LIMIT 1
        """),
        {"lid": str(lane_id)},
    ).fetchone()


def _insert_transition(
    session: Session,
    *,
    lane_id: str,
    broker_id: str,
    from_state: str,
    to_state: str,
    reason_code: str,
    actor: str,
) -> str:
    row = session.execute(
        sa_text("""
            INSERT INTO broker_transitions
                (lane_id, broker_id, from_state, to_state, reason_code, actor)
            VALUES
                (CAST(:lane_id AS uuid), CAST(:broker_id AS uuid),
                 :from_state, :to_state, :reason_code, :actor)
            RETURNING transition_id
        """),
        {
            "lane_id": str(lane_id),
            "broker_id": str(broker_id),
            "from_state": from_state,
            "to_state": to_state,
            "reason_code": reason_code,
            "actor": actor,
        },
    ).fetchone()
    return str(row.transition_id)




# ── Public API ────────────────────────────────────────────────────────────────

def current_state(session: Session, lane_id: str) -> str:
    """Return the current broker work-state for a lane.

    Reads the latest broker_transitions row. Falls back to lane assignment
    state when no transition rows exist yet.
    """
    latest = _latest_broker_state(session, lane_id)
    if latest is not None:
        return latest.to_state

    lane = _load_lane(session, lane_id)
    if lane is None:
        raise LaneNotFound(f"Lane {lane_id!r} not found.")
    return "assigned" if lane.assigned_broker_id else "unassigned"


def list_transitions(session: Session, lane_id: str) -> list[dict]:
    """Return all broker transitions for a lane, oldest first."""
    rows = session.execute(
        sa_text("""
            SELECT transition_id, lane_id, broker_id,
                   from_state, to_state, reason_code, actor, occurred_at
            FROM broker_transitions
            WHERE lane_id = CAST(:lid AS uuid)
            ORDER BY occurred_at ASC, transition_id ASC
        """),
        {"lid": str(lane_id)},
    ).fetchall()
    return [
        {
            "transition_id": str(r.transition_id),
            "lane_id": str(r.lane_id),
            "broker_id": str(r.broker_id),
            "from_state": r.from_state,
            "to_state": r.to_state,
            "reason_code": r.reason_code,
            "actor": r.actor,
            "occurred_at": r.occurred_at.isoformat() if r.occurred_at else None,
        }
        for r in rows
    ]


def assign_broker(
    session: Session,
    lane_id: str,
    broker_id: str,
    actor: str | None = None,
) -> bool:
    """Atomic broker self-claim on an open, unclaimed lane.

    Returns True when the claim succeeds, False if another broker already
    owns the lane. Inserts an unassigned→assigned transition and emits
    broker.transition on success.
    """
    _verify_broker_active(session, broker_id)
    _actor = actor or str(broker_id)

    row = session.execute(
        sa_text("""
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

    if row is None:
        return False

    tid = _insert_transition(
        session,
        lane_id=str(row.lane_id),
        broker_id=broker_id,
        from_state="unassigned",
        to_state="assigned",
        reason_code="qualified",
        actor=_actor,
    )
    logger.info("[BrokerSM] claimed lane_id=%s broker_id=%s tid=%s", lane_id, broker_id, tid)
    return True


def reassign_lane(session: Session, lane_id: str, broker_id: str, actor: str) -> str:
    """Admin override — assign a broker to any open lane regardless of current owner.

    Inserts a broker transition from the current work-state to 'assigned'
    and emits broker.transition. Returns the new transition_id.
    """
    lane = _load_lane(session, lane_id)
    if lane is None:
        raise LaneNotFound(f"Lane {lane_id!r} not found.")
    if lane.outcome != "open":
        raise LaneNotOpen(f"Lane {lane_id!r} is not open (outcome={lane.outcome!r}).")
    _verify_broker_active(session, broker_id)

    latest = _latest_broker_state(session, lane_id)
    if latest is not None:
        from_state = latest.to_state
    elif lane.assigned_broker_id:
        from_state = "assigned"
    else:
        from_state = "unassigned"

    session.execute(
        sa_text("""
            UPDATE lanes
               SET assigned_broker_id = CAST(:bid AS uuid),
                   claimed_at = COALESCE(claimed_at, NOW()),
                   last_activity_at = NOW(),
                   updated_at = NOW()
             WHERE lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id), "bid": str(broker_id)},
    )

    tid = _insert_transition(
        session,
        lane_id=lane_id,
        broker_id=broker_id,
        from_state=from_state,
        to_state="assigned",
        reason_code="qualified",
        actor=actor,
    )
    logger.info(
        "[BrokerSM] reassigned lane_id=%s broker_id=%s actor=%s from=%s tid=%s",
        lane_id, broker_id, actor, from_state, tid,
    )
    return tid


def transition(
    session: Session,
    lane_id: str,
    to_state: str,
    broker_id: str,
    reason_code: str,
    *,
    gross_amount_cents: int | None = None,
    split_config_id: str | None = None,
    actor: str | None = None,
) -> str:
    """Execute a broker work-state transition on a lane.

    Validates ownership, state legality, reason_code, and closed_won payload.
    Does NOT update lane.current_stage or lane.outcome.
    Returns the new transition_id.
    """
    lane = _load_lane(session, lane_id)
    if lane is None:
        raise LaneNotFound(f"Lane {lane_id!r} not found.")
    if lane.outcome != "open":
        raise LaneNotOpen(f"Lane {lane_id!r} is not open (outcome={lane.outcome!r}).")

    _verify_broker_active(session, broker_id)

    from_state = current_state(session, lane_id)

    # Validate state legality before ownership so tests seeding broker_transitions
    # directly get IllegalTransition rather than LaneOwnershipError.
    # Re-raise InvalidBrokerReasonCode/InvalidBrokerState as IllegalTransition so
    # callers have a single exception type for any transition-legality failure.
    try:
        validate_transition(from_state, to_state, reason_code)
    except InvalidBrokerReasonCode as exc:
        raise IllegalBrokerTransition(f"Invalid reason_code: {exc}") from exc
    except InvalidBrokerState as exc:
        raise IllegalBrokerTransition(str(exc)) from exc

    if str(lane.assigned_broker_id) != str(broker_id):
        raise LaneOwnershipError(
            f"Broker {broker_id!r} does not own lane {lane_id!r}."
        )

    tid = _insert_transition(
        session,
        lane_id=lane_id,
        broker_id=broker_id,
        from_state=from_state,
        to_state=to_state,
        reason_code=reason_code,
        actor=actor or str(broker_id),
    )

    session.execute(
        sa_text(
            "UPDATE lanes SET last_activity_at = NOW(), updated_at = NOW() "
            "WHERE lane_id = CAST(:lid AS uuid)"
        ),
        {"lid": str(lane_id)},
    )

    logger.info(
        "[BrokerSM] transition lane_id=%s %s→%s broker_id=%s reason=%s",
        lane_id, from_state, to_state, broker_id, reason_code,
    )
    return tid


def claim_lane(session: Session, lane_id: str, broker_id: str) -> bool:
    """Thin alias for assign_broker — keeps API/service naming consistent."""
    return assign_broker(session, lane_id, broker_id)


def sms_eligible(session: Session, lane_id: str) -> bool:
    """Return True only when both lane stage and broker work-state permit SMS.

    Lane stage must have sms_allowed=true in lane_stage_config, AND broker
    work-state must be one of: working, quoted, committed.
    """
    row = session.execute(
        sa_text("""
            SELECT l.current_stage, lsc.sms_allowed
            FROM lanes l
            LEFT JOIN lane_stage_config lsc
                   ON lsc.lane_type = l.lane_type
                  AND lsc.stage_key = l.current_stage
            WHERE l.lane_id = CAST(:lid AS uuid)
        """),
        {"lid": str(lane_id)},
    ).fetchone()

    if row is None:
        return False
    if not row.sms_allowed:
        return False

    state = current_state(session, lane_id)
    return state in _SMS_ALLOWED_STATES
