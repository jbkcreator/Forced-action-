"""Event consumers for the Loan Lane domain — Layer 3E.

Handlers for:
  broker.transition  → handle_lane_closer    (terminal states → set_lane_outcome)
  broker.transition  → handle_commission_poster (closed_won → post_commission)
  truth.verdict      → handle_truth_verdict  (routed_channel=loan_lane → enter_lane)

All handlers are idempotent. They receive a row-like object with
``event_id``, ``prospect_id``, and ``payload`` attributes.
"""
from __future__ import annotations

import logging

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def handle_truth_verdict(session: Session, event_row) -> None:
    """Create a loan lane when a truth verdict routes to loan_lane channel."""
    payload = event_row.payload or {}
    if payload.get("routed_channel") != "loan_lane":
        return

    from src.services.loan_lane_service import enter_lane
    prospect_id = str(event_row.prospect_id)
    lane_id = enter_lane(session, prospect_id)
    logger.info("[LaneCons] truth_verdict → lane_id=%s prospect_id=%s", lane_id, prospect_id)


def handle_lane_closer(session: Session, event_row) -> None:
    """Close the lane when broker transitions to a terminal state.

    closed_won  → outcome=funded
    closed_lost → outcome=dead
    All other states are silently ignored (idempotent per set_lane_outcome).
    """
    payload = event_row.payload or {}
    to_state = payload.get("to_state")
    if to_state not in ("closed_won", "closed_lost"):
        return

    lane_id = payload.get("lane_id")
    if not lane_id:
        logger.warning("[LaneCons] lane_closer missing lane_id in payload")
        return

    outcome = "funded" if to_state == "closed_won" else "dead"
    from src.services.loan_lane_service import set_lane_outcome
    set_lane_outcome(session, lane_id, outcome, actor="broker_state_machine")
    logger.info(
        "[LaneCons] lane_closer lane_id=%s to_state=%s outcome=%s",
        lane_id, to_state, outcome,
    )


def handle_commission_poster(session: Session, event_row) -> None:
    """Post commission when broker transitions to closed_won.

    Requires payload: to_state, transition_id, gross_amount_cents, split_config_id.
    Silently skips non-closed_won transitions.
    """
    payload = event_row.payload or {}
    if payload.get("to_state") != "closed_won":
        return

    transition_id = payload.get("transition_id")
    gross_amount_cents = payload.get("gross_amount_cents")
    split_config_id = payload.get("split_config_id")

    if not all([transition_id, gross_amount_cents is not None, split_config_id]):
        logger.warning(
            "[LaneCons] commission_poster missing payload fields: %s", payload
        )
        return

    from src.services.commission_ledger import post_commission
    entry_id = post_commission(
        session, transition_id, gross_amount_cents, split_config_id
    )
    if entry_id:
        logger.info(
            "[LaneCons] commission_poster posted entry_id=%s transition_id=%s",
            entry_id, transition_id,
        )
    else:
        logger.info(
            "[LaneCons] commission_poster replay skipped transition_id=%s", transition_id
        )
