"""Event consumers for Block 13 — Outcome Closed-Loop (T-B13-02).

Handlers for the decoupled deal-outcome fan-out:
  outcome.recorded → handle_outcome_snapshot   (learning-loop score snapshot)
  outcome.recorded → handle_outcome_loss_autopsy (dead → LLM loss autopsy)

Both are the score/retune-feeding consumers, so both honor the client's
loss-reason rule: only lead-fault dead outcomes feed scoring; buyer-neutral
dead outcomes are score-protected (logged buyer-side on the DealOutcome row
only, never fed to the snapshot/autopsy learning loop). Closed (won) outcomes
always feed as a positive signal. pending never reaches here (not emitted).

All handlers are idempotent (processed_events guard + capture_snapshot's own
deal_outcome_id guard). They receive a row-like object with ``payload``.
"""
from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from src.services import outcome_reasons

logger = logging.getLogger(__name__)


def _feeds_scoring(payload: dict) -> bool:
    """Whether this outcome should feed the scoring/retune learning loop.

    closed → positive signal (always). dead → only when lead-fault
    (buyer-neutral is score-protected). pending → never.

    This gate IS the deal-capture path's Outcome Sanity Filter (T-B13-03): the
    buyer_neutral reason class is the analog of score_feedback_service's B0-02
    buyer-capacity shield (low_fico / no_capital) — a death the buyer caused must
    not teach the model the lead is bad. No dead outcome reaches the snapshot /
    loss-autopsy scoring consumers without passing here.
    """
    state = payload.get("outcome_state")
    if state == "closed":
        return True
    if state == "dead":
        return payload.get("reason_fault_class") == outcome_reasons.LEAD_FAULT
    return False


def apply_snapshot(session: Session, payload: dict) -> None:
    """Capture the pre-decision score snapshot for a scoring-eligible outcome.

    Pure payload entry point — used by the async handler and by deal-capture's
    inline fallback (property with no prospect row for the prospect-scoped outbox).
    """
    if not _feeds_scoring(payload):
        logger.info(
            "[OutcomeCons] snapshot skipped (score-protected) deal_outcome_id=%s state=%s fault=%s",
            payload.get("deal_outcome_id"), payload.get("outcome_state"),
            payload.get("reason_fault_class"),
        )
        return

    deal_outcome_id = payload.get("deal_outcome_id")
    status = "funded" if payload.get("outcome_state") == "closed" else "lost"

    from src.services.snapshot_service import capture_snapshot
    capture_snapshot(
        property_id=payload.get("property_id"),
        db=session,
        deal_outcome_id=deal_outcome_id,
        selected_vertical=payload.get("selected_vertical"),
        outcome_status=status,
    )
    logger.info(
        "[OutcomeCons] snapshot captured deal_outcome_id=%s status=%s",
        deal_outcome_id, status,
    )


def apply_loss_autopsy(session: Session, payload: dict) -> None:
    """Run the loss autopsy for a lead-fault dead outcome only.

    Pure payload entry point — see apply_snapshot.
    """
    if payload.get("outcome_state") != "dead":
        return
    if payload.get("reason_fault_class") != outcome_reasons.LEAD_FAULT:
        logger.info(
            "[OutcomeCons] loss autopsy skipped (score-protected buyer-neutral) "
            "deal_outcome_id=%s reason=%s",
            payload.get("deal_outcome_id"), payload.get("dead_reason"),
        )
        return

    from src.services.loss_autopsy import run_loss_autopsy
    run_loss_autopsy(
        property_id=payload.get("property_id"),
        trigger_reason="CLOSED_LOST",
        db=session,
        deal_outcome_id=payload.get("deal_outcome_id"),
    )
    logger.info(
        "[OutcomeCons] loss autopsy run deal_outcome_id=%s reason=%s",
        payload.get("deal_outcome_id"), payload.get("dead_reason"),
    )


def handle_outcome_snapshot(session: Session, event_row) -> None:
    """Poll-consumer wrapper around apply_snapshot."""
    apply_snapshot(session, event_row.payload or {})


def handle_outcome_loss_autopsy(session: Session, event_row) -> None:
    """Poll-consumer wrapper around apply_loss_autopsy."""
    apply_loss_autopsy(session, event_row.payload or {})
