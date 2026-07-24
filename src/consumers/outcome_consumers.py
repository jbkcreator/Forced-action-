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
deal_outcome_id guard) and freshness-checked (a superseded event — the buyer
changed the outcome again before this one was processed — is skipped rather
than writing a stale artifact). They receive a row-like object with ``payload``.
"""
from __future__ import annotations

import logging

from sqlalchemy import text as sa_text
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


def _is_current(session: Session, payload: dict) -> bool:
    """Whether this event's outcome still matches the DealOutcome row today.

    A subscriber can change an outcome (e.g. closed -> dead) before an earlier
    event for the same deal is processed — each update emits its own event but
    reuses the same deal_outcome_id, and snapshot/autopsy are idempotent on that
    id. Without this check, a stale event processed after a newer one would
    either write the wrong learning artifact or (since idempotent) be silently
    ignored while the correct state never gets captured, depending on
    processing order. Comparing against pipeline_stage — always populated,
    including on the legacy bucket path — and reason_fault_class detects
    whether *this* event is still the deal's current state; a superseded event
    is skipped rather than acted on.
    """
    deal_outcome_id = payload.get("deal_outcome_id")
    if deal_outcome_id is None:
        return True
    row = session.execute(
        sa_text(
            "SELECT pipeline_stage, reason_fault_class FROM deal_outcomes WHERE id = :id"
        ),
        {"id": deal_outcome_id},
    ).mappings().one_or_none()
    if row is None:
        return True
    return (
        row["pipeline_stage"] == payload.get("pipeline_stage")
        and row["reason_fault_class"] == payload.get("reason_fault_class")
    )


def apply_snapshot(session: Session, payload: dict) -> None:
    """Capture the pre-decision score snapshot for a scoring-eligible outcome.

    Pure payload entry point — used by the async handler and by deal-capture's
    inline fallback (property with no prospect row for the prospect-scoped outbox).

    Raises on a genuine capture_snapshot failure (distinct from its own
    already-exists idempotency, which is a normal no-op) so poll_and_dispatch
    records and retries it instead of silently marking the event processed.
    """
    if not _feeds_scoring(payload):
        logger.info(
            "[OutcomeCons] snapshot skipped (score-protected) deal_outcome_id=%s state=%s fault=%s",
            payload.get("deal_outcome_id"), payload.get("outcome_state"),
            payload.get("reason_fault_class"),
        )
        return
    if not _is_current(session, payload):
        logger.info(
            "[OutcomeCons] snapshot skipped (superseded by a newer outcome) deal_outcome_id=%s",
            payload.get("deal_outcome_id"),
        )
        return

    deal_outcome_id = payload.get("deal_outcome_id")
    status = "funded" if payload.get("outcome_state") == "closed" else "lost"

    already_exists = deal_outcome_id is not None and session.execute(
        sa_text("SELECT 1 FROM pre_decision_snapshots WHERE deal_outcome_id = :did"),
        {"did": deal_outcome_id},
    ).first() is not None

    from src.services.snapshot_service import capture_snapshot
    result = capture_snapshot(
        property_id=payload.get("property_id"),
        db=session,
        deal_outcome_id=deal_outcome_id,
        selected_vertical=payload.get("selected_vertical"),
        outcome_status=status,
    )
    if result is None and not already_exists:
        raise RuntimeError(
            f"capture_snapshot failed for deal_outcome_id={deal_outcome_id} "
            f"property_id={payload.get('property_id')} — see snapshot_service logs"
        )
    logger.info(
        "[OutcomeCons] snapshot captured deal_outcome_id=%s status=%s",
        deal_outcome_id, status,
    )


def apply_loss_autopsy(session: Session, payload: dict) -> None:
    """Run the loss autopsy for a lead-fault dead outcome only.

    Pure payload entry point — see apply_snapshot. Raises on a genuine
    run_loss_autopsy failure (distinct from its own already-run idempotency)
    so poll_and_dispatch records and retries it.
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
    if not _is_current(session, payload):
        logger.info(
            "[OutcomeCons] loss autopsy skipped (superseded by a newer outcome) deal_outcome_id=%s",
            payload.get("deal_outcome_id"),
        )
        return

    deal_outcome_id = payload.get("deal_outcome_id")
    already_exists = deal_outcome_id is not None and session.execute(
        sa_text("SELECT 1 FROM loss_autopsies WHERE deal_outcome_id = :d"),
        {"d": deal_outcome_id},
    ).first() is not None

    from src.services.loss_autopsy import run_loss_autopsy
    result = run_loss_autopsy(
        property_id=payload.get("property_id"),
        trigger_reason="CLOSED_LOST",
        db=session,
        deal_outcome_id=deal_outcome_id,
    )
    if result is None and not already_exists:
        raise RuntimeError(
            f"run_loss_autopsy failed for deal_outcome_id={deal_outcome_id} "
            f"property_id={payload.get('property_id')} — see loss_autopsy logs"
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
