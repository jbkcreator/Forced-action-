"""GHL pipeline stage -> loan state machine (R1 0.1, grill D5/D7).

Josh moves a deal by hand in GHL; the ``/webhooks/lending/ghl-stage`` webhook hands the stage here:

* "Funded" -> ``transition(FUNDED)``.
* "Paid"   -> a payment event (``record_payment(mark_paid=True)``), which moves a FUNDED file to PAID.  On a file that
  is not FUNDED the move is refused like any other skipped stage.

Every event is keyed by ``<opportunity id>:<stage>``, so a replayed webhook changes nothing.  A refused move is
stored as a ``rejected`` stage event with a card for Josh; the webhook still answers 200.  A deal that cannot be tied
to a loan file unambiguously is not moved; a card says so: the borrower's phone must belong to exactly ONE loan file,
open or closed, because a GHL event carries no loan file id and a borrower with two deals could otherwise move the
wrong one (PR #473 review). The replay key also names the file's current stage (``<opportunity>:<stage>@<file
stage>``): a redelivery changes nothing, but a move refused because it came too early is applied when the deal is
moved again after the file has caught up.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.contracts import LoanStage
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.state_machine import IllegalStageTransitionError, record_payment, transition

logger = logging.getLogger(__name__)

SOURCE = "ghl_stage"
ACTOR = "system:ghl_stage"

# GHL stage name (lower-cased) -> loan stage.  Only stages whose meaning is certain; others leave the file alone.
GHL_STAGE_TO_LOAN_STAGE: dict[str, LoanStage] = {"funded": LoanStage.FUNDED, "paid": LoanStage.PAID}

_FILES_FOR_PHONE = text(
    "SELECT id, stage FROM lending.loan_files WHERE borrower_phone = :phone ORDER BY id LIMIT 2")


def _unmatched(db: Session, opportunity_id: str, stage_key: str, why: str) -> None:
    add_to_inbox_safely(
        db, event_type="ghl_stage_unmatched", dedup_key=f"ghl-stage-unmatched:{opportunity_id}:{stage_key}",
        summary=f"GHL deal {opportunity_id} moved to \"{stage_key}\", but {why}. Move the loan file by hand.",
        payload={"ghl_opportunity_id": opportunity_id, "stage": stage_key, "reason": why})


def apply_stage_payload(db: Session, payload: dict) -> Optional[str]:
    """Replay entry point for ``lending.webhook_failures``: the saved event, applied the same way."""
    return apply_stage(db, opportunity_id=str(payload["opportunity_id"]), stage_name=str(payload["stage_name"]),
                       phone=payload.get("phone"))


def apply_stage(db: Session, *, opportunity_id: str, stage_name: str, phone: Optional[str]) -> Optional[str]:
    """Apply a GHL stage event to its loan file.  Returns the outcome (``applied``, ``noop``, ``duplicate``,
    ``rejected``, ``unmatched``) or None when the stage has no loan-stage meaning.  The caller commits."""
    stage_key = stage_name.strip().lower()
    target = GHL_STAGE_TO_LOAN_STAGE.get(stage_key)
    if target is None:
        return None
    files = db.execute(_FILES_FOR_PHONE, {"phone": phone}).all() if phone else []
    if len(files) != 1:
        _unmatched(db, opportunity_id, stage_key,
                   "no loan file has that phone" if not files else
                   "the borrower has more than one loan file, so the deal cannot be matched safely")
        return "unmatched"
    file_id, current = int(files[0].id), LoanStage(files[0].stage)
    event_id = f"{opportunity_id}:{stage_key}@{current.value}"
    try:
        if target is LoanStage.PAID and current is LoanStage.FUNDED:
            result = record_payment(db, file_id, source=SOURCE, payment_id=event_id, actor=ACTOR, mark_paid=True)
            return "applied" if result.paid else "duplicate"
        return transition(db, file_id, target, source=SOURCE, actor=ACTOR, source_event_id=event_id).outcome
    except IllegalStageTransitionError:
        return "rejected"  # stored as a rejected event, card queued (state_machine.transition)
