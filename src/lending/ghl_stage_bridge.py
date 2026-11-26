"""GHL pipeline stage -> loan state machine (R1 0.1, grill D5/D7).

Josh moves a deal by hand in GHL; the ``/webhooks/lending/ghl-stage`` webhook hands the stage here:

* "Funded" -> ``transition(FUNDED)``.
* "Paid"   -> a payment event (``record_payment(mark_paid=True)``), which moves a FUNDED file to PAID.  On a file that
  is CLOSING, a recorded Funded webhook allows the bridge to apply both steps in sequence.

Which file: only the file **linked** to the GHL opportunity (``loan_files.ghl_opportunity_id``). A GHL event carries no
loan file id, and a borrower can have more deals in GHL than files here, so a phone match could move the wrong loan
(PR #473 review). A deal with no linked file moves nothing; Cora's inbox gets a card saying how to link it::

    python -m src.lending.ghl_stage_bridge --link <loan file id> <GHL opportunity id>

Linking applies the deal's current GHL stage straight away.

Stale events: the event is applied only while it is still the deal's current GHL stage (the webhook records the current
stage first, and holds redeliveries). A late copy of "Funded" that arrives after the deal moved on (to Lost, say) is
skipped.

Replays: every event is keyed ``<opportunity>:<stage>@<file stage>``, so a redelivery changes nothing, while a move
refused because it came too early applies when the deal is moved again after the file caught up. A refused move is
stored as a ``rejected`` stage event with a card for Josh; the webhook still answers 200.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.closing.blockers import current_stage
from src.lending.contracts import LoanStage
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.state_machine import IllegalStageTransitionError, record_payment, transition

logger = logging.getLogger(__name__)

SOURCE = "ghl_stage"
ACTOR = "system:ghl_stage"
OUTCOME_UNLINKED = "unlinked"
OUTCOME_STALE = "stale"

# GHL stage name (lower-cased) -> loan stage.  Only stages whose meaning is certain; others leave the file alone.
GHL_STAGE_TO_LOAN_STAGE: dict[str, LoanStage] = {"funded": LoanStage.FUNDED, "paid": LoanStage.PAID}

_FILE_FOR_OPPORTUNITY = text("SELECT id, stage FROM lending.loan_files WHERE ghl_opportunity_id = :opp")
_LINK = text("""
    UPDATE lending.loan_files SET ghl_opportunity_id = :opp, updated_at = now()
     WHERE id = :id AND (ghl_opportunity_id IS NULL OR ghl_opportunity_id = :opp)
       AND NOT EXISTS (SELECT 1 FROM lending.loan_files o WHERE o.ghl_opportunity_id = :opp AND o.id <> :id)
""")


def _unlinked(db: Session, opportunity_id: str, stage_key: str) -> None:
    add_to_inbox_safely(
        db, event_type="ghl_stage_unlinked", dedup_key=f"ghl-stage-unlinked:{opportunity_id}:{stage_key}",
        summary=(f"GHL deal {opportunity_id} moved to \"{stage_key}\", but no loan file is linked to it, so nothing "
                 f"moved. Link it: python -m src.lending.ghl_stage_bridge --link <loan file id> {opportunity_id}"),
        payload={"ghl_opportunity_id": opportunity_id, "stage": stage_key})


def _still_current(db: Session, opportunity_id: str, stage_key: str) -> bool:
    current = current_stage(db, opportunity_id)
    return current is not None and current[0].strip().lower() == stage_key


def apply_stage_payload(db: Session, payload: dict) -> Optional[str]:
    """Replay entry point for ``lending.webhook_failures``: the saved event, applied the same way."""
    return apply_stage(db, opportunity_id=str(payload["opportunity_id"]), stage_name=str(payload["stage_name"]),
                       phone=payload.get("phone"))


def apply_stage(db: Session, *, opportunity_id: str, stage_name: str, phone: Optional[str] = None) -> Optional[str]:
    """Apply a GHL stage event to the loan file linked to its opportunity.  Returns the outcome (``applied``, ``noop``,
    ``duplicate``, ``rejected``, ``unlinked``, ``stale``) or None when the stage has no loan-stage meaning.  ``phone``
    is accepted for the webhook's payload and never used to pick a file.  The caller commits."""
    stage_key = stage_name.strip().lower()
    target = GHL_STAGE_TO_LOAN_STAGE.get(stage_key)
    if target is None:
        return None
    if not _still_current(db, opportunity_id, stage_key):
        logger.info("[ghl-stage-bridge] %s skipped: no longer the deal's current GHL stage", stage_key)
        return OUTCOME_STALE
    row = db.execute(_FILE_FOR_OPPORTUNITY, {"opp": opportunity_id}).first()
    if row is None:
        _unlinked(db, opportunity_id, stage_key)
        return OUTCOME_UNLINKED
    file_id, current = int(row.id), LoanStage(row.stage)
    event_id = f"{opportunity_id}:{stage_key}@{current.value}"
    try:
        if target is LoanStage.PAID and current is LoanStage.CLOSING and db.execute(text(
            "SELECT EXISTS (SELECT 1 FROM lending.ghl_stage_events "
            "WHERE ghl_opportunity_id = :opp AND stage_key = 'funded' "
            "AND raw_event->>'source' IS DISTINCT FROM 'reconcile')"
        ), {"opp": opportunity_id}).scalar():
            transition(db, file_id, LoanStage.FUNDED, source=SOURCE, actor=ACTOR,
                       source_event_id=f"{opportunity_id}:funded@{current.value}")
            current = LoanStage.FUNDED
        if target is LoanStage.PAID and current is LoanStage.FUNDED:
            result = record_payment(db, file_id, source=SOURCE, payment_id=event_id, actor=ACTOR, mark_paid=True)
            return "applied" if result.paid else "duplicate"
        return transition(db, file_id, target, source=SOURCE, actor=ACTOR, source_event_id=event_id).outcome
    except IllegalStageTransitionError:
        return "rejected"  # stored as a rejected event, card queued (state_machine.transition)


def link(db: Session, loan_file_id: int, opportunity_id: str) -> Optional[str]:
    """A person ties a loan file to its GHL deal, then the deal's current GHL stage is applied (None when that stage
    has no loan meaning). Raises ValueError when the file does not exist or either side is already linked elsewhere.
    The caller commits."""
    if db.execute(_LINK, {"id": loan_file_id, "opp": opportunity_id}).rowcount != 1:
        raise ValueError(f"loan file {loan_file_id} not found, or it or GHL deal {opportunity_id} is already linked "
                         "to something else")
    current = current_stage(db, opportunity_id)
    return apply_stage(db, opportunity_id=opportunity_id, stage_name=current[0]) if current else None


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Link a loan file to its GHL deal (R1 0.1)")
    parser.add_argument("--link", nargs=2, metavar=("LOAN_FILE_ID", "GHL_OPPORTUNITY_ID"), required=True)
    args = parser.parse_args(argv)
    from src.lending.db import lending_session

    try:
        with lending_session() as db:
            outcome = link(db, int(args.link[0]), args.link[1])
    except ValueError as exc:
        sys.stdout.write(f"{exc}\n")
        return 2
    sys.stdout.write(f"linked; current GHL stage applied: {outcome or 'nothing to apply'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
