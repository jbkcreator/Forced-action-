"""
Follow-up scheduling — C2a addendum (docs/plans/cora_relay_followup_qa.md).

Periodic sweep, not an event handler: reads Cora's own opportunity_state for
threads stuck at "touched" (targeted -> touched -> [never advanced]) and, for
any that have crossed the day-2 or day-5 cadence offset since the PARENT
touch actually went out, drafts a follow-up via the same outreach.py path
Cora already uses for a first touch — every follow-up is a fresh draft
requiring fresh approval, same as the original (no authority-ladder
progression, ever).

Anchor for "the parent touch actually went out": ideally a read-only lookup
of Relay's `relay_approval_queue.dispatched_at`, matched by
opportunity_thread_id (free-text match against `relay_approval_queue.thread_id`
— no FK requested, per the Q&A). That table does not exist in this database
as of this build (confirmed via a repo-wide search — Relay's own schema work
is separate, unmerged planning), so the lookup is wrapped defensively and
falls back to the parent OutboundDraftRecord's own created_at as the anchor.
This is an honest interim substitute, not a guess: Cora's own persisted
draft time is a real timestamp, just not confirmation that Relay actually
sent it. Closeable once relay_approval_queue exists — see _dispatched_at().

Cadence is a fixed, universal day-2 + day-5 offset, 2 touches total — a
placeholder pending REVINT/Learning-Engine cadence optimization later, per
the Q&A answer (not the existing Lifecycle runtime's followup_cadence_v1 A/B
arm, which is a different, later-stage pattern).

A reply anywhere in the sequence flips opportunity_state to "replied", which
immediately makes is_awaiting_reply() False — so the next sweep simply stops
proposing further touches for that thread. No separate "cancel" action
needed.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.cora import opportunity_state, store
from src.agents.cora.subgraphs import outreach
from src.agents.cora.tools.read_tools import get_buyer_entity_by_opportunity_thread_id, get_contact_channel

logger = logging.getLogger(__name__)

FOLLOWUP_OFFSETS_DAYS: Dict[int, int] = {1: 2, 2: 5}  # sequence -> days since anchor
MAX_FOLLOWUPS = max(FOLLOWUP_OFFSETS_DAYS)

_warned_no_relay_table = False


def _dispatched_at(db: Session, opportunity_thread_id: str) -> Optional[str]:
    """Read-only lookup of Relay's dispatch timestamp. None if unavailable for any reason."""
    global _warned_no_relay_table
    try:
        row = db.execute(
            text("SELECT dispatched_at FROM relay_approval_queue WHERE thread_id = :tid ORDER BY dispatched_at DESC LIMIT 1"),
            {"tid": opportunity_thread_id},
        ).mappings().first()
        return str(row["dispatched_at"]) if row and row.get("dispatched_at") else None
    except Exception as exc:  # noqa: BLE001
        db.rollback()  # relay_approval_queue not existing yet aborts this connection's transaction
        if not _warned_no_relay_table:
            logger.warning(
                "followup_scheduler: relay_approval_queue unavailable (%s) — falling back to Cora's own "
                "draft created_at as the follow-up cadence anchor for every thread this sweep",
                type(exc).__name__,
            )
            _warned_no_relay_table = True
        return None


def _anchor_for(db: Session, opportunity_thread_id: str, parent_draft: Dict[str, Any]) -> Optional[str]:
    return _dispatched_at(db, opportunity_thread_id) or parent_draft.get("created_at")


def _next_due_sequence(db: Session, opportunity_thread_id: str, anchor_iso: str) -> Optional[int]:
    followups = [
        d for d in store.read_drafts(db, opportunity_thread_id=opportunity_thread_id)
        if d.get("is_followup")
    ]
    sent_sequences = {d.get("followup_sequence") for d in followups if d.get("followup_sequence") is not None}
    if len(sent_sequences) >= MAX_FOLLOWUPS:
        return None

    anchor = store.parse_dt(anchor_iso)
    if anchor is None:
        return None
    now = store.now()

    for sequence in sorted(FOLLOWUP_OFFSETS_DAYS):
        if sequence in sent_sequences:
            continue
        due_at = anchor + timedelta(days=FOLLOWUP_OFFSETS_DAYS[sequence])
        if now >= due_at:
            return sequence
        return None  # cadence is sequential — don't skip ahead to a later offset
    return None


def _parent_draft(db: Session, opportunity_thread_id: str) -> Optional[Dict[str, Any]]:
    non_followups = [
        d for d in store.read_drafts(db, opportunity_thread_id=opportunity_thread_id)
        if not d.get("is_followup")
    ]
    if not non_followups:
        return None
    return max(non_followups, key=lambda d: d.get("created_at") or "")


def run_followup_sweep(db: Session) -> List[Dict[str, Any]]:
    """Returns one result dict per follow-up actually drafted this sweep (for logging/tests)."""
    results: List[Dict[str, Any]] = []
    eligible_threads = store.list_opportunities_by_status("touched")

    for opportunity_thread_id in eligible_threads:
        if not opportunity_state.is_awaiting_reply(opportunity_thread_id):
            continue  # raced with a reply/state change since list_opportunities_by_status ran

        parent = _parent_draft(db, opportunity_thread_id)
        if parent is None:
            logger.warning(
                "followup_scheduler: thread=%s is 'touched' but has no parent draft on file — skipping",
                opportunity_thread_id,
            )
            continue

        anchor = _anchor_for(db, opportunity_thread_id, parent)
        if anchor is None:
            continue

        sequence = _next_due_sequence(db, opportunity_thread_id, anchor)
        if sequence is None:
            continue

        buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, opportunity_thread_id)
        if buyer_entity is None:
            logger.warning(
                "followup_scheduler: thread=%s buyer_entity no longer resolvable — skipping followup_sequence=%d",
                opportunity_thread_id, sequence,
            )
            continue

        contact = get_contact_channel(db, buyer_entity["id"])
        result = outreach.run_outreach(
            {
                "buyer_entity": buyer_entity,
                "cell_id": parent["cell_id"],
                "facts_used": parent.get("facts_used", []),
                "contact_email": contact.get("email"),
                "contact_phone": contact.get("phone"),
                "is_followup": True,
                "followup_sequence": sequence,
            },
            db=db,
        )
        logger.info(
            "followup_scheduler: thread=%s followup_sequence=%d terminal_status=%s reject_reason=%s",
            opportunity_thread_id, sequence, result.get("terminal_status"), result.get("reject_reason"),
        )
        results.append({"opportunity_thread_id": opportunity_thread_id, "followup_sequence": sequence, **result})

    return results
