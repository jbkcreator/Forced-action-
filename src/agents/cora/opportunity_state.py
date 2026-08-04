"""
Opportunity state machine — targeted -> touched -> replied -> call ->
proposal -> closed, keyed on Hunter's existing opportunity_thread_id.
Never mints a second id.

Thin, named wrapper over store.py's generic transition_opportunity(), so
callers (subgraphs) read as business logic rather than raw store calls.

mark_replied() additionally dual-writes outbound_drafts.replied_at (CL4). The
file store stays canonical for opportunity state; the column exists because
reply RATE has to be answerable in SQL, grouped by (venture_key, cell_id), and
the file store cannot serve that query — it is gitignored, guarded by a
single-process threading.Lock, and read by de-duplicating append-only
transitions at read time. src/services/venture_ladder.py scales real sending
volume off that number, so it reads the indexed column instead.
"""
from __future__ import annotations

import logging
from typing import Optional

from src.agents.cora import store

logger = logging.getLogger(__name__)


def mark_targeted(opportunity_thread_id: str, reason: str = "target_produced") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "targeted", reason)


def mark_touched(opportunity_thread_id: str, reason: str = "draft_approved_sent") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "touched", reason)


def mark_replied(opportunity_thread_id: str, reason: str = "reply_received") -> bool:
    transitioned = store.transition_opportunity(opportunity_thread_id, "replied", reason)
    _stamp_replied_at(opportunity_thread_id)
    return transitioned


def _stamp_replied_at(opportunity_thread_id: str) -> None:
    """Set outbound_drafts.replied_at for this thread's drafts (CL4).

    Best-effort and never raises: the file-store transition above is what the
    follow-up scheduler and every other caller depend on, and a DB hiccup must
    not turn a received reply into an unrecorded one. A miss here understates a
    reply rate, which can only make the auto-double rule more conservative —
    the safe direction to fail in.

    Only stamps rows that are still NULL, so a second reply on the same thread
    keeps the first reply's timestamp rather than sliding it forward out of the
    window it belongs to.
    """
    try:
        from sqlalchemy import text

        from src.core.database import get_db_context

        with get_db_context() as db:
            db.execute(
                text("""
                    UPDATE outbound_drafts
                    SET replied_at = now()
                    WHERE opportunity_thread_id = :thread_id
                      AND replied_at IS NULL
                """),
                {"thread_id": opportunity_thread_id},
            )
            db.commit()
    except Exception:
        logger.warning(
            "[cora] could not stamp replied_at for thread %s — the file-store "
            "transition still applies; reply-rate metrics will understate this reply",
            opportunity_thread_id, exc_info=True,
        )


def mark_call(opportunity_thread_id: str, reason: str = "call_booked") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "call", reason)


def mark_proposal(opportunity_thread_id: str, reason: str = "proposal_sent") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "proposal", reason)


def mark_closed(opportunity_thread_id: str, reason: str) -> bool:
    return store.transition_opportunity(opportunity_thread_id, "closed", reason)


def current_status(opportunity_thread_id: str) -> Optional[str]:
    return store.current_opportunity_status(opportunity_thread_id)


def is_awaiting_reply(opportunity_thread_id: str) -> bool:
    """True if the opportunity has been touched but hasn't advanced past it —
    the exact eligibility condition the follow-up scheduler checks."""
    return current_status(opportunity_thread_id) == "touched"
