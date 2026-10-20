"""Keep a partner file's status, and its partner, in step with the router.

A partner file (lending.loan_files) shows the partner a status and sends them a notice when it moves.  The
Partner Door owns that: ``partner_pipeline.change_status`` records the status, commits, and tells the partner,
in one step, so no change can skip the notice.  The router calls it here on every route, decline, approval,
funding and hold of a partner file.  LendingFlow leads have no partner, so for them this does nothing.

Call it only after the router's own transaction is committed: ``change_status`` opens its own session and
updates the same file row, so it would otherwise wait on a lock this transaction still holds.

Best effort: a failure is logged at ERROR and never undoes or blocks the routing decision, which is already
committed and recorded in ``file_routing_events``.  For an approval or funding there may be no next routing
event, so ``lagging_outcomes`` finds a partner file whose latest event is that outcome but whose status is not,
and the partner sweep re-applies it (``resync_outcomes``) until it sticks: each file is re-checked and written
under its routing-row lock in one transaction.  The outcome event itself is the pending record: it commits
atomically with the outcome, and the repair never writes another one.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text

from config.lending_routing import OUTCOME_ACTIONS

from src.lending.routing.file_key import LOAN_FILE, FileKey

logger = logging.getLogger(__name__)

# How a routing outcome reads on the shared file: lending.loan_files.status only knows these six.
STATUS_FOR_HOLD = "held"  # held and exhausted both read "held" on the file; the reason keeps them apart


def resync_if_never_synced(key: FileKey, routing_status: str, hold_reason: Optional[str], db) -> None:
    """Give a partner file the status its routing already reached, but only if it still says ``received``.

    Covers a crash or failure between the router's commit and the status sync.  A file that has taken any
    status since (routed, declined, approved, funded) is left alone, so this can never undo an outcome.
    """
    if key.kind != LOAN_FILE:
        return
    current = db.execute(text("SELECT status FROM lending.loan_files WHERE id = :id"), {"id": key.id}).scalar()
    if current != "received":
        return
    if routing_status == "routed":
        mirror_status(key, "routed")
    else:  # held or exhausted
        mirror_status(key, STATUS_FOR_HOLD, hold_reason)


def mirror_status(key: FileKey, status: str, reason: Optional[str] = None) -> None:
    """Move a partner file to ``status`` (routed / declined / approved / funded / held). No-op for other files."""
    if key.kind != LOAN_FILE:
        return
    try:
        from src.lending.partner_pipeline import change_status

        change_status(key.id, status, reason)
    except Exception as exc:  # noqa: BLE001 - routing must not fail because the partner notice did
        logger.error("partner status sync failed file=%d status=%s (%s)", key.id, status, type(exc).__name__)


# A partner file whose latest routing event is an outcome (approved / funded) but whose status is something else:
# the status update after that outcome's commit failed. Newest event per file; a later decline or re-route makes
# the outcome no longer the latest, and that event syncs the status itself.
_LAGGING_OUTCOMES = text("""
    WITH latest AS (
        SELECT DISTINCT ON (loan_file_id) loan_file_id, action, created_at
          FROM lending.file_routing_events
         WHERE loan_file_id IS NOT NULL
         ORDER BY loan_file_id, id DESC
    )
    SELECT l.loan_file_id, l.action
      FROM latest l JOIN lending.loan_files f ON f.id = l.loan_file_id
     WHERE l.action = ANY(:outcomes) AND f.status IS DISTINCT FROM l.action AND l.created_at < :cutoff
     ORDER BY l.created_at
     LIMIT :limit
""")


def lagging_outcomes(db, *, older_than_minutes: int, limit: int,
                     now: Optional[datetime] = None) -> list[tuple[int, str]]:
    """``(loan_file_id, outcome)`` for partner files that do not yet show their latest approval or funding.
    ``older_than_minutes`` leaves the immediate sync after the click time to finish first."""
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(minutes=older_than_minutes)
    rows = db.execute(_LAGGING_OUTCOMES, {"outcomes": list(OUTCOME_ACTIONS), "cutoff": cutoff, "limit": limit}).all()
    return [(int(row[0]), str(row[1])) for row in rows]


_LOCK_ROUTING = text("SELECT 1 FROM lending.file_routing WHERE loan_file_id = :id FOR UPDATE")
_LATEST_ACTION = text("SELECT action FROM lending.file_routing_events WHERE loan_file_id = :id ORDER BY id DESC LIMIT 1")


def resync_outcome(file_id: int, outcome: str, *, session_factory: Optional[Callable[[], Any]] = None,
                   services: Any = None) -> bool:
    """Give a partner file the status of its latest outcome, re-checked and written atomically.

    ``lagging_outcomes`` is a snapshot; a decline or re-route may have landed since. Under the file's routing-row
    lock (the lock a decline takes) this re-reads the latest event and writes the status only if that event is
    still ``outcome``, in one transaction, so a newer route can never be overwritten by an older outcome. The
    partner notice goes out after the commit. True when the status was applied. Never raises.
    """
    from src.lending.partner_pipeline import notify_partner_status
    from src.lending.partners import record_status_change

    if session_factory is None:
        from src.lending.db import lending_session as session_factory
    try:
        with session_factory() as db:
            locked = db.execute(_LOCK_ROUTING, {"id": file_id}).scalar()
            if not locked or db.execute(_LATEST_ACTION, {"id": file_id}).scalar() != outcome:
                return False
            change = record_status_change(db, file_id, outcome)
    except Exception as exc:  # noqa: BLE001 - logged; the file is found again on the next sweep
        logger.error("partner outcome resync failed file=%d status=%s (%s)", file_id, outcome, type(exc).__name__)
        return False
    if change is not None:
        notify_partner_status(change, services=services, session_factory=session_factory)
    return True


def resync_outcomes(lagging: list[tuple[int, str]], *, session_factory: Optional[Callable[[], Any]] = None,
                    services: Any = None) -> int:
    """``resync_outcome`` for each ``(file_id, outcome)`` from ``lagging_outcomes``. Call outside any routing
    transaction. Returns how many were applied; a skipped or failed file is found again on the next sweep."""
    return sum(resync_outcome(file_id, outcome, session_factory=session_factory, services=services)
               for file_id, outcome in lagging)
