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
and the partner sweep re-applies it (``resync_outcomes``) until it sticks.  The outcome event itself is the
pending record: it commits atomically with the outcome, and the repair never writes another one.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

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


def resync_outcomes(lagging: list[tuple[int, str]]) -> None:
    """Give each file the status of its latest outcome (and its partner the notice). Call outside any routing
    transaction. A failure is logged by ``mirror_status`` and the file is found again on the next sweep."""
    for file_id, outcome in lagging:
        mirror_status(FileKey(LOAN_FILE, file_id), outcome)
