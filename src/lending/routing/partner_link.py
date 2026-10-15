"""Keep a partner file's status, and its partner, in step with the router.

A partner file (lending.loan_files) shows the partner a status and sends them a notice when it moves.  The
Partner Door owns that: ``partner_pipeline.change_status`` records the status, commits, and tells the partner,
in one step, so no change can skip the notice.  The router calls it here on every route, decline, approval,
funding and hold of a partner file.  LendingFlow leads have no partner, so for them this does nothing.

Call it only after the router's own transaction is committed: ``change_status`` opens its own session and
updates the same file row, so it would otherwise wait on a lock this transaction still holds.

Best effort: a failure is logged at ERROR and never undoes or blocks the routing decision, which is already
committed and recorded in ``file_routing_events``.  The file's status can then lag its routing state until the
next routing event rewrites it.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

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
