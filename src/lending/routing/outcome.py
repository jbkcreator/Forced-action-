"""Record an approved / funded outcome against a file's current lender.

The seat cap counts ``SEAT_CAP_COUNTED_ACTION`` events (default ``funded``), and nothing else
writes them, so this is the production path that moves the cap.  Append-only: it never edits
an earlier event.  The file is a LendingFlow lead or a partner file; for a partner file the
partner is told (its status moves to approved / funded).

Idempotent: the same action for the same lender on the same file is recorded once.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import OUTCOME_ACTIONS
from src.lending.routing import partner_link
from src.lending.routing.file_key import LEAD, FileKey

logger = logging.getLogger(__name__)


def record_outcome(
    file_id: int,
    *,
    action: str,
    actor: str,
    db: Session,
    free_text: Optional[str] = None,
    kind: str = LEAD,
) -> bool:
    """Append an ``approved`` / ``funded`` event for the file's current lender.

    Returns True when an event was written, False when it was already recorded.
    Raises ValueError for an unknown action, a file with no routing row, or a file that is
    not currently routed to a lender.
    """
    if action not in OUTCOME_ACTIONS:
        raise ValueError(f"action must be one of {OUTCOME_ACTIONS}, got {action!r}")

    key = FileKey(kind, file_id)
    routing = db.execute(
        text(
            "SELECT status, current_lender_id, current_lender_name"
            f"  FROM lending.file_routing WHERE {key.column} = :id FOR UPDATE"
        ),
        {"id": key.id},
    ).mappings().one_or_none()
    if routing is None:
        raise ValueError(f"no routing row for {key.column}={key.id}")
    if routing["status"] != "routed" or routing["current_lender_id"] is None:
        raise ValueError(
            f"{key.column}={key.id} is {routing['status']!r}, not routed to a lender; cannot record {action}"
        )

    already = db.execute(
        text(
            "SELECT 1 FROM lending.file_routing_events"
            f" WHERE {key.column} = :id AND lender_id = :lender AND action = :action LIMIT 1"
        ),
        {"id": key.id, "lender": routing["current_lender_id"], "action": action},
    ).scalar()
    if already:
        logger.info("record_outcome %s=%d action=%s already recorded - no-op", key.column, key.id, action)
        return False

    db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            f"  ({key.column}, lender_id, lender_name, action, free_text, actor)"
            " VALUES (:id, :lender, :name, :action, :free, :actor)"
        ),
        {
            "id": key.id, "lender": routing["current_lender_id"],
            "name": routing["current_lender_name"], "action": action,
            "free": free_text, "actor": actor,
        },
    )
    db.commit()
    logger.info("record_outcome %s=%d lender_id=%d action=%s actor=%s",
                key.column, key.id, routing["current_lender_id"], action, actor)
    partner_link.mirror_status(key, action)
    return True
