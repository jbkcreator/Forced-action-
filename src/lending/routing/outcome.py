"""Record an approved / funded outcome against a file's current lender.

The seat cap counts ``SEAT_CAP_COUNTED_ACTION`` events (default ``funded``), and nothing else
writes them, so this is the production path that moves the cap.  Append-only: it never edits
an earlier event.

Idempotent: the same action for the same lender on the same file is recorded once.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import OUTCOME_ACTIONS

logger = logging.getLogger(__name__)


def record_outcome(
    lead_id: int,
    *,
    action: str,
    actor: str,
    db: Session,
    free_text: Optional[str] = None,
) -> bool:
    """Append an ``approved`` / ``funded`` event for the file's current lender.

    Returns True when an event was written, False when it was already recorded.
    Raises ValueError for an unknown action, a file with no routing row, or a file that is
    not currently routed to a lender.
    """
    if action not in OUTCOME_ACTIONS:
        raise ValueError(f"action must be one of {OUTCOME_ACTIONS}, got {action!r}")

    routing = db.execute(
        text(
            "SELECT status, current_lender_id, current_lender_name"
            "  FROM lending.file_routing WHERE lead_id = :lid FOR UPDATE"
        ),
        {"lid": lead_id},
    ).mappings().one_or_none()
    if routing is None:
        raise ValueError(f"no routing row for lead_id={lead_id}")
    if routing["status"] != "routed" or routing["current_lender_id"] is None:
        raise ValueError(
            f"lead_id={lead_id} is {routing['status']!r}, not routed to a lender; cannot record {action}"
        )

    already = db.execute(
        text(
            "SELECT 1 FROM lending.file_routing_events"
            " WHERE lead_id = :lid AND lender_id = :lender AND action = :action LIMIT 1"
        ),
        {"lid": lead_id, "lender": routing["current_lender_id"], "action": action},
    ).scalar()
    if already:
        logger.info("record_outcome lead_id=%d action=%s already recorded — no-op", lead_id, action)
        return False

    db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            "  (lead_id, lender_id, lender_name, action, free_text, actor)"
            " VALUES (:lid, :lender, :name, :action, :free, :actor)"
        ),
        {
            "lid": lead_id, "lender": routing["current_lender_id"],
            "name": routing["current_lender_name"], "action": action,
            "free": free_text, "actor": actor,
        },
    )
    db.commit()
    logger.info("record_outcome lead_id=%d lender_id=%d action=%s actor=%s",
                lead_id, routing["current_lender_id"], action, actor)
    return True
