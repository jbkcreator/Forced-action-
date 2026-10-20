"""Record an approved / funded outcome against a file's current lender.

The seat cap counts ``SEAT_CAP_COUNTED_ACTION`` events (default ``funded``), and nothing else
writes them, so this is the production path that moves the cap.  Append-only: it never edits
an earlier event.  The file is a LendingFlow lead or a partner file; for a partner file the
partner is told (its status moves to approved / funded).

Idempotent: the same action for the same lender on the same file is recorded once.

A Slack click passes the ``routed`` event its card shows (``routed_event_id``); it is checked again after the
routing row is locked, so a decline that re-routed the file in between can never move the outcome to the next
lender (``StaleRoute``). Funding is terminal: once a file has a ``funded`` event, an approval is refused
(``AlreadyFunded``) and another funding is a no-op, both decided under the same lock.

The partner-file status update runs after the commit and can fail; the committed outcome event is the record of
what the file must show, and ``partner_link.resync_outcomes`` (partner sweep) applies it until it sticks.
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

# The events that end a "routed" event as the file's current route (an approval or a funding does not).
_ROUTE_ENDING_ACTIONS = ("routed", "declined", "held")


class StaleRoute(ValueError):
    """The clicked routing event is no longer the file's current route; nothing was recorded."""


class AlreadyFunded(ValueError):
    """The file is already funded; an approval would move it backwards. Nothing was recorded."""


def _still_current(db: Session, key: FileKey, routed_event_id: int, current_lender_id: int) -> bool:
    """Whether ``routed_event_id`` is this file's current route. Run after the routing row is locked: a decline
    takes that lock too, so nothing can re-route the file between this check and the insert."""
    event = db.execute(
        text(f"SELECT action, lender_id, {key.column} AS file_id FROM lending.file_routing_events WHERE id = :e"),
        {"e": routed_event_id},
    ).mappings().one_or_none()
    if event is None or event["action"] != "routed" or event["file_id"] != key.id             or event["lender_id"] != current_lender_id:
        return False
    newer = db.execute(
        text(f"SELECT 1 FROM lending.file_routing_events WHERE {key.column} = :id AND id > :e"
             " AND action = ANY(:ending) LIMIT 1"),
        {"id": key.id, "e": routed_event_id, "ending": list(_ROUTE_ENDING_ACTIONS)},
    ).scalar()
    return not newer


def record_outcome(
    file_id: int,
    *,
    action: str,
    actor: str,
    db: Session,
    free_text: Optional[str] = None,
    kind: str = LEAD,
    routed_event_id: Optional[int] = None,
) -> bool:
    """Append an ``approved`` / ``funded`` event for the file's current lender.

    Returns True when an event was written, False when it was already recorded.
    Raises ValueError for an unknown action, a file with no routing row, or a file that is
    not currently routed to a lender; ``StaleRoute`` when ``routed_event_id`` is given and is no longer the
    file's current route; ``AlreadyFunded`` for an approval of a funded file.
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
        raise ValueError("no routing record for this file")
    if routing["status"] != "routed" or routing["current_lender_id"] is None:
        raise ValueError(f"the file is {routing['status']!r}, not routed to a lender; cannot record {action}")
    if routed_event_id is not None and not _still_current(db, key, routed_event_id, routing["current_lender_id"]):
        raise StaleRoute("the clicked lender is no longer the file's current route; nothing recorded")

    funded = db.execute(
        text(f"SELECT 1 FROM lending.file_routing_events WHERE {key.column} = :id AND action = 'funded' LIMIT 1"),
        {"id": key.id},
    ).scalar()
    if funded:  # terminal, checked under the routing lock: a queued Approved click cannot undo it
        if action != "funded":
            raise AlreadyFunded(f"the file is already funded; {action} not recorded")
        logger.info("record_outcome %s=%d action=funded already funded - no-op", key.column, key.id)
        return False

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
