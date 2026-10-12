"""Decline handler: record a lender decline and re-run the waterfall.

``record_decline`` is called by the decline endpoint.  It:
1. Validates the event is a 'routed' event belonging to the file's current lender.
2. Appends a 'declined' event.
3. Calls ``route_file`` to run the next pass of the waterfall.

The one-click decline flow (Q2 default):
  POST /api/lending/routing/events/{event_id}/decline
  Body: {"reason_code": "unspecified", "actor": "josh", "free_text": ""}

OPEN Q2: One-click records reason 'unspecified' and returns.  Josh to confirm
whether the reason can be updated in a later call.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import DEFAULT_DECLINE_REASON
from src.lending.routing.notify import post_decline_event

logger = logging.getLogger(__name__)


def record_decline(
    event_id: int,
    *,
    reason_code: Optional[str],
    actor: str,
    db: Session,
    free_text: Optional[str] = None,
) -> None:
    """Record a decline against a 'routed' event and re-run the waterfall.

    Idempotent: if the event is already declined (i.e. a 'declined' event with
    the same source_event_id already exists), the call is a no-op.

    Args:
        event_id: The ``file_routing_events.id`` of the 'routed' event being declined.
        reason_code: Decline reason code from lending.decline_reason_codes.
                     Defaults to Q2's 'unspecified'.
        actor: Identifier of the user declining (e.g. 'josh', admin user id).
        db: SQLAlchemy session.
        free_text: Optional free text to accompany the reason.
    """
    # Load the routed event to get lead_id and lender info.
    event = db.execute(
        text(
            "SELECT id, lead_id, lender_id, lender_name, action"
            "  FROM lending.file_routing_events"
            " WHERE id = :eid"
            " FOR UPDATE"
        ),
        {"eid": event_id},
    ).mappings().one_or_none()

    if event is None:
        raise ValueError(f"file_routing_events.id={event_id} not found")
    if event["action"] != "routed":
        raise ValueError(
            f"Event {event_id} has action {event['action']!r}; only 'routed' events can be declined"
        )

    lead_id = event["lead_id"]

    # Check file_routing is in 'routed' state for this lender (idempotency guard).
    routing = db.execute(
        text(
            "SELECT status, current_lender_id"
            "  FROM lending.file_routing"
            " WHERE lead_id = :lid"
            " FOR UPDATE"
        ),
        {"lid": lead_id},
    ).mappings().one_or_none()

    if routing is None:
        raise ValueError(f"file_routing row not found for lead_id={lead_id}")

    if routing["status"] in ("held", "exhausted"):
        logger.warning(
            "record_decline event_id=%d lead_id=%d: file already %s — no-op",
            event_id, lead_id, routing["status"],
        )
        return

    # Stale or repeated request: any later event on this file (a prior decline of this
    # same event, or the re-route it triggered) means this routed event is no longer current.
    superseded = db.execute(
        text(
            "SELECT 1 FROM lending.file_routing_events"
            " WHERE lead_id = :lid AND id > :eid LIMIT 1"
        ),
        {"lid": lead_id, "eid": event_id},
    ).scalar()
    if superseded or routing["status"] != "routed" or routing["current_lender_id"] != event["lender_id"]:
        logger.warning(
            "record_decline event_id=%d lead_id=%d: event no longer current — no-op",
            event_id, lead_id,
        )
        return

    code = reason_code or DEFAULT_DECLINE_REASON

    # Validate reason code exists (warn-only so an unknown code is stored raw, never dropped)
    valid = db.execute(
        text("SELECT 1 FROM lending.decline_reason_codes WHERE code = :code AND active"),
        {"code": code},
    ).scalar()
    if not valid:
        logger.warning(
            "record_decline: reason_code %r not in decline_reason_codes — storing raw", code
        )

    # Append the declined event.
    decline_event_id = db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            "  (lead_id, lender_id, lender_name, action, reason_code, free_text, actor)"
            " VALUES (:lead_id, :lid, :name, 'declined', :code, :text, :actor)"
            " RETURNING id"
        ),
        {
            "lead_id": lead_id,
            "lid": event["lender_id"],
            "name": event["lender_name"],
            "code": code,
            "text": free_text,
            "actor": actor,
        },
    ).scalar_one()
    # Reopen the file so route_file's idempotency guard lets the next pass run;
    # lenders_tried is kept so the declined lender is skipped.
    db.execute(
        text(
            "UPDATE lending.file_routing"
            "   SET status = 'unrouted',"
            "       current_lender_id = NULL,"
            "       current_lender_name = NULL,"
            "       updated_at = NOW()"
            " WHERE lead_id = :lead_id"
        ),
        {"lead_id": lead_id},
    )
    db.commit()

    # Notify Slack.
    try:
        post_decline_event(lead_id, event["lender_name"], code, decline_event_id, db=db)
    except Exception:
        logger.exception("record_decline: Slack post failed — continuing")

    # Re-run the waterfall. The router will see the current lender in lenders_tried
    # and skip to the next one.
    from src.lending.routing.router import route_file  # local import avoids circular

    route_file(lead_id, db)
