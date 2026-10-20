"""Decline handler: record a lender decline and re-run the waterfall.

``record_decline`` is called by the decline endpoint.  It:
1. Validates the event is a 'routed' event belonging to the file's current lender.
2. Appends a 'declined' event.
3. Tells the partner (a partner file's status moves to 'declined').
4. Calls the router to run the next pass of the waterfall, which routes the file on or holds it.

The file is a LendingFlow lead or a partner file; the event row says which.

A decline always carries a reason code picked from Josh's list (his answer to Q2: no default, because a
decline for one lender may still work for another). The catch-all reason 'other' also needs a free-text
explanation.
  POST /api/lending/routing/files/{file_id}/decline
  Body: {"reason_code": "ltv_over_max", "actor": "josh", "free_text": ""}
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import CATCH_ALL_DECLINE_REASON
from src.lending.routing import partner_link
from src.lending.routing.file_key import LEAD, key_from_row
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

    Idempotent: a repeated or stale decline of the same event is a no-op.

    Args:
        event_id: The ``file_routing_events.id`` of the 'routed' event being declined.
        reason_code: Decline reason code from lending.decline_reason_codes (a fixed enum). Required: a
                     missing or unknown code raises ValueError.

        actor: Identifier of the user declining (e.g. 'josh', admin user id).
        db: SQLAlchemy session.
        free_text: Free text to accompany the reason. Required when the reason is the catch-all 'other'.
    """
    code = (reason_code or "").strip()
    if not code:
        raise ValueError("a decline reason is required")
    if code == CATCH_ALL_DECLINE_REASON and not (free_text or "").strip():
        raise ValueError("say why in the free text when the reason is 'other'")

    from src.lending.routing.router import load_file, route_file, route_loan_file

    event = db.execute(
        text(
            "SELECT id, lead_id, loan_file_id, lender_id, lender_name, action"
            "  FROM lending.file_routing_events"
            " WHERE id = :eid"
            " FOR UPDATE"
        ),
        {"eid": event_id},
    ).mappings().one_or_none()

    if event is None:
        raise ValueError("that routing event was not found")
    if event["action"] != "routed":
        raise ValueError(f"only a 'routed' event can be declined (this one is {event['action']!r})")

    key = key_from_row(event)

    # Check file_routing is in 'routed' state for this lender (idempotency guard).
    routing = db.execute(
        text(
            "SELECT status, current_lender_id, route_position"
            "  FROM lending.file_routing"
            f" WHERE {key.column} = :id"
            " FOR UPDATE"
        ),
        {"id": key.id},
    ).mappings().one_or_none()

    if routing is None:
        raise ValueError("no routing record for this file")

    if routing["status"] in ("held", "exhausted"):
        logger.warning(
            "record_decline event_id=%d %s=%d: file already %s - no-op",
            event_id, key.column, key.id, routing["status"],
        )
        return

    # Stale or repeated request: a later event on this file (a prior decline of this same event, the re-route
    # it triggered, or a funding) means this routed event is no longer current. An approval does not: a lender
    # that approved can still decline before funding.
    superseded = db.execute(
        text(
            "SELECT 1 FROM lending.file_routing_events"
            f" WHERE {key.column} = :id AND id > :eid AND action <> 'approved' LIMIT 1"
        ),
        {"id": key.id, "eid": event_id},
    ).scalar()
    if superseded or routing["status"] != "routed" or routing["current_lender_id"] != event["lender_id"]:
        logger.warning(
            "record_decline event_id=%d %s=%d: event no longer current - no-op",
            event_id, key.column, key.id,
        )
        return

    # The reason comes from a fixed enum Josh supplies (lending.decline_reason_codes).
    valid = db.execute(
        text("SELECT 1 FROM lending.decline_reason_codes WHERE code = :code AND active"),
        {"code": code},
    ).scalar()
    if not valid:
        raise ValueError(f"reason_code {code!r} is not an active decline reason")

    # Append the declined event.
    decline_event_id = db.execute(
        text(
            "INSERT INTO lending.file_routing_events"
            f"  ({key.column}, lender_id, lender_name, action, reason_code, free_text, actor)"
            " VALUES (:id, :lid, :name, 'declined', :code, :text, :actor)"
            " RETURNING id"
        ),
        {
            "id": key.id,
            "lid": event["lender_id"],
            "name": event["lender_name"],
            "code": code,
            "text": free_text,
            "actor": actor,
        },
    ).scalar_one()
    # Reopen the file so the router's idempotency guard lets the next pass run;
    # lenders_tried is kept so the declined lender is skipped.
    db.execute(
        text(
            "UPDATE lending.file_routing"
            "   SET status = 'unrouted',"
            "       current_lender_id = NULL,"
            "       current_lender_name = NULL,"
            "       updated_at = NOW()"
            f" WHERE {key.column} = :id"
        ),
        {"id": key.id},
    )
    db.commit()

    # Notify Slack (file, last name, product, amount, lender, position).
    try:
        post_decline_event(
            load_file(key, db), event["lender_name"], routing["route_position"], code, decline_event_id, db=db,
        )
    except Exception:
        logger.exception("record_decline %s=%d: Slack post failed - continuing", key.column, key.id)

    # A partner file's status moves to 'declined' and the partner is told, before the next pass runs.
    partner_link.mirror_status(key, "declined", code)

    # Re-run the waterfall. The router sees the declined lender in lenders_tried and skips to the next one.
    if key.kind == LEAD:
        route_file(key.id, db)
    else:
        route_loan_file(key.id, db)
