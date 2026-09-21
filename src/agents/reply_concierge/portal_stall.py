"""
Portal-stall webhook handler — WP-T2-4.

Backflip calls POST /api/webhooks/portal-stall when a borrower has started
the pre-qual flow and not completed it within the stall threshold (default 15
minutes). This handler:

  1. Looks up the person in FA Max state by backflip_contact_id or email.
  2. Checks suppression — if suppressed, logs and stops.
  3. If a concierge inbound exists for this person (they replied first), hands
     it to the concierge router for classification + response.
  4. If no inbound exists, publishes a portal.stall event so the Abandonment
     Agent (WP-T2-5) owns touch #1 of the five-touch sequence.

FastAPI route is registered in src/api/main.py.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

STALL_THRESHOLD_MINUTES = 15


class PortalStallPayload(BaseModel):
    backflip_contact_id: Optional[str] = None
    contact_email: Optional[str] = None
    borrower_first_name: Optional[str] = None
    portal_started_at: Optional[str] = None  # ISO-8601


def handle_portal_stall(payload: PortalStallPayload, db: Session) -> dict:
    """
    Called by the FastAPI route. Returns a status dict.
    """
    if not payload.backflip_contact_id and not payload.contact_email:
        raise HTTPException(status_code=422, detail="backflip_contact_id or contact_email required")

    person = _resolve_person(
        backflip_contact_id=payload.backflip_contact_id,
        contact_email=payload.contact_email,
        db=db,
    )

    if person and _is_suppressed(person["person_id"], db):
        logger.info(
            "portal_stall: person_id=%s is suppressed — no action",
            person["person_id"],
        )
        return {"status": "suppressed", "person_id": str(person["person_id"])}

    person_id = str(person["person_id"]) if person else None
    opportunity_id = _resolve_opportunity(person_id, db) if person_id else None

    # Check whether this person has a recent unprocessed inbound in the concierge log.
    # If so, classify and respond to it rather than triggering abandonment.
    pending_inbound = _pop_pending_inbound(person_id, db)
    if pending_inbound:
        logger.info(
            "portal_stall: person_id=%s has pending inbound — routing to concierge",
            person_id,
        )
        from src.agents.reply_concierge.router import handle_inbound
        outcome = handle_inbound(
            inbound_text=pending_inbound["inbound_text"],
            channel=pending_inbound["channel"],
            person_id=person_id,
            contact_email=payload.contact_email,
            opportunity_id=opportunity_id,
            borrower_first_name=payload.borrower_first_name,
            db=db,
        )
        return {
            "status": "concierge_handled",
            "action": outcome.action,
            "person_id": person_id,
        }

    # No pending inbound — publish portal.stall event for Abandonment Agent
    _publish_stall_event(
        person_id=person_id,
        opportunity_id=opportunity_id,
        contact_email=payload.contact_email,
        borrower_first_name=payload.borrower_first_name,
        portal_started_at=payload.portal_started_at,
        db=db,
    )
    logger.info(
        "portal_stall: person_id=%s published portal.stall event for abandonment",
        person_id,
    )
    return {"status": "stall_event_published", "person_id": person_id}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_person(
    backflip_contact_id: Optional[str],
    contact_email: Optional[str],
    db: Session,
) -> Optional[dict]:
    # source='backflip' rows store the Backflip contact ID in source_reference
    if backflip_contact_id:
        row = db.execute(
            text("""
                SELECT person_id, lifecycle_state
                FROM fa_max_persons
                WHERE source = 'backflip' AND source_reference = :ref
                LIMIT 1
            """),
            {"ref": backflip_contact_id},
        ).fetchone()
        if row:
            return dict(row._mapping)

    # Fall back to source_reference match (when stored as email at onboard time)
    if contact_email:
        row = db.execute(
            text("""
                SELECT person_id, lifecycle_state
                FROM fa_max_persons
                WHERE source_reference ILIKE :email
                LIMIT 1
            """),
            {"email": contact_email.strip().lower()},
        ).fetchone()
        if row:
            return dict(row._mapping)

    logger.warning(
        "portal_stall: could not resolve person backflip_id=%s email=%s",
        backflip_contact_id, contact_email,
    )
    return None


def _is_suppressed(person_id: str, db: Session) -> bool:
    row = db.execute(
        text("SELECT lifecycle_state FROM fa_max_persons WHERE person_id = :pid"),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return False
    return row[0] in ("suppressed", "do_not_contact", "dead")


def _resolve_opportunity(person_id: str, db: Session) -> Optional[str]:
    row = db.execute(
        text("""
            SELECT opportunity_id FROM fa_max_opportunities
            WHERE person_id = :pid
              AND current_stage NOT IN ('funded', 'declined', 'dead')
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"pid": person_id},
    ).fetchone()
    return str(row[0]) if row else None


def _pop_pending_inbound(person_id: Optional[str], db: Session) -> Optional[dict]:
    """Return and mark the most recent unprocessed inbound for this person, if any."""
    if not person_id:
        return None
    row = db.execute(
        text("""
            SELECT id, inbound_snippet, inbound_channel
            FROM fa_max_concierge_log
            WHERE person_id   = :pid
              AND action_taken = 'no_action'
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return None
    db.execute(
        text("UPDATE fa_max_concierge_log SET action_taken = 'escalated_exceptions' WHERE id = :id"),
        {"id": row[0]},
    )
    db.commit()
    return {"inbound_text": row[1] or "", "channel": row[2]}


def _publish_stall_event(
    person_id: Optional[str],
    opportunity_id: Optional[str],
    contact_email: Optional[str],
    borrower_first_name: Optional[str],
    portal_started_at: Optional[str],
    db: Session,
) -> None:
    try:
        from src.agents.events.ingestion import publish_lifecycle_event
        publish_lifecycle_event({
            "event_type": "portal.stall",
            "person_id": person_id,
            "opportunity_id": opportunity_id,
            "contact_email": contact_email,
            "borrower_first_name": borrower_first_name,
            "portal_started_at": portal_started_at,
            "stall_threshold_minutes": STALL_THRESHOLD_MINUTES,
        })
    except Exception as exc:
        # Fallback: write directly to fa_max_state_transition_events so the
        # event is not lost even if Redis is unavailable.
        logger.warning("portal_stall: publish_lifecycle_event failed (%s) — writing DB fallback", exc)
        try:
            db.execute(
                text("""
                    INSERT INTO fa_max_state_transition_events
                        (entity_uuid, event_type, payload, created_at)
                    VALUES
                        (:eid, 'portal.stall', CAST(:payload AS JSONB), NOW())
                """),
                {
                    "eid": person_id,
                    "payload": __import__("json").dumps({
                        "opportunity_id": opportunity_id,
                        "contact_email": contact_email,
                        "portal_started_at": portal_started_at,
                    }),
                },
            )
            db.commit()
        except Exception as db_exc:
            db.rollback()
            logger.error("portal_stall: DB fallback also failed: %s", db_exc)
