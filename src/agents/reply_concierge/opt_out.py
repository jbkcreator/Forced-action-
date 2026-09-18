"""
Opt-out handler for WP-T2-4.

Suppresses a person immediately and irreversibly across all channels.
Sets lifecycle stage to 'do_not_contact' in the FA Max person table.
Also writes to the existing email_suppression store used by Cora so the
suppression is enforced by every outbound path, not just the concierge.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)


@dataclass
class OptOutResult:
    suppressed: bool
    person_id: Optional[str]
    reason: str


def handle_opt_out(
    person_id: Optional[str],
    contact_email: Optional[str],
    inbound_text: str,
    channel: str,
    db: Session,
) -> OptOutResult:
    """
    Suppress a contact immediately. At least one of person_id or contact_email
    must be provided. Both paths are attempted when both are available.

    This is the only write this module performs — it does not send anything.
    """
    if not person_id and not contact_email:
        logger.error("opt_out: called with neither person_id nor contact_email — no action taken")
        return OptOutResult(suppressed=False, person_id=None, reason="missing_identifier")

    now = datetime.now(timezone.utc)
    snippet = inbound_text[:500] if inbound_text else ""

    suppressed = False

    # 1. Transition FA Max person lifecycle state → do_not_contact via state engine
    # (direct UPDATE is forbidden by fa_max_guard_state_write trigger)
    if person_id:
        try:
            from src.services.state_engine import get_person_state, transition, TransitionOutcome
            import uuid as _uuid

            current = get_person_state(session=db, person_id=person_id)
            current_state = current.get("lifecycle_state") if current else None

            if current_state in ("do_not_contact", "suppressed", None):
                # Already suppressed or person not found — treat as success
                suppressed = True
                logger.info(
                    "opt_out: person_id=%s already in state=%s — no transition needed",
                    person_id, current_state,
                )
            else:
                idem = f"opt_out:{person_id}:{snippet[:40]}"
                result = transition(
                    entity_type="person",
                    entity_uuid=person_id,
                    from_state=current_state,
                    to_state="do_not_contact",
                    actor="reply_concierge",
                    source_component="src.agents.reply_concierge.opt_out",
                    idempotency_key=idem,
                    state_version=current.get("state_version"),
                    person_id=person_id,
                    session=db,
                )
                if result.outcome in (TransitionOutcome.succeeded, TransitionOutcome.idempotent_skip):
                    suppressed = True
                    logger.info(
                        "opt_out: person_id=%s transitioned to do_not_contact via %s",
                        person_id, channel,
                    )
                else:
                    logger.error(
                        "opt_out: state transition refused for person_id=%s outcome=%s",
                        person_id, result.outcome,
                    )
        except Exception as exc:
            db.rollback()
            logger.error("opt_out: failed to transition fa_max_persons for person_id=%s: %s", person_id, exc)

    # 2. Suppress in the email suppression store (covers Cora's outbound path)
    if contact_email:
        try:
            from src.services.email_suppression import suppress_contact
            suppress_contact(db, email=contact_email, source="concierge")
            suppressed = True
            logger.info("opt_out: email=%s suppressed in email_suppression store", contact_email)
        except Exception as exc:
            logger.error("opt_out: email_suppression.suppress_contact failed for %s: %s", contact_email, exc)

    # 3. Log the opt-out event
    try:
        db.execute(
            sa_text("""
                INSERT INTO fa_max_concierge_log
                    (person_id, inbound_channel, inbound_snippet, classification,
                     kb_topic_key, confidence, action_taken, created_at)
                VALUES
                    (:pid, :channel, :snippet, 'opt_out', NULL, 1.0, 'opt_out_suppressed', :now)
            """),
            {
                "pid": person_id,
                "channel": channel,
                "snippet": snippet,
                "now": now,
            },
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("opt_out: failed to write concierge_log: %s", exc)

    return OptOutResult(
        suppressed=suppressed,
        person_id=person_id,
        reason="opt_out_processed" if suppressed else "suppression_failed",
    )
