"""src/agents/reply_concierge/backflip_stage_ingest.py

WP-T2-6 -- resolves a Backflip reference to an FA Max opportunity and
applies a ParsedBackflipEvent through fa_max_file_state.py. The single
place both observation adapters (Slack manual-update command, Backflip
email parser) converge, so neither reimplements resolution or governance.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.reply_concierge import stage_monitor
from src.agents.reply_concierge.backflip_email_parser import ParsedBackflipEvent
from src.services import fa_max_file_state

logger = logging.getLogger(__name__)


def resolve_opportunity_by_backflip_ref(session: Session, backflip_ref: str) -> Optional[Dict[str, Any]]:
    row = session.execute(
        text("""
            SELECT opportunity_id::text, person_id::text
            FROM fa_max_opportunities
            WHERE backflip_ref = :ref
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"ref": backflip_ref},
    ).fetchone()
    if row is None:
        return None
    return dict(row._mapping)


def apply_parsed_event(
    session: Session, event: ParsedBackflipEvent, *, source: str, actor: str,
) -> bool:
    """Returns True if the event was matched to a known opportunity and
    applied, False if the backflip_ref didn't resolve (logged, not raised
    -- an unresolved ref should never crash the poller or the sweep)."""
    resolved = resolve_opportunity_by_backflip_ref(session, event.backflip_ref)
    if resolved is None:
        logger.warning(
            "backflip_stage_ingest: unresolved backflip_ref=%s event_type=%s — dropped",
            event.backflip_ref, event.event_type,
        )
        return False

    opportunity_id = resolved["opportunity_id"]
    person_id = resolved["person_id"]
    # No contact_email in hand here -- Backflip's own notification email
    # doesn't reliably carry the borrower's address. ensure_file_state()
    # makes its own best-effort attempt via fa_max_persons.source_reference
    # (Assumption 8); a NULL result is expected and handled by Tasks 10-11.
    fa_max_file_state.ensure_file_state(session, opportunity_id=opportunity_id, person_id=person_id)

    if event.event_type == "stage_change":
        fa_max_file_state.update_backflip_stage(
            session, opportunity_id=opportunity_id, to_stage=event.stage,
            actor=actor, source=source,
        )
    elif event.event_type == "document_request":
        idempotency_key = f"docreq:{opportunity_id}:{event.document_name}"
        fa_max_file_state.record_document_request(
            session, opportunity_id=opportunity_id, person_id=person_id,
            document_name=event.document_name, source=source,
            idempotency_key=idempotency_key,
        )
        file_state = fa_max_file_state.get_file_state(session, opportunity_id=opportunity_id)
        stage_monitor.send_first_chase_touch(
            session, opportunity_id=opportunity_id, person_id=person_id,
            document_name=event.document_name,
            contact_email=(file_state or {}).get("contact_email"),
        )
    elif event.event_type == "terms":
        fa_max_file_state.record_terms(
            session, opportunity_id=opportunity_id, actor=actor,
            loan_amount_cents=event.loan_amount_cents,
            maturity_months=event.maturity_months,
            backflip_ref=event.backflip_ref,
        )
    else:
        logger.error("backflip_stage_ingest: unknown event_type=%s", event.event_type)
        return False

    return True
