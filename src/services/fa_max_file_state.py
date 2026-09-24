"""src/services/fa_max_file_state.py

WP-T2-6 Stage Monitoring -- the one read/write seam for fa_max_file_state
and fa_max_document_requests. Mirrors state_engine.py's own "one write
function per concern, never touched directly elsewhere" convention.

backflip_stage here is deliberately NOT the same enum as
fa_max_opportunity_stage_config.stage_key (the coarse borrower-journey FSM
that governs fa_max_opportunities.current_stage). It tracks Backflip's
internal, finer-grained stage detail that the coarse FSM has no room for.
The two are cascaded together at exactly two points -- 'declined' and the
moment real terms are recorded -- see update_backflip_stage() and
record_terms(). Everywhere else the two states move independently. See the
plan doc's Assumption 6 for the reasoning: Backflip's real stage
terminology is unconfirmed (Q13/Q14 open), so guessing a full mapping today
would encode a taxonomy nobody has verified.

Sends never go through src.services.relay.queue.enqueue() -- verified
against src/agents/contracts/cora_to_relay.py: it hard-requires a thread_id
matching Hunter's OPP-YYYY-##### format, which FA Max opportunities never
carry. Every send here is a raw INSERT INTO relay_approval_queue, the same
pattern src/agents/reply_concierge/router.py and abandonment_agent.py
already use for exactly this reason (see plan doc Assumption 7). Because
that path skips enqueue()'s automatic checks, every borrower-facing send
calls fa_max_send_governance.validate_safe_payload()/require_consent()/
suppression_reason() first. The payload check is not merely policy:
relay_approval_queue carries a CHECK constraint rejecting any
fa_max_lending payload whose text matches a prohibited-financial-term
regex, which common document names ("Bank Statement", "Tax Return") all
match -- hence the generic, document-name-free borrower copy in
stage_monitor.py.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.fa_max_stage_monitoring import BACKFLIP_STAGE_KEYS
from src.services import state_engine

logger = logging.getLogger(__name__)

_VENTURE_KEY = "fa_max_lending"
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def get_file_state(session: Session, *, opportunity_id: str) -> Optional[Dict[str, Any]]:
    row = session.execute(
        text("""
            SELECT opportunity_id::text, person_id::text, backflip_stage, contact_email,
                   last_stage_change_at, last_borrower_touch_at,
                   expected_next_stage, stall_flagged_at
            FROM fa_max_file_state
            WHERE opportunity_id = :opportunity_id ::uuid
        """),
        {"opportunity_id": opportunity_id},
    ).fetchone()
    if row is None:
        return None
    return dict(row._mapping)


def _best_effort_contact_email(session: Session, *, person_id: str) -> Optional[str]:
    """fa_max_persons has no dedicated email column. source_reference is
    dual-purpose -- a Backflip contact ID when source='backflip', an email
    only "when stored as email at onboard time" (see
    src/agents/reply_concierge/portal_stall.py's own comment on this exact
    field). Only trust it here when it actually looks like an email;
    otherwise leave contact_email NULL rather than guess (plan doc,
    Assumption 8).
    """
    row = session.execute(
        text("SELECT source_reference FROM fa_max_persons WHERE person_id = :pid ::uuid"),
        {"pid": person_id},
    ).fetchone()
    if row is None:
        return None
    candidate = dict(row._mapping).get("source_reference")
    if candidate and _EMAIL_RE.match(candidate.strip()):
        return candidate.strip().lower()
    return None


def ensure_file_state(
    session: Session, *, opportunity_id: str, person_id: str,
    contact_email: Optional[str] = None,
) -> Dict[str, Any]:
    """Create the file-state row on first observation of a submitted file.
    Idempotent -- a second call for the same opportunity is a no-op read.

    contact_email: pass explicitly when the caller has it (e.g. a future
    Slack command extension). When omitted, makes one best-effort attempt
    via _best_effort_contact_email() -- see Assumption 8. NULL is a valid,
    expected outcome; Tasks 10-11's sweeps skip (and log) any row without
    a usable email rather than guessing further.
    """
    existing = get_file_state(session, opportunity_id=opportunity_id)
    if existing is not None:
        return existing

    if contact_email is None:
        contact_email = _best_effort_contact_email(session, person_id=person_id)

    session.execute(
        text("""
            INSERT INTO fa_max_file_state (opportunity_id, person_id, backflip_stage, contact_email)
            VALUES (:opportunity_id ::uuid, :person_id ::uuid, 'submitted', :contact_email)
            ON CONFLICT (opportunity_id) DO NOTHING
        """),
        {"opportunity_id": opportunity_id, "person_id": person_id, "contact_email": contact_email},
    )
    session.commit()
    created = get_file_state(session, opportunity_id=opportunity_id)
    if created is None:
        raise RuntimeError(f"ensure_file_state: insert did not produce a row for {opportunity_id}")
    return created


def update_backflip_stage(
    session: Session, *, opportunity_id: str, to_stage: str, actor: str, source: str,
) -> Dict[str, Any]:
    """Update Backflip-side stage detail. Cascades to the coarse
    fa_max_opportunities FSM only for 'declined' (an unambiguous terminal
    state both taxonomies agree on).
    """
    if to_stage not in BACKFLIP_STAGE_KEYS:
        raise ValueError(f"unknown backflip stage: {to_stage!r}")

    row = session.execute(
        text("""
            UPDATE fa_max_file_state
            SET backflip_stage = :to_stage, last_stage_change_at = NOW(),
                stall_flagged_at = NULL, updated_at = NOW()
            WHERE opportunity_id = :opportunity_id ::uuid
            RETURNING person_id::text
        """),
        {"to_stage": to_stage, "opportunity_id": opportunity_id},
    ).fetchone()
    if row is None:
        raise ValueError(f"update_backflip_stage: no file_state row for {opportunity_id}")
    person_id = row[0]

    # channel reflects how this observation actually arrived -- 'slack' for
    # a manually-typed update, 'email' for a parsed Backflip notification.
    # fa_max_interactions' CHECK constraint already allows 'slack'; hardcoding
    # 'email' regardless of source would misrepresent a manual update in the
    # audit log.
    state_engine.write_interaction(
        session=session, person_id=person_id,
        channel="slack" if source == "manual" else "email",
        direction="inbound", actor=actor,
        body_redacted=f"stage observed: {to_stage} (source={source})",
    )

    if to_stage == "declined":
        current = state_engine.get_opportunity_state(session=session, opportunity_id=opportunity_id)
        if current is not None and current["current_stage"] not in ("declined", "dead"):
            # transition() requires a real fa_max_entity_registry
            # entity_uuid, not the opportunity's own native ID
            # (WP-T2-6 review fix).
            entity_uuid = state_engine.ensure_entity_registry(
                session=session, entity_type="opportunity", native_id=opportunity_id,
            )
            state_engine.transition(
                session=session, entity_type="opportunity", entity_uuid=entity_uuid,
                from_state=current["current_stage"], to_state="declined",
                actor=actor, source_component="src.services.fa_max_file_state",
                idempotency_key=f"fa_max_file_state:declined:{opportunity_id}",
                state_version=current["state_version"],
            )

    # One commit for the whole operation (file_state UPDATE + interaction
    # log + optional declined-cascade transition) -- this function must not
    # depend on the caller's session-lifecycle convention. Found live: the
    # previous mid-function commit here committed the file_state UPDATE but
    # left write_interaction()'s INSERT (and transition(), which explicitly
    # documents "caller owns commit/rollback") pending in an uncommitted
    # transaction. Every other call site uses get_db_context(), which
    # auto-commits on clean exit and masked this; the Backflip email poller
    # manages a bare Session with no trailing commit, so that second write
    # was silently rolled back on session close.
    session.commit()

    logger.info(
        "fa_max_file_state: opportunity_id=%s stage=%s source=%s actor=%s",
        opportunity_id, to_stage, source, actor,
    )
    # Built from what the UPDATE...RETURNING already gave us rather than a
    # second round trip through get_file_state (CLAUDE.md: minimise round
    # trips) -- callers needing the full row can call get_file_state directly.
    return {"opportunity_id": opportunity_id, "person_id": person_id, "backflip_stage": to_stage}


def record_document_request(
    session: Session, *, opportunity_id: str, person_id: str,
    document_name: str, source: str, idempotency_key: str,
) -> Dict[str, Any]:
    session.execute(
        text("""
            INSERT INTO fa_max_document_requests
                (opportunity_id, person_id, document_name, source, idempotency_key)
            VALUES (:opportunity_id ::uuid, :person_id ::uuid, :document_name, :source, :idem)
            ON CONFLICT (idempotency_key) DO NOTHING
        """),
        {
            "opportunity_id": opportunity_id, "person_id": person_id,
            "document_name": document_name, "source": source, "idem": idempotency_key,
        },
    )
    session.commit()
    logger.info(
        "fa_max_file_state: document requested opportunity_id=%s document=%s source=%s",
        opportunity_id, document_name, source,
    )
    return {"opportunity_id": opportunity_id, "document_name": document_name, "source": source}


def record_document_received(session: Session, *, opportunity_id: str, document_name: str) -> int:
    # document_name is free text typed twice by a human (once on request,
    # once on receipt) -- case-fold and trim both sides so a casing/
    # whitespace difference doesn't leave the request stuck open forever.
    result = session.execute(
        text("""
            UPDATE fa_max_document_requests
            SET received_at = NOW()
            WHERE opportunity_id = :opportunity_id ::uuid
              AND lower(trim(document_name)) = lower(trim(:document_name))
              AND received_at IS NULL
        """),
        {"opportunity_id": opportunity_id, "document_name": document_name},
    )
    session.commit()
    count = result.rowcount
    if count:
        logger.info(
            "fa_max_file_state: document received opportunity_id=%s document=%s",
            opportunity_id, document_name,
        )
    return count


def record_terms(
    session: Session, *, opportunity_id: str, actor: str,
    loan_amount_cents: Optional[int] = None,
    maturity_months: Optional[int] = None,
    backflip_ref: Optional[str] = None,
) -> None:
    """Write received terms to the opportunity record and notify the client
    via MONEY. Never composes a borrower-facing message -- internal-only
    fields (loan_amount_cents, maturity_months), per SOT.md.
    """
    session.execute(
        text("""
            UPDATE fa_max_opportunities
            SET loan_amount_cents = COALESCE(:loan_amount_cents, loan_amount_cents),
                maturity_months   = COALESCE(:maturity_months, maturity_months),
                backflip_ref      = COALESCE(:backflip_ref, backflip_ref),
                updated_at        = NOW()
            WHERE opportunity_id = :opportunity_id ::uuid
        """),
        {
            "loan_amount_cents": loan_amount_cents, "maturity_months": maturity_months,
            "backflip_ref": backflip_ref, "opportunity_id": opportunity_id,
        },
    )
    session.commit()

    current = state_engine.get_opportunity_state(session=session, opportunity_id=opportunity_id)
    if current is not None and current["current_stage"] == "submitted":
        # transition() requires a real fa_max_entity_registry entity_uuid,
        # not the opportunity's own native ID (WP-T2-6 review fix).
        entity_uuid = state_engine.ensure_entity_registry(
            session=session, entity_type="opportunity", native_id=opportunity_id,
        )
        state_engine.transition(
            session=session, entity_type="opportunity", entity_uuid=entity_uuid,
            from_state="submitted", to_state="term_sheet",
            actor=actor, source_component="src.services.fa_max_file_state",
            idempotency_key=f"fa_max_file_state:term_sheet:{opportunity_id}",
            state_version=current["state_version"],
        )

    # No thread_id exists for an FA Max opportunity in Hunter's OPP-YYYY-#####
    # format, so enqueue() (which hard-requires it via CoraRelayHandoff) is
    # not usable here -- raw insert, same as router.py/abandonment_agent.py.
    # 'noop' is a registered dispatcher (src/services/relay/channels.py);
    # this row is informational only, posted to MONEY, never dispatched to
    # a real channel.
    session.execute(
        text("""
            INSERT INTO relay_approval_queue
                (idempotency_key, venture_key, lane, channel, recipient,
                 payload, status, agent_name, autonomy_tier_at_send, person_id)
            VALUES
                (:idem, :vk, :lane, 'noop', 'n/a',
                 CAST(:payload AS JSONB), 'pending', :agent, 'A', :pid)
            ON CONFLICT (idempotency_key) DO NOTHING
        """),
        {
            "idem": f"fa_max_terms_notify:{opportunity_id}", "vk": _VENTURE_KEY, "lane": "MONEY",
            "payload": json.dumps({"type": "terms_received", "opportunity_id": opportunity_id}),
            "agent": "stage_monitor", "pid": current.get("person_id") if current else None,
        },
    )
    session.commit()
    logger.info("fa_max_file_state: terms recorded and MONEY notified opportunity_id=%s", opportunity_id)


def touch_borrower(session: Session, *, opportunity_id: str) -> None:
    session.execute(
        text("""
            UPDATE fa_max_file_state
            SET last_borrower_touch_at = NOW(), updated_at = NOW()
            WHERE opportunity_id = :opportunity_id ::uuid
        """),
        {"opportunity_id": opportunity_id},
    )
    session.commit()


def send_governed_email(
    session: Session, *, opportunity_id: str, person_id: str, contact_email: str,
    subject: str, body: str, lane: str, agent_name: str, idempotency_key: str,
) -> bool:
    """The one seam every borrower-facing send in Tasks 10-11 goes through.
    Runs the full canonical governance check (content safety + consent +
    suppression) before a raw insert into relay_approval_queue -- the same
    checks enqueue() would have run internally, applied explicitly here
    since enqueue() itself isn't usable (see module docstring).

    Returns True only when a row was actually written. False means either
    governance blocked it (logged, not raised -- a blocked send must never
    crash a sweep processing other files) or ON CONFLICT DO NOTHING deduped
    it. Callers use the return value to decide whether to stamp a
    "we sent this" timestamp, so a deduped no-op must not read as a send.

    The content check matters beyond policy: relay_approval_queue carries a
    CHECK constraint rejecting any fa_max_lending payload whose text matches
    a prohibited-financial-term regex, so an unchecked borrower-facing
    payload raises IntegrityError and the caller retries it forever.
    """
    from src.services import fa_max_send_governance as governance

    try:
        governance.validate_safe_payload({"subject": subject, "body": body})
    except governance.GovernanceBlocked as exc:
        logger.warning(
            "fa_max_file_state: send blocked opportunity_id=%s reason=%s",
            opportunity_id, exc.reason,
        )
        return False

    consent = governance.require_consent(session, person_id=person_id, channel="email")
    if not consent.allowed:
        logger.info(
            "fa_max_file_state: send blocked opportunity_id=%s reason=%s",
            opportunity_id, consent.reason,
        )
        return False

    blocked_reason = governance.suppression_reason(session, recipient=contact_email, channel="email")
    if blocked_reason:
        logger.info(
            "fa_max_file_state: send blocked opportunity_id=%s reason=%s",
            opportunity_id, blocked_reason,
        )
        return False

    inserted = session.execute(
        text("""
            INSERT INTO relay_approval_queue
                (idempotency_key, venture_key, lane, channel, recipient,
                 payload, status, agent_name, autonomy_tier_at_send, person_id)
            VALUES
                (:idem, :vk, :lane, 'email', :recipient,
                 CAST(:payload AS JSONB), 'pending', :agent, 'A', :pid)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
        """),
        {
            "idem": idempotency_key, "vk": _VENTURE_KEY, "lane": lane,
            "recipient": contact_email,
            "payload": json.dumps({"subject": subject, "body": body}),
            "agent": agent_name, "pid": person_id,
        },
    ).fetchone()
    session.commit()
    if inserted is None:
        logger.info(
            "fa_max_file_state: send deduped opportunity_id=%s lane=%s agent=%s",
            opportunity_id, lane, agent_name,
        )
        return False
    logger.info(
        "fa_max_file_state: sent opportunity_id=%s lane=%s agent=%s",
        opportunity_id, lane, agent_name,
    )
    return True
