"""src/agents/reply_concierge/stage_monitor.py

WP-T2-6 -- the three periodic sweeps: stall detection, proactive status
touch, document chase. Cron-called by scripts/run_stage_monitor_worker.py
(Task 12), mirroring WP-T2-5's abandonment_agent.py fire_due_touches()
shape: read due rows, act, log, never crash the whole sweep on one row's
failure.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.fa_max_stage_monitoring import (
    DOC_CHASE_ESCALATE_BUSINESS_DAYS,
    DOC_CHASE_FOLLOWUP_BUSINESS_DAYS,
    STALL_THRESHOLD_BUSINESS_DAYS,
    STATUS_TOUCH_INTERVAL_BUSINESS_DAYS,
    TERMINAL_BACKFLIP_STAGES,
)
from src.services.fa_max_business_days import business_days_since

logger = logging.getLogger(__name__)

_VENTURE_KEY = "fa_max_lending"
_AGENT_NAME = "stage_monitor"

_STALLED_CANDIDATES_SQL = text("""
    SELECT opportunity_id::text, person_id::text, backflip_stage, last_stage_change_at
    FROM fa_max_file_state
    -- 'funded'/'declined' mirrors config.fa_max_stage_monitoring.TERMINAL_BACKFLIP_STAGES;
    -- keep this literal in sync if that constant ever changes.
    WHERE backflip_stage NOT IN ('funded', 'declined')
      AND stall_flagged_at IS NULL
    ORDER BY last_stage_change_at
    LIMIT 200
""")

_MARK_STALLED_SQL = text("""
    UPDATE fa_max_file_state
    SET stall_flagged_at = NOW()
    WHERE opportunity_id = :opportunity_id ::uuid
""")


def sweep_stalled_files(session: Session) -> int:
    rows = session.execute(_STALLED_CANDIDATES_SQL).fetchall()
    flagged = 0
    for row in rows:
        data = dict(row._mapping)
        elapsed = business_days_since(data["last_stage_change_at"])
        if elapsed < STALL_THRESHOLD_BUSINESS_DAYS:
            continue
        try:
            _post_stall_exception(session, data, elapsed)
            session.execute(_MARK_STALLED_SQL, {"opportunity_id": data["opportunity_id"]})
            session.commit()
            flagged += 1
        except Exception:
            session.rollback()
            logger.exception(
                "stage_monitor: failed to flag stall for opportunity_id=%s",
                data["opportunity_id"],
            )
    if flagged:
        logger.info("stage_monitor: flagged %d stalled file(s)", flagged)
    return flagged


def _post_stall_exception(session: Session, data: dict[str, Any], elapsed_days: float) -> None:
    # Raw insert, not enqueue() -- FA Max opportunities carry no Hunter-style
    # OPP-YYYY-##### thread_id, which enqueue()'s CoraRelayHandoff hard-
    # requires. Same pattern as router.py's _route_to_exceptions. 'noop' is
    # a registered dispatcher; this row is informational only.
    import json

    session.execute(
        text("""
            INSERT INTO relay_approval_queue
                (idempotency_key, venture_key, lane, channel, recipient,
                 payload, status, agent_name, autonomy_tier_at_send, person_id)
            VALUES
                (:idem, :vk, :lane, :channel, :recipient,
                 CAST(:payload AS JSONB), :status, :agent, :autonomy, :pid)
            ON CONFLICT (idempotency_key) DO NOTHING
        """),
        {
            "idem": f"fa_max_stall:{data['opportunity_id']}:{int(elapsed_days)}",
            "vk": _VENTURE_KEY,
            "lane": "EXCEPTIONS",
            "channel": "noop",
            "recipient": "n/a",
            "payload": json.dumps({
                "type": "file_stalled",
                "opportunity_id": data["opportunity_id"],
                "backflip_stage": data["backflip_stage"],
                "business_days_stalled": round(elapsed_days, 1),
            }),
            "status": "pending",
            "agent": _AGENT_NAME,
            "autonomy": "A",
            "pid": data["person_id"],
        },
    )


_STATUS_TOUCH_CANDIDATES_SQL = text("""
    SELECT fs.opportunity_id::text, fs.person_id::text, fs.backflip_stage,
           fs.last_borrower_touch_at, fs.contact_email
    FROM fa_max_file_state fs
    -- 'funded'/'declined' mirrors config.fa_max_stage_monitoring.TERMINAL_BACKFLIP_STAGES;
    -- keep this literal in sync if that constant ever changes.
    WHERE fs.backflip_stage NOT IN ('funded', 'declined')
    ORDER BY fs.last_borrower_touch_at NULLS FIRST
    LIMIT 200
""")

_STATUS_TOUCH_BODY = (
    "Quick update on your file — it's currently {stage_label}. "
    "We'll reach out as soon as there's something new. In the meantime, "
    "reply here anytime with questions."
)

_STAGE_LABELS = {
    "submitted": "with Backflip for initial review",
    "under_review": "under review",
    "conditional_approval": "conditionally approved",
    "docs_requested": "waiting on a couple of documents",
    "cleared_to_close": "cleared to close",
}


def sweep_status_touches(session: Session) -> int:
    from src.services import fa_max_file_state

    rows = session.execute(_STATUS_TOUCH_CANDIDATES_SQL).fetchall()
    sent = 0
    for row in rows:
        data = dict(row._mapping)
        if data["last_borrower_touch_at"] is not None:
            elapsed = business_days_since(data["last_borrower_touch_at"])
            if elapsed < STATUS_TOUCH_INTERVAL_BUSINESS_DAYS:
                continue
        if not data.get("contact_email"):
            logger.warning(
                "stage_monitor: opportunity_id=%s has no resolvable contact email "
                "(fa_max_file_state.contact_email is NULL) — skipping status touch",
                data["opportunity_id"],
            )
            continue
        try:
            body = _STATUS_TOUCH_BODY.format(
                stage_label=_STAGE_LABELS.get(data["backflip_stage"], "moving forward")
            )
            was_sent = fa_max_file_state.send_governed_email(
                session, opportunity_id=data["opportunity_id"], person_id=data["person_id"],
                contact_email=data["contact_email"], subject="Update on your application",
                body=body, lane="RELATIONSHIPS", agent_name=_AGENT_NAME,
                idempotency_key=f"fa_max_status_touch:{data['opportunity_id']}:{_today_key()}",
            )
            if was_sent:
                fa_max_file_state.touch_borrower(session, opportunity_id=data["opportunity_id"])
                sent += 1
        except Exception:
            logger.exception(
                "stage_monitor: failed to send status touch for opportunity_id=%s",
                data["opportunity_id"],
            )
    if sent:
        logger.info("stage_monitor: sent %d status touch(es)", sent)
    return sent


def _today_key() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).date().isoformat()


_OUTSTANDING_DOC_REQUESTS_SQL = text("""
    SELECT dr.id, dr.opportunity_id::text, dr.person_id::text, dr.document_name,
           dr.first_chase_sent_at, dr.followup_chase_sent_at, dr.escalated_at,
           fs.contact_email
    FROM fa_max_document_requests dr
    JOIN fa_max_file_state fs ON fs.opportunity_id = dr.opportunity_id
    WHERE dr.received_at IS NULL AND dr.escalated_at IS NULL
      -- 'funded'/'declined' mirrors config.fa_max_stage_monitoring.TERMINAL_BACKFLIP_STAGES;
      -- keep this literal in sync if that constant ever changes.
      AND fs.backflip_stage NOT IN ('funded', 'declined')
    ORDER BY dr.requested_at
    LIMIT 200
""")

# Borrower-facing document-chase copy never names the specific document.
# relay_approval_queue's CHECK constraint rejects any fa_max_lending payload
# matching a prohibited-financial-term regex, and the document names that
# actually occur ("Bank Statement", "Tax Return", "Proof of income",
# "Commitment letter") all match it. The document name lives in
# fa_max_document_requests / fa_max_interactions for internal reference; the
# borrower gets a prompt to reply, which is the action we want anyway.
_DOC_CHASE_FIRST_SUBJECT = "Action needed: a document is still outstanding"
_DOC_CHASE_FIRST_BODY = (
    "We're waiting on one more document to keep your file moving. "
    "Reply here and we'll confirm exactly what's needed and how to send it."
)
_DOC_CHASE_FOLLOWUP_SUBJECT = "Still waiting on a document"
_DOC_CHASE_FOLLOWUP_BODY = (
    "Following up — we're still waiting on a document to keep your file "
    "moving. Reply here and we'll confirm exactly what's needed and how to "
    "send it."
)

_MARK_FOLLOWUP_SENT_SQL = text("""
    UPDATE fa_max_document_requests SET followup_chase_sent_at = NOW() WHERE id = :id
""")

_MARK_ESCALATED_SQL = text("""
    UPDATE fa_max_document_requests SET escalated_at = NOW() WHERE id = :id
""")


def sweep_document_chases(session: Session) -> int:
    rows = session.execute(_OUTSTANDING_DOC_REQUESTS_SQL).fetchall()
    actioned = 0
    for row in rows:
        data = dict(row._mapping)
        try:
            if data["followup_chase_sent_at"] is None:
                if data["first_chase_sent_at"] is None:
                    continue  # first touch fires at request time, not in this sweep
                elapsed = business_days_since(data["first_chase_sent_at"])
                if elapsed < DOC_CHASE_FOLLOWUP_BUSINESS_DAYS:
                    continue
                if not data.get("contact_email"):
                    logger.warning(
                        "stage_monitor: document request id=%s has no resolvable "
                        "contact email — skipping follow-up chase",
                        data["id"],
                    )
                    continue
                if _send_chase_followup(session, data):
                    session.execute(_MARK_FOLLOWUP_SENT_SQL, {"id": data["id"]})
                    session.commit()
                    actioned += 1
            else:
                elapsed = business_days_since(data["followup_chase_sent_at"])
                if elapsed < DOC_CHASE_ESCALATE_BUSINESS_DAYS:
                    continue
                _escalate_chase(session, data)
                session.execute(_MARK_ESCALATED_SQL, {"id": data["id"]})
                session.commit()
                actioned += 1
        except Exception:
            session.rollback()
            logger.exception(
                "stage_monitor: document chase action failed for request id=%s",
                data["id"],
            )
    if actioned:
        logger.info("stage_monitor: actioned %d document chase(s)", actioned)
    return actioned


def _send_chase_followup(session: Session, data: dict[str, Any]) -> bool:
    from src.services import fa_max_file_state

    return fa_max_file_state.send_governed_email(
        session, opportunity_id=data["opportunity_id"], person_id=data["person_id"],
        contact_email=data["contact_email"],
        subject=_DOC_CHASE_FOLLOWUP_SUBJECT, body=_DOC_CHASE_FOLLOWUP_BODY,
        lane="RELATIONSHIPS", agent_name=_AGENT_NAME,
        idempotency_key=f"fa_max_doc_chase_followup:{data['id']}",
    )


def _escalate_chase(session: Session, data: dict[str, Any]) -> None:
    import json

    from src.services import fa_max_send_governance as governance

    # document_name is redacted even here: relay_approval_queue's CHECK
    # constraint rejects any fa_max_lending payload matching the prohibited-
    # financial-term regex regardless of lane, and common document names
    # ("Bank Statement", "Tax Return", "Proof of income") all match it. The
    # real name stays in fa_max_document_requests -- document_request_id is
    # what the client looks it up by.
    payload = {
        "type": "document_chase_escalation",
        "opportunity_id": data["opportunity_id"],
        "document_request_id": data["id"],
        "document_name_redacted": True,
    }
    try:
        governance.validate_safe_payload(payload)
    except governance.GovernanceBlocked as exc:
        logger.warning(
            "stage_monitor: escalation blocked for request id=%s reason=%s",
            data["id"], exc.reason,
        )
        return

    # Raw insert, not enqueue() -- same reason as _post_stall_exception:
    # no Hunter-style thread_id exists for an FA Max opportunity.
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
            "idem": f"fa_max_doc_chase_escalate:{data['id']}", "vk": _VENTURE_KEY,
            "lane": "EXCEPTIONS",
            "payload": json.dumps(payload),
            "agent": _AGENT_NAME, "pid": data["person_id"],
        },
    )


_MARK_FIRST_CHASE_SENT_SQL = text("""
    UPDATE fa_max_document_requests
    SET first_chase_sent_at = NOW()
    WHERE idempotency_key = :idempotency_key
""")


def send_first_chase_touch(
    session: Session, *, opportunity_id: str, person_id: str,
    document_name: str, contact_email: Optional[str],
) -> bool:
    """The first document-chase touch, sent immediately on request detection
    -- distinct from sweep_document_chases()'s later follow-up/escalation.
    Same governed-send shape as _send_chase_followup: skips (logs, never
    raises) on a missing contact_email so it never blocks the caller's
    document-request write, and marks first_chase_sent_at on success.
    """
    from src.services import fa_max_file_state

    if not contact_email:
        logger.warning(
            "stage_monitor: opportunity_id=%s document=%s has no resolvable "
            "contact email — skipping first chase touch",
            opportunity_id, document_name,
        )
        return False

    idempotency_key = f"docreq:{opportunity_id}:{document_name}"
    was_sent = fa_max_file_state.send_governed_email(
        session, opportunity_id=opportunity_id, person_id=person_id,
        contact_email=contact_email,
        subject=_DOC_CHASE_FIRST_SUBJECT, body=_DOC_CHASE_FIRST_BODY,
        lane="RELATIONSHIPS", agent_name=_AGENT_NAME,
        idempotency_key=f"fa_max_doc_chase_first:{idempotency_key}",
    )
    if was_sent:
        session.execute(_MARK_FIRST_CHASE_SENT_SQL, {"idempotency_key": idempotency_key})
        session.commit()
    return was_sent
