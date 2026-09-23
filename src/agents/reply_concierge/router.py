"""
Concierge router — WP-T2-4.

Orchestrates: classify → opt_out | build_reply | route_to_exceptions.

Entry point for all inbound messages routed to the FA Max concierge.
Writes audit rows to fa_max_concierge_log regardless of outcome.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from src.agents.reply_concierge.classifier import classify_inbound, CONFIDENCE_THRESHOLD
from src.agents.reply_concierge.opt_out import handle_opt_out
from src.agents.reply_concierge.responder import build_reply

logger = logging.getLogger(__name__)

_AGENT_NAME = "reply_concierge"
_VENTURE_KEY = "fa_max_lending"

# Autonomy stub — False until WP-T2-2 tool registry is wired.
# When True AND send count >= 25, high-confidence KB replies send autonomously.
_AUTONOMY_ENABLED = False
_AUTONOMY_THRESHOLD_SENDS = 25


@dataclass
class ConciergeOutcome:
    action: str  # kb_reply_sent | kb_reply_queued | escalated_exceptions | opt_out_suppressed | no_action
    classification: str
    kb_topic_key: Optional[str]
    confidence: float
    relay_queue_id: Optional[int]
    cost_usd: float


def handle_inbound(
    inbound_text: str,
    channel: str,
    person_id: Optional[str],
    contact_email: Optional[str],
    opportunity_id: Optional[str],
    borrower_first_name: Optional[str],
    db: Session,
    raw_payload: Optional[dict] = None,
) -> ConciergeOutcome:
    """
    Classify an inbound message and take the appropriate action.

    channel: email | sms | slack | webhook
    """
    now = datetime.now(timezone.utc)

    classification = classify_inbound(inbound_text)
    cost = classification.cost_usd

    # Halt any active abandonment sequence — borrower replied, stop all touches.
    if person_id:
        try:
            from src.agents.reply_concierge.abandonment_agent import halt_sequence
            halt_sequence(person_id, db, reason="reply_received")
        except Exception as _halt_exc:
            logger.warning("router: halt_sequence failed for person_id=%s: %s", person_id, _halt_exc)

    # ── Opt-out: immediate, irreversible, no reply ─────────────────────────
    if classification.intent == "opt_out":
        handle_opt_out(
            person_id=person_id,
            contact_email=contact_email,
            inbound_text=inbound_text,
            channel=channel,
            db=db,
        )
        # Log entry written by opt_out.handle_opt_out; return here.
        return ConciergeOutcome(
            action="opt_out_suppressed",
            classification="opt_out",
            kb_topic_key=None,
            confidence=classification.confidence,
            relay_queue_id=None,
            cost_usd=cost,
        )

    # ── Pricing escalate or below-threshold: straight to EXCEPTIONS ────────
    if (
        classification.intent == "below_threshold"
        or classification.confidence < CONFIDENCE_THRESHOLD
        or (classification.intent == "clarifying_question" and classification.kb_topic == "none")
    ):
        queue_id = _route_to_exceptions(
            person_id=person_id,
            opportunity_id=opportunity_id,
            inbound_text=inbound_text,
            classification=classification.intent,
            confidence=classification.confidence,
            suggested_reply=None,
            raw_payload=raw_payload,
            db=db,
        )
        _write_log(
            person_id=person_id,
            opportunity_id=opportunity_id,
            channel=channel,
            inbound_text=inbound_text,
            classification=classification.intent,
            kb_topic_key=None,
            confidence=classification.confidence,
            action="escalated_exceptions",
            relay_queue_id=queue_id,
            db=db,
        )
        return ConciergeOutcome(
            action="escalated_exceptions",
            classification=classification.intent,
            kb_topic_key=None,
            confidence=classification.confidence,
            relay_queue_id=queue_id,
            cost_usd=cost,
        )

    # ── Clarifying question with KB match ──────────────────────────────────
    if classification.intent == "clarifying_question" and classification.kb_topic != "none":
        reply = build_reply(
            kb_topic_key=classification.kb_topic,
            db=db,
            borrower_first_name=borrower_first_name,
        )

        if not reply.has_reply:
            # KB lookup failed — escalate
            queue_id = _route_to_exceptions(
                person_id=person_id,
                opportunity_id=opportunity_id,
                inbound_text=inbound_text,
                classification=classification.intent,
                confidence=classification.confidence,
                suggested_reply=None,
                raw_payload=raw_payload,
                db=db,
            )
            _write_log(
                person_id=person_id,
                opportunity_id=opportunity_id,
                channel=channel,
                inbound_text=inbound_text,
                classification=classification.intent,
                kb_topic_key=classification.kb_topic,
                confidence=classification.confidence,
                action="escalated_exceptions",
                relay_queue_id=queue_id,
                db=db,
            )
            return ConciergeOutcome(
                action="escalated_exceptions",
                classification=classification.intent,
                kb_topic_key=classification.kb_topic,
                confidence=classification.confidence,
                relay_queue_id=queue_id,
                cost_usd=cost,
            )

        # Autonomy check — stub: always queue for approval until T2-2 wires tier state
        is_autonomous = _AUTONOMY_ENABLED and _is_autonomous_eligible(db)
        action = "kb_reply_sent" if is_autonomous else "kb_reply_queued"

        queue_id = _queue_reply(
            person_id=person_id,
            opportunity_id=opportunity_id,
            contact_email=contact_email,
            reply_text=reply.reply_text,
            kb_topic_key=reply.kb_topic_key,
            channel=channel,
            auto_send=is_autonomous,
            db=db,
        )
        _write_log(
            person_id=person_id,
            opportunity_id=opportunity_id,
            channel=channel,
            inbound_text=inbound_text,
            classification=classification.intent,
            kb_topic_key=reply.kb_topic_key,
            confidence=classification.confidence,
            action=action,
            relay_queue_id=queue_id,
            db=db,
        )
        return ConciergeOutcome(
            action=action,
            classification=classification.intent,
            kb_topic_key=reply.kb_topic_key,
            confidence=classification.confidence,
            relay_queue_id=queue_id,
            cost_usd=cost,
        )

    # ── All other intents (interested, not_interested, referral, etc.) ──────
    # Route to EXCEPTIONS with context so Josh can decide the next move.
    queue_id = _route_to_exceptions(
        person_id=person_id,
        opportunity_id=opportunity_id,
        inbound_text=inbound_text,
        classification=classification.intent,
        confidence=classification.confidence,
        suggested_reply=None,
        raw_payload=raw_payload,
        db=db,
    )
    _write_log(
        person_id=person_id,
        opportunity_id=opportunity_id,
        channel=channel,
        inbound_text=inbound_text,
        classification=classification.intent,
        kb_topic_key=None,
        confidence=classification.confidence,
        action="escalated_exceptions",
        relay_queue_id=queue_id,
        db=db,
    )
    return ConciergeOutcome(
        action="escalated_exceptions",
        classification=classification.intent,
        kb_topic_key=None,
        confidence=classification.confidence,
        relay_queue_id=queue_id,
        cost_usd=cost,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _route_to_exceptions(
    person_id: Optional[str],
    opportunity_id: Optional[str],
    inbound_text: str,
    classification: str,
    confidence: float,
    suggested_reply: Optional[str],
    raw_payload: Optional[dict],
    db: Session,
) -> Optional[int]:
    idempotency_key = _idem_key("exceptions", person_id, inbound_text)
    payload = {
        "type": "concierge_exceptions",
        "classification": classification,
        "confidence": round(confidence, 3),
        "opportunity_id": opportunity_id,
        "suggested_reply": suggested_reply,
        "raw": raw_payload or {},
    }
    # inbound_snippet is intentionally excluded from relay_approval_queue payload:
    # the governance constraint blocks financial terms (interest rate, APR, etc.)
    # in fa_max_lending rows. The full snippet is stored in fa_max_concierge_log.
    try:
        row = db.execute(
            text("""
                INSERT INTO relay_approval_queue
                    (idempotency_key, venture_key, lane, channel, recipient, payload,
                     status, agent_name, autonomy_tier_at_send, person_id)
                VALUES
                    (:idem, :vk, 'EXCEPTIONS', 'noop', 'n/a',
                     CAST(:payload AS JSONB), 'pending', :agent, 'A', :pid)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
            """),
            {
                "idem": idempotency_key,
                "vk": _VENTURE_KEY,
                "payload": json.dumps(payload),
                "agent": _AGENT_NAME,
                "pid": person_id,
            },
        ).fetchone()
        db.commit()
        return row[0] if row else None
    except Exception as exc:
        db.rollback()
        logger.error("router: failed to write EXCEPTIONS row: %s", exc)
        return None


def _queue_reply(
    person_id: Optional[str],
    opportunity_id: Optional[str],
    contact_email: Optional[str],
    reply_text: str,
    kb_topic_key: Optional[str],
    channel: str,
    auto_send: bool,
    db: Session,
) -> Optional[int]:
    idempotency_key = _idem_key("reply", person_id, reply_text)
    payload = {
        "type": "concierge_reply",
        "kb_topic_key": kb_topic_key,
        "opportunity_id": opportunity_id,
        "reply_text": reply_text,
        "auto_send": auto_send,
    }
    # auto_send=True → status='approved' so the sweep sends it immediately.
    # auto_send=False → status='pending' so Josh sees it in EXCEPTIONS for approval.
    status = "approved" if auto_send else "pending"
    lane = "MONEY" if auto_send else "EXCEPTIONS"
    recipient = contact_email or "n/a"
    send_channel = "email" if contact_email else "noop"

    try:
        row = db.execute(
            text("""
                INSERT INTO relay_approval_queue
                    (idempotency_key, venture_key, lane, channel, recipient, payload,
                     status, agent_name, autonomy_tier_at_send, person_id)
                VALUES
                    (:idem, :vk, :lane, :channel, :recipient,
                     CAST(:payload AS JSONB), :status, :agent, 'A', :pid)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
            """),
            {
                "idem": idempotency_key,
                "vk": _VENTURE_KEY,
                "lane": lane,
                "channel": send_channel,
                "recipient": recipient,
                "payload": json.dumps(payload),
                "status": status,
                "agent": _AGENT_NAME,
                "pid": person_id,
            },
        ).fetchone()
        db.commit()
        return row[0] if row else None
    except Exception as exc:
        db.rollback()
        logger.error("router: failed to write reply queue row: %s", exc)
        return None


def _write_log(
    person_id: Optional[str],
    opportunity_id: Optional[str],
    channel: str,
    inbound_text: str,
    classification: str,
    kb_topic_key: Optional[str],
    confidence: float,
    action: str,
    relay_queue_id: Optional[int],
    db: Session,
) -> None:
    try:
        db.execute(
            text("""
                INSERT INTO fa_max_concierge_log
                    (person_id, opportunity_id, inbound_channel, inbound_snippet,
                     classification, kb_topic_key, confidence, action_taken,
                     autonomy_tier, relay_queue_id, created_at)
                VALUES
                    (:pid, :oid, :channel, :snippet, :cls, :topic,
                     :conf, :action, 'A', :qid, NOW())
            """),
            {
                "pid": person_id,
                "oid": opportunity_id,
                "channel": channel,
                "snippet": inbound_text[:500],
                "cls": classification,
                "topic": kb_topic_key,
                "conf": round(confidence, 3),
                "action": action,
                "qid": relay_queue_id,
            },
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("router: failed to write concierge_log: %s", exc)


def _idem_key(prefix: str, person_id: Optional[str], text: str) -> str:
    h = hashlib.sha256(f"{person_id}:{text[:200]}".encode()).hexdigest()[:16]
    return f"concierge:{prefix}:{h}"


def _is_autonomous_eligible(db: Session) -> bool:
    """Check whether Tier A autonomy threshold (25 approved sends) has been met."""
    try:
        row = db.execute(
            text("""
                SELECT COUNT(*) FROM relay_approval_queue
                WHERE venture_key = 'fa_max_lending'
                  AND agent_name  = 'reply_concierge'
                  AND status      IN ('sent', 'approved')
                  AND autonomy_tier_at_send = 'A'
            """)
        ).scalar()
        return int(row or 0) >= _AUTONOMY_THRESHOLD_SENDS
    except Exception as exc:
        logger.warning("router: autonomy check failed (%s) — defaulting to queued", exc)
        return False
