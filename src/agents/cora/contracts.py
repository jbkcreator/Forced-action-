"""
Cora's cross-team seams — defined now, per the "do not block on neighboring
Week 2 tasks" instruction. Nothing here is built against a real backing
system; each is a typed interface plus (where noted) a naive stand-in
implementation that's swappable later with no signature change.

- REVINT I1 (real ranking)              -> src.agents.cora.fallback_ranking
- REVINT I2 (real offer recommendation) -> recommend_offer_stub() below
- THROUGH T1/T2 (batch-approval output) -> to_batch_approval_item() below
- QUALITY Q1/Q3 (fleet event schema)    -> FleetEvent + emit_fleet_event_stub() below
- Relay handoff payload                 -> RelayHandoffPayload below (Cora never calls Relay)
- Reply-mailbox input format            -> ReplyStubPayload below (the one genuinely-external stub)
- Call-booked input format              -> CallBookedStubPayload below
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TypedDict

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# REVINT I2 stand-in — offer recommendation
# ─────────────────────────────────────────────────────────────────────────────

class OfferRecommendation(TypedDict):
    offer: str
    reason: str
    confidence: float  # 0-1, naive


def recommend_offer_stub(buyer_entity: Dict[str, Any]) -> OfferRecommendation:
    """Naive default until REVINT I2 ships. Swappable with no signature change."""
    if buyer_entity.get("is_whale"):
        return {"offer": "founder_tier", "reason": "is_whale=True", "confidence": 0.6}
    return {"offer": "core_subscription", "reason": "default", "confidence": 0.3}


# ─────────────────────────────────────────────────────────────────────────────
# THROUGH T1/T2 — batch-approval output contract
# ─────────────────────────────────────────────────────────────────────────────

class BatchApprovalItem(TypedDict):
    draft_id: str
    opportunity_thread_id: str
    cell_id: str
    offer: str
    subject: str
    channel: str
    booking_link: Optional[str]
    payment_link: Optional[str]
    created_at: str


def to_batch_approval_item(draft: Dict[str, Any]) -> BatchApprovalItem:
    """Read-only projection over an OutboundDraftRecord dict — no mutation of Cora internals."""
    return {
        "draft_id": draft["draft_id"],
        "opportunity_thread_id": draft["opportunity_thread_id"],
        "cell_id": draft["cell_id"],
        "offer": draft["offer"],
        "subject": draft["subject"],
        "channel": draft["recommended_channel"],
        "booking_link": draft.get("booking_link"),
        "payment_link": draft.get("payment_link"),
        "created_at": draft["created_at"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# QUALITY Q1/Q3 — fleet event schema (log-only stub, never a real dispatcher)
# ─────────────────────────────────────────────────────────────────────────────

class FleetEvent(TypedDict):
    event_type: str
    source_agent: str
    opportunity_thread_id: Optional[str]
    occurred_at: str
    payload: Dict[str, Any]


def emit_fleet_event_stub(event: FleetEvent) -> None:
    """Log-only. Never touches src.services.business_events — Quality owns that formalization."""
    logger.info("fleet_event_stub: %s", event)


def make_fleet_event(event_type: str, opportunity_thread_id: Optional[str], **payload: Any) -> FleetEvent:
    return {
        "event_type": event_type,
        "source_agent": "cora",
        "opportunity_thread_id": opportunity_thread_id,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Relay handoff payload — Cora never calls Relay, only defines the shape
# ─────────────────────────────────────────────────────────────────────────────

class RelayHandoffPayload(TypedDict):
    draft_id: str
    opportunity_thread_id: str
    channel: str
    subject: str
    body: str
    booking_link: Optional[str]
    payment_link: Optional[str]
    approved_by: Optional[str]
    approved_at: Optional[str]


def to_relay_handoff_payload(draft: Dict[str, Any]) -> RelayHandoffPayload:
    return {
        "draft_id": draft["draft_id"],
        "opportunity_thread_id": draft["opportunity_thread_id"],
        "channel": draft["recommended_channel"],
        "subject": draft["subject"],
        "body": draft["body"],
        "booking_link": draft.get("booking_link"),
        "payment_link": draft.get("payment_link"),
        "approved_by": None,
        "approved_at": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Reply-mailbox stub payload — the one genuinely-external, unconfirmed format
# ─────────────────────────────────────────────────────────────────────────────

class ReplyStubPayload(TypedDict):
    opportunity_thread_id: Optional[str]  # None if unresolvable from headers/body
    from_address: str
    subject: str
    body_text: str
    received_at: str
    raw_headers: Dict[str, str]


class CallBookedStubPayload(TypedDict):
    opportunity_thread_id: str
    call_booked_at: str
    rep: Optional[str]
    scheduled_for: Optional[str]
