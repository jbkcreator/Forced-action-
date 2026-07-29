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
from typing import Any, Dict, List, Optional, TypedDict

from config.cora_guardrails import CONCIERGE_ELIGIBLE_SIGNALS, OFFER_RULE_CONFIG

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# REVINT I2 — offer recommendation
# ─────────────────────────────────────────────────────────────────────────────

class OfferRecommendation(TypedDict):
    offer: str
    reason: str
    confidence: float               # 0-1
    rule_priority: int              # which rule fired (lower = higher priority)
    fallback_offer: Optional[str]   # if primary offer unavailable
    matched_rule_id: str            # e.g. "whale_founder_tier"
    signals_used: List[str]         # e.g. ["is_whale", "total_purchase_count"]
    config_version: str             # for calibration drift detection
    alternative_offer: Optional[str]  # next-best offer


def _is_lapsed_subscriber(buyer_entity: Dict[str, Any]) -> bool:
    """True when entity had a subscription but has no active one now."""
    return (
        not buyer_entity.get("has_active_subscription", False)
        and buyer_entity.get("had_prior_subscription", False)
    )


def _has_concierge_signals(buyer_entity: Dict[str, Any]) -> bool:
    signals: list[str] = buyer_entity.get("signals", [])
    return any(s in CONCIERGE_ELIGIBLE_SIGNALS for s in signals)


def _has_lender_intro_signals(buyer_entity: Dict[str, Any]) -> bool:
    signals: list[str] = buyer_entity.get("signals", [])
    return any(s in {"hard_money_lender", "lender_intro_requested"} for s in signals)


def recommend_offer(buyer_entity: Dict[str, Any]) -> OfferRecommendation:
    """
    Per-prospect offer recommendation — REVINT I2.
    Rule priority: lower number fires first. First match wins.
    """
    config_version: str = str(OFFER_RULE_CONFIG["config_version"])
    whale_threshold: float = float(OFFER_RULE_CONFIG["whale_confidence_threshold"])
    multi_purchase_threshold: int = int(OFFER_RULE_CONFIG["multi_purchase_threshold"])

    # Priority 1 — whale + founder tier
    if buyer_entity.get("is_whale"):
        return {
            "offer": "founder_tier",
            "reason": "is_whale=True",
            "confidence": whale_threshold,
            "rule_priority": 1,
            "fallback_offer": "core_subscription",
            "matched_rule_id": "whale_founder_tier",
            "signals_used": ["is_whale"],
            "config_version": config_version,
            "alternative_offer": "core_subscription",
        }

    # Priority 2 — auction winner → single ZIP pack
    entity_links: list[str] = buyer_entity.get("entity_links", [])
    if "BuyerEntityLink" in entity_links and buyer_entity.get("is_auction_winner"):
        return {
            "offer": "single_ZIP_pack",
            "reason": "BuyerEntityLink auction winner",
            "confidence": 0.75,
            "rule_priority": 2,
            "fallback_offer": "core_subscription",
            "matched_rule_id": "auction_winner_zip_pack",
            "signals_used": ["BuyerEntityLink", "is_auction_winner"],
            "config_version": config_version,
            "alternative_offer": "core_subscription",
        }

    # Priority 3 — lapsed subscriber win-back
    if _is_lapsed_subscriber(buyer_entity):
        return {
            "offer": "core_subscription",
            "reason": f"lapsed subscriber — {OFFER_RULE_CONFIG['winback_discount_pct']}% win-back config",
            "confidence": 0.65,
            "rule_priority": 3,
            "fallback_offer": "bankruptcy_alert",
            "matched_rule_id": "lapsed_sub_winback",
            "signals_used": ["has_active_subscription", "had_prior_subscription"],
            "config_version": config_version,
            "alternative_offer": "bankruptcy_alert",
        }

    # Priority 4 — multi-purchase, not whale
    total_purchases: int = int(buyer_entity.get("total_purchase_count", 0))
    if total_purchases >= multi_purchase_threshold and not buyer_entity.get("is_whale"):
        return {
            "offer": "core_subscription",
            "reason": f"total_purchase_count >= {multi_purchase_threshold}",
            "confidence": 0.7,
            "rule_priority": 4,
            "fallback_offer": "lead_packs",
            "matched_rule_id": "multi_purchase_core",
            "signals_used": ["total_purchase_count"],
            "config_version": config_version,
            "alternative_offer": "lead_packs",
        }

    # Priority 5 — DBPR contractor → insurance distress pack
    if buyer_entity.get("entity_type") == "contractor":
        return {
            "offer": "insurance_distress_pack",
            "reason": "entity_type=contractor",
            "confidence": 0.65,
            "rule_priority": 5,
            "fallback_offer": "core_subscription",
            "matched_rule_id": "dbpr_contractor",
            "signals_used": ["entity_type"],
            "config_version": config_version,
            "alternative_offer": "core_subscription",
        }

    # Priority 6 — concierge-eligible signals
    if _has_concierge_signals(buyer_entity):
        return {
            "offer": "concierge_wedge",
            "reason": "concierge eligibility signals matched",
            "confidence": 0.6,
            "rule_priority": 6,
            "fallback_offer": "core_subscription",
            "matched_rule_id": "concierge_wedge_default",
            "signals_used": [s for s in buyer_entity.get("signals", []) if s in CONCIERGE_ELIGIBLE_SIGNALS],
            "config_version": config_version,
            "alternative_offer": "core_subscription",
        }

    # Priority 7 — hard money lender intro
    # RESPA gate: no fee mechanics until Josh confirms clearance in writing
    if _has_lender_intro_signals(buyer_entity):
        return {
            "offer": "hard_money_intro",
            "reason": "lender-intro signals present — booking only, no price",
            "confidence": 0.55,
            "rule_priority": 7,
            "fallback_offer": None,  # booking-only, no fallback price path
            "matched_rule_id": "hard_money_intro_lender",
            "signals_used": [s for s in buyer_entity.get("signals", []) if s in {"hard_money_lender", "lender_intro_requested"}],
            "config_version": config_version,
            "alternative_offer": None,
        }

    # Priority 99 — default fallback
    return {
        "offer": "core_subscription",
        "reason": "default",
        "confidence": 0.3,
        "rule_priority": 99,
        "fallback_offer": None,
        "matched_rule_id": "default_core_sub",
        "signals_used": [],
        "config_version": config_version,
        "alternative_offer": None,
    }


# Backward-compat alias — callers using the old stub name continue to work.
recommend_offer_stub = recommend_offer


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
