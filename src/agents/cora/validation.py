"""
Cora's draft-reject gates.

Five checks, in order, each able to reject a draft before it's ever
composed or persisted:

    1. kill switch active
    2. confidence below Hunter's floor (src.agents.hunter.gating.is_citable)
    3. facts stale or missing
    4. target suppressed
    5. duplicate actionable draft already exists

All deterministic, no LLM involved anywhere in this module — matches the
constitution's "do not use an LLM for ... validation" rule.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from sqlalchemy.orm import Session

from src.agents.cora import store
from src.agents.cora.kill_switch import cora_halted
from src.agents.hunter.gating import is_citable

RejectReason = Literal[
    "kill_switch_active",
    "low_confidence",
    "stale_facts",
    "facts_missing",
    "suppressed",
    "duplicate_actionable",
]


@dataclass
class DraftValidationResult:
    allowed: bool
    reject_reason: Optional[RejectReason] = None
    detail: Optional[str] = None


def _check_facts(facts_used: List[Dict[str, Any]]) -> Optional[DraftValidationResult]:
    if not facts_used:
        return DraftValidationResult(allowed=False, reject_reason="facts_missing", detail="no facts_used supplied")
    for fact in facts_used:
        freshness_class = fact.get("freshness_class", "generic")
        observed_at = fact.get("observed_at")
        if observed_at is None:
            return DraftValidationResult(
                allowed=False, reject_reason="facts_missing",
                detail=f"fact {fact.get('fact_key')!r} has no observed_at",
            )
        if store.is_stale(freshness_class, observed_at):
            return DraftValidationResult(
                allowed=False, reject_reason="stale_facts",
                detail=f"fact {fact.get('fact_key')!r} stale (freshness_class={freshness_class})",
            )
    return None


def _check_suppression(
    *,
    email: Optional[str],
    phone: Optional[str],
    channel: str,
    db: Session,
) -> Optional[DraftValidationResult]:
    # Pure-read checks, confirmed safe to call at draft time (no send side
    # effects) — src.services.email_suppression.is_email_suppressed and
    # src.services.compliance_gator.validate_outbound.
    if channel == "email" and email:
        from src.services.email_suppression import is_email_suppressed
        if is_email_suppressed(db, email):
            return DraftValidationResult(allowed=False, reject_reason="suppressed", detail="email opted out")
    if channel in ("sms", "voice") and phone:
        from src.services.compliance_gator import validate_outbound
        result = validate_outbound(phone, channel, db)
        if not result.allowed:
            return DraftValidationResult(allowed=False, reject_reason="suppressed", detail=result.reason)
    return None


def validate_can_draft(
    *,
    buyer_entity: Dict[str, Any],
    cell_id: str,
    facts_used: List[Dict[str, Any]],
    recommended_channel: str,
    db: Session,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    is_followup: bool = False,
) -> DraftValidationResult:
    """
    Enforces, in order: kill switch -> confidence -> fact staleness/missing
    -> suppression -> duplicate actionable draft.

    buyer_entity is expected to carry at least: opportunity_thread_id,
    confidence_score. Caller (subgraphs/outreach.py) is responsible for
    passing the actual contact channel/address it intends to use.

    is_followup=True (src.agents.cora.followup_scheduler) skips the
    duplicate-actionable-draft check — a scheduled follow-up is an
    INTENTIONAL second touch on the same cell_id, not an accidental replay
    of the same event; the accidental-replay case is caught upstream by the
    worker's idempotency-key gate instead.
    """
    if cora_halted():
        return DraftValidationResult(allowed=False, reject_reason="kill_switch_active")

    confidence_score = int(buyer_entity.get("confidence_score", 0) or 0)
    if not is_citable(confidence_score):
        return DraftValidationResult(
            allowed=False, reject_reason="low_confidence",
            detail=f"confidence_score={confidence_score} < Hunter's UNVERIFIED_FLOOR",
        )

    facts_result = _check_facts(facts_used)
    if facts_result is not None:
        return facts_result

    suppression_result = _check_suppression(email=email, phone=phone, channel=recommended_channel, db=db)
    if suppression_result is not None:
        return suppression_result

    opportunity_thread_id = buyer_entity.get("opportunity_thread_id")
    if (
        not is_followup
        and opportunity_thread_id
        and store.has_duplicate_actionable_draft(db, opportunity_thread_id, cell_id)
    ):
        return DraftValidationResult(allowed=False, reject_reason="duplicate_actionable")

    return DraftValidationResult(allowed=True)
