"""T-B13-01 — buyer-facing dead-reason taxonomy for the one-tap outcome card.

Distinct from loss_autopsy's AI-inferred rejection taxonomy: these are the
reasons a buyer *taps* when marking a delivered lead dead. Each reason carries a
fault class that decides whether it feeds the CDS retune (client decision):

- LEAD_FAULT   — the lead itself was bad (bad contact, already gone, not
                 distressed, wrong owner). Lowers the lead score and is the
                 ONLY class routed into score_feedback / the CDS retune.
- BUYER_NEUTRAL — the buyer could not close for buyer-side / neutral reasons
                 (no conversation, declined, too busy, budget). Score-protected:
                 logged buyer-side only, never fed to scoring.
"""
from __future__ import annotations

LEAD_FAULT = "lead_fault"
BUYER_NEUTRAL = "buyer_neutral"

# reason -> fault class. The keys are the closed enum of tappable dead reasons.
DEAD_REASON_FAULT_CLASS: dict[str, str] = {
    "bad_contact_info": LEAD_FAULT,
    "already_sold_listed": LEAD_FAULT,
    "owner_not_distressed": LEAD_FAULT,
    "wrong_owner": LEAD_FAULT,
    "no_conversation": BUYER_NEUTRAL,
    "declined": BUYER_NEUTRAL,
    "too_busy": BUYER_NEUTRAL,
    "budget": BUYER_NEUTRAL,
}

VALID_DEAD_REASONS: frozenset[str] = frozenset(DEAD_REASON_FAULT_CLASS)
VALID_OUTCOME_STATES: frozenset[str] = frozenset({"closed", "dead", "pending"})


def fault_class_for(dead_reason: str) -> str:
    """Return the fault class for a dead reason, or raise on an unknown reason."""
    try:
        return DEAD_REASON_FAULT_CLASS[dead_reason]
    except KeyError as exc:
        raise ValueError(f"unknown dead_reason: {dead_reason!r}") from exc


def feeds_retune(dead_reason: str) -> bool:
    """True when this dead reason should reach the CDS retune (lead-fault only)."""
    return DEAD_REASON_FAULT_CLASS.get(dead_reason) == LEAD_FAULT
