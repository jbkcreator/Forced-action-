"""
Opportunity state machine — targeted -> touched -> replied -> call ->
proposal -> closed, keyed on Hunter's existing opportunity_thread_id.
Never mints a second id.

Thin, named wrapper over store.py's generic transition_opportunity(), so
callers (subgraphs) read as business logic rather than raw store calls.
"""
from __future__ import annotations

from typing import Optional

from src.agents.cora import store


def mark_targeted(opportunity_thread_id: str, reason: str = "target_produced") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "targeted", reason)


def mark_touched(opportunity_thread_id: str, reason: str = "draft_approved_sent") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "touched", reason)


def mark_replied(opportunity_thread_id: str, reason: str = "reply_received") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "replied", reason)


def mark_call(opportunity_thread_id: str, reason: str = "call_booked") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "call", reason)


def mark_proposal(opportunity_thread_id: str, reason: str = "proposal_sent") -> bool:
    return store.transition_opportunity(opportunity_thread_id, "proposal", reason)


def mark_closed(opportunity_thread_id: str, reason: str) -> bool:
    return store.transition_opportunity(opportunity_thread_id, "closed", reason)


def current_status(opportunity_thread_id: str) -> Optional[str]:
    return store.current_opportunity_status(opportunity_thread_id)


def is_awaiting_reply(opportunity_thread_id: str) -> bool:
    """True if the opportunity has been touched but hasn't advanced past it —
    the exact eligibility condition the follow-up scheduler checks."""
    return current_status(opportunity_thread_id) == "touched"
