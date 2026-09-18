"""
Tool registry for the FA Max agent runtime (WP-T2-2).

Deliberately NOT a reuse of src.agents.tools.registry.TOOL_REGISTRY (the
Lifecycle tool registry). That registry's ``allowed_graphs`` vocabulary is
Lifecycle-graph-name-scoped (fomo, abandonment_wave1, retention, ...) and its
compliance flag (``requires_compliance``) is defined specifically as "can
emit SMS/VM to a user" and is validated to only ever apply to
``category="write"`` tools. FA Max needs a materially different axis —
``requires_send_gate``, meaning "must pass check_tier_gate()/suppression_
reason() before it may run" — and registering FA Max tools into the same
module-level ``TOOL_REGISTRY`` dict as every Lifecycle tool would put both
systems' tool names into one shared global namespace for no benefit. This
module is therefore a small, FA-Max-scoped equivalent of the same pattern
(decorator + module registry + lookup), not a duplicate of its mechanics —
it does not reimplement idempotency/category validation beyond what FA Max
actually needs.

Exactly four tools are registered here — no more, per the WP-T2-2 spec's
"not pre-populated for tools that don't yet exist":

  - get_fa_max_person_state    (read,  requires_send_gate=False)
  - get_fa_max_person_history  (read,  requires_send_gate=False)
  - send                       (write, requires_send_gate=True)
  - check_suppression          (read,  requires_send_gate=False — a check,
                                 not a send)
  - post_slack                 (write, requires_send_gate=False — posting to
                                 Slack for human review is not an outbound
                                 contact to a suppressed party)

``send`` wraps src.services.relay.queue.enqueue(). Suppression
(fa_max_send_governance.suppression_reason) and consent
(fa_max_send_governance.require_consent) are already run unconditionally
inside enqueue() for venture_key='fa_max_lending', on both the auto_authorize
and human-approval branches — see enqueue()'s body. This module's `send`
tool does not re-implement or bypass that; it decides the auto_authorize
flag (via check_tier_gate) and lets enqueue() do the rest, including its own
fresh, unconditional post_for_approval() call for a still-pending row.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Literal, Optional

logger = logging.getLogger(__name__)

FaMaxToolCategory = Literal["read", "write"]


class FaMaxToolRegistrationError(Exception):
    """Raised when a tool is registered with invalid configuration."""


@dataclass(frozen=True)
class FaMaxToolSpec:
    name: str
    category: FaMaxToolCategory
    idempotent: bool
    requires_send_gate: bool
    description: str
    func: Callable


FA_MAX_TOOL_REGISTRY: Dict[str, FaMaxToolSpec] = {}


def fa_max_tool(
    *,
    category: FaMaxToolCategory,
    idempotent: bool,
    requires_send_gate: bool = False,
    name: Optional[str] = None,
) -> Callable[[Callable], Callable]:
    """Register a function as an FA Max agent tool."""
    if category not in ("read", "write"):
        raise FaMaxToolRegistrationError(
            f"Invalid tool category {category!r} — must be 'read' or 'write'"
        )

    def decorator(fn: Callable) -> Callable:
        tool_name = name or fn.__name__
        if tool_name in FA_MAX_TOOL_REGISTRY:
            raise FaMaxToolRegistrationError(
                f"FA Max tool {tool_name!r} is already registered — names must be unique"
            )
        spec = FaMaxToolSpec(
            name=tool_name,
            category=category,
            idempotent=idempotent,
            requires_send_gate=requires_send_gate,
            description=(fn.__doc__ or "").strip().split("\n")[0],
            func=fn,
        )
        FA_MAX_TOOL_REGISTRY[tool_name] = spec
        fn.__fa_max_tool_spec__ = spec  # type: ignore[attr-defined]
        return fn

    return decorator


def get_fa_max_tool(name: str) -> FaMaxToolSpec:
    """Look up an FA Max tool by name. Raises KeyError if unknown."""
    if name not in FA_MAX_TOOL_REGISTRY:
        raise KeyError(f"Unknown FA Max tool: {name!r}")
    return FA_MAX_TOOL_REGISTRY[name]


# ──────────────────────────────────────────────────────────────────────────
# Read tools — thin re-exports of the existing Lifecycle read tools. These
# are plain functions (the @tool decorator on them only attaches metadata
# and returns the function unchanged), so calling them directly here is
# exactly the same call as Lifecycle graphs already make.
# ──────────────────────────────────────────────────────────────────────────

@fa_max_tool(category="read", idempotent=True)
def get_fa_max_person_state(*, person_id: str, session=None) -> Dict[str, Any]:
    """Load FA Max person lifecycle state (delegates to src.agents.tools.read_tools)."""
    from src.agents.tools.read_tools import get_fa_max_person_state as _impl

    return _impl(person_id=person_id, session=session)


@fa_max_tool(category="read", idempotent=True)
def get_fa_max_person_history(
    *, person_id: str, limit: int = 100, after_seq: Optional[int] = None, session=None,
) -> Dict[str, Any]:
    """Return one page of an FA Max person's unified ordered history."""
    from src.agents.tools.read_tools import get_fa_max_person_history as _impl

    return _impl(person_id=person_id, limit=limit, after_seq=after_seq, session=session)


# ──────────────────────────────────────────────────────────────────────────
# check_suppression — read-only gate check, not itself a send.
# ──────────────────────────────────────────────────────────────────────────

@fa_max_tool(category="read", idempotent=True, requires_send_gate=False)
def check_suppression(*, recipient: str, channel: str, session) -> Dict[str, Any]:
    """Return the current suppression reason for a recipient/channel, or None."""
    from src.services.fa_max_send_governance import suppression_reason

    reason = suppression_reason(session, recipient=recipient, channel=channel)
    return {"recipient": recipient, "channel": channel, "suppressed": reason is not None, "reason": reason}


# ──────────────────────────────────────────────────────────────────────────
# send — the one write path into relay_approval_queue for FA Max.
# ──────────────────────────────────────────────────────────────────────────

@fa_max_tool(category="write", idempotent=True, requires_send_gate=True)
def send(
    *,
    idempotency_key: str,
    channel: str,
    recipient: str,
    payload: dict,
    agent_name: str,
    lane: str,
    autonomy_tier_at_send: str,
    person_id: str,
    thread_id: Optional[str] = None,
    session,
) -> Dict[str, Any]:
    """Propose an outbound FA Max send via relay_approval_queue.enqueue().

    Decides auto_authorize by calling check_tier_gate(agent_name, tier,
    session) fresh — this decision belongs here, in the tool wrapper, not
    duplicated elsewhere. enqueue() itself re-verifies the tier gate and
    suppression fresh, immediately before ever marking a row 'approved'
    (see its docstring), and unconditionally calls post_for_approval() at
    the end for every fa_max_lending item — a no-op there for anything not
    still 'pending'. This tool does not call post_for_approval() again;
    doing so would just be a second, redundant no-op/race against the one
    enqueue() already performs.

    On success for a row that is still 'pending' (i.e. auto_authorize was
    False, or the fresh re-check inside enqueue() declined it), captures
    the drafted content into original_draft via capture_original_draft() —
    once, so a later Slack Revise never overwrites the pre-revision text.
    """
    from src.services.fa_max_autonomy import check_tier_gate
    from src.services.relay import queue as relay_queue

    gate = check_tier_gate(agent_name, autonomy_tier_at_send, session)

    item = relay_queue.enqueue(
        idempotency_key=idempotency_key,
        channel=channel,
        recipient=recipient,
        payload=payload,
        thread_id=thread_id,
        venture_key="fa_max_lending",
        lane=lane,
        agent_name=agent_name,
        autonomy_tier_at_send=autonomy_tier_at_send,
        person_id=person_id,
        auto_authorize=gate.allowed,
    )

    if item.status == "pending":
        draft = payload.get("body") if isinstance(payload, dict) and payload.get("body") else str(payload)
        relay_queue.capture_original_draft(item.id, draft=draft)

    return {
        "item_id": item.id,
        "status": item.status,
        "auto_authorize_requested": gate.allowed,
        "tier_gate_outcome": gate.outcome.value,
    }


# ──────────────────────────────────────────────────────────────────────────
# post_slack — explicit (re)post of a queue item's approval card.
# ──────────────────────────────────────────────────────────────────────────

@fa_max_tool(category="write", idempotent=True, requires_send_gate=False)
def post_slack(*, item_id: int) -> Dict[str, Any]:
    """(Re)post a pending relay_approval_queue item's Slack approval card.

    Wraps src.services.relay.slack_post.post_for_approval, which already
    no-ops for anything not still 'pending' with no message posted yet —
    safe to call speculatively (e.g. a retry after a prior Slack outage).
    Not a contact to a suppressed party: this posts an internal card to
    Josh's approval channel, not an outbound message to a borrower.
    """
    from src.services.relay import queue as relay_queue
    from src.services.relay.slack_post import post_for_approval

    item = relay_queue.get_item(item_id)
    if item is None:
        return {"item_id": item_id, "posted": False, "reason": "not_found"}
    post_for_approval(item)
    refreshed = relay_queue.get_item(item_id)
    return {
        "item_id": item_id,
        "posted": bool(refreshed and refreshed.slack_message_ts),
    }
