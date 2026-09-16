"""WP-9 Dial List — in-place action handling (button taps + thread replies).

Turns a Slack ``block_actions`` payload — or a natural-language thread reply —
into a call disposition via the already-built ``record_dial_disposition``.
Closes the client's "actionable in place / Banks button model / run a full
working day from Slack alone" requirement (amendment 410-418).

Button semantics:
  - Won  -> terminal won  (record_dial_disposition)
  - Lost -> terminal lost + one of the 8 loss codes (record_dial_disposition)
  - Called / Skip -> non-terminal card-state updates; NO outcome row is written
    (Called = a touch marker, Skip = a today-only dismissal).

Single-approver gate, idempotency (thread-level UNIQUE in the outcomes table),
and card-update-on-success mirror the spec. Thread-reply understanding is a
deterministic keyword parser — no LLM (kept reproducible + testable).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from src.services.opportunity_outcome import LOSS_REASON_CODES

from .delivery import (
    ACTION_CALLED,
    ACTION_LOST,
    ACTION_SKIP,
    ACTION_WON,
)
from .disposition import DispositionResult, record_dial_disposition

logger = logging.getLogger(__name__)

_ACTOR = "josh"

# Non-terminal actions never write an outcome row.
_NON_TERMINAL = {ACTION_CALLED: "called", ACTION_SKIP: "skipped"}

_STATUS_LABEL = {
    "won": ":white_check_mark: Won",
    "lost": ":x: Lost",
    "called": ":phone: Called",
    "skipped": ":fast_forward: Skipped",
    "unauthorized": ":no_entry: Ignored (not the approver)",
    "no_thread": ":warning: No opportunity thread — can't code outcome",
}


@dataclass(frozen=True, slots=True)
class ActionResult:
    status: str                       # recorded | touched | ignored | error
    kind: Optional[str] = None        # won | lost | called | skipped
    opportunity_thread_id: Optional[str] = None
    loss_code: Optional[str] = None
    disposition: Optional[DispositionResult] = None
    updated_blocks: Optional[List[Dict[str, Any]]] = None
    message: Optional[str] = None


def _status_block(block_id: str, kind: str, now: datetime, detail: str = "") -> Dict[str, Any]:
    label = _STATUS_LABEL.get(kind, kind)
    suffix = f" ({detail})" if detail else ""
    return {
        "type": "context",
        "block_id": block_id,
        "elements": [
            {"type": "mrkdwn", "text": f"_{label}{suffix} · {now:%H:%M}_"}
        ],
    }


def _replace_action_block(
    blocks: List[Dict[str, Any]], block_id: str, replacement: Dict[str, Any]
) -> List[Dict[str, Any]]:
    return [replacement if b.get("block_id") == block_id else b for b in blocks]


def _parse_as_of(raw: Optional[str]) -> date:
    if raw:
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
    return date.today()


def handle_action(
    payload: Dict[str, Any],
    session: Session,
    *,
    approver_id: str = "",
    client: Any = None,
    now: Optional[datetime] = None,
) -> ActionResult:
    """Handle one Slack ``block_actions`` payload from a dial-list card.

    Records the disposition (won/lost) or marks a non-terminal touch
    (called/skip), and — when a ``client`` is supplied — updates the tapped
    card in place. Returns an :class:`ActionResult`; never raises on a business
    rule (invalid combos become an ``error`` result).
    """
    now = now or datetime.now()
    user_id = (payload.get("user") or {}).get("id")
    if approver_id and user_id != approver_id:
        logger.info("[DialList] action ignored — %s is not the approver", user_id)
        return ActionResult(status="ignored", kind=None, message="unauthorized")

    actions = payload.get("actions") or []
    if not actions:
        return ActionResult(status="error", message="no action in payload")
    action = actions[0]
    action_id = action.get("action_id")

    # value carrier: static_select uses selected_option.value, buttons use value
    if action_id == ACTION_LOST:
        selected = action.get("selected_option") or {}
        raw_value = selected.get("value")
    else:
        raw_value = action.get("value")
    try:
        data = json.loads(raw_value) if raw_value else {}
    except (TypeError, ValueError):
        data = {}

    thread = data.get("thread")
    as_of = _parse_as_of(data.get("as_of"))
    block_id = action.get("block_id")
    message = payload.get("message") or {}
    channel = (payload.get("channel") or {}).get("id")

    def _apply_update(kind: str, detail: str = "") -> Optional[List[Dict[str, Any]]]:
        blocks = message.get("blocks")
        if not (client and channel and message.get("ts") and blocks and block_id):
            return None
        new_blocks = _replace_action_block(
            blocks, block_id, _status_block(block_id, kind, now, detail)
        )
        try:
            client.chat_update(channel=channel, ts=message["ts"], blocks=new_blocks,
                               text="dial-list update")
        except Exception:  # a failed cosmetic update must not lose the write
            logger.warning("[DialList] chat_update failed for %s", thread, exc_info=True)
        return new_blocks

    # non-terminal: called / skip — no outcome row
    if action_id in _NON_TERMINAL:
        kind = _NON_TERMINAL[action_id]
        updated = _apply_update(kind)
        return ActionResult(status="touched", kind=kind,
                            opportunity_thread_id=thread, updated_blocks=updated)

    # Fail closed on terminal outcomes when no approver gate is configured.
    # Called/Skip above are non-terminal and harmless; Won/Lost write canonical
    # outcomes and must be gated even in dev when approver_id is unset.
    if not approver_id:
        logger.warning(
            "[DialList] terminal action %r rejected — "
            "DIAL_LIST_APPROVER_USER_ID not configured", action_id
        )
        return ActionResult(status="ignored", message="no approver configured")

    # terminal: won / lost
    if action_id == ACTION_WON:
        outcome, loss_code = "won", None
    elif action_id == ACTION_LOST:
        outcome, loss_code = "lost", data.get("loss_code")
    else:
        return ActionResult(status="error", message=f"unknown action {action_id!r}")

    if not thread:
        updated = _apply_update("no_thread")
        return ActionResult(status="error", kind=outcome,
                            message="no opportunity_thread_id", updated_blocks=updated)

    try:
        disp = record_dial_disposition(
            session, opportunity_thread_id=thread, outcome=outcome,
            loss_code=loss_code, actor=user_id or _ACTOR, as_of=as_of,
        )
    except ValueError as exc:
        logger.warning("[DialList] invalid disposition for %s: %s", thread, exc)
        return ActionResult(status="error", kind=outcome,
                            opportunity_thread_id=thread, message=str(exc))

    # If already terminal (inserted=False), show the canonical stored outcome
    # on the card — not the newly attempted action — so Slack and DB never diverge.
    canonical_outcome = disp.outcome if not disp.inserted else outcome
    canonical_code = disp.reason_code if not disp.inserted else loss_code
    if not disp.inserted:
        logger.info(
            "[DialList] thread %s already terminal (%s) — card updated to canonical outcome",
            thread, disp.outcome,
        )
    updated = _apply_update(canonical_outcome, detail=canonical_code or "")
    return ActionResult(status="recorded", kind=canonical_outcome,
                        opportunity_thread_id=thread, loss_code=canonical_code,
                        disposition=disp, updated_blocks=updated)


# ---------------------------------------------------------------------------
# Thread-reply parsing (deterministic keyword intent — no LLM)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ThreadIntent:
    kind: Optional[str]                 # won | lost | called | skipped | None
    loss_code: Optional[str] = None


def parse_thread_reply(text: str) -> ThreadIntent:
    """Map a free-text thread reply to a disposition intent. Deterministic."""
    if not text:
        return ThreadIntent(kind=None)
    t = text.lower()
    if "won" in t or "win" in t:
        return ThreadIntent(kind="won")
    if "lost" in t or "lose" in t or "loss" in t:
        code = next((c for c in LOSS_REASON_CODES if c.replace("_", " ") in t or c in t), None)
        return ThreadIntent(kind="lost", loss_code=code)
    if "called" in t or "call" in t or "reached" in t:
        return ThreadIntent(kind="called")
    if "skip" in t:
        return ThreadIntent(kind="skipped")
    return ThreadIntent(kind=None)


def handle_thread_reply(
    text: str,
    session: Session,
    *,
    opportunity_thread_id: str,
    as_of: Optional[date] = None,
) -> ActionResult:
    """Apply a parsed thread reply. The caller resolves which entry the thread
    belongs to (message-ts → opportunity mapping is a listener concern) and
    passes ``opportunity_thread_id`` in.
    """
    intent = parse_thread_reply(text)
    if intent.kind is None:
        return ActionResult(status="ignored", message="unrecognised reply")
    if intent.kind in ("called", "skipped"):
        return ActionResult(status="touched", kind=intent.kind,
                            opportunity_thread_id=opportunity_thread_id)
    if intent.kind == "lost" and not intent.loss_code:
        return ActionResult(status="error", kind="lost",
                            opportunity_thread_id=opportunity_thread_id,
                            message="lost reply needs a reason code")
    try:
        disp = record_dial_disposition(
            session, opportunity_thread_id=opportunity_thread_id,
            outcome=intent.kind, loss_code=intent.loss_code, actor=_ACTOR, as_of=as_of,
        )
    except ValueError as exc:
        return ActionResult(status="error", kind=intent.kind,
                            opportunity_thread_id=opportunity_thread_id, message=str(exc))
    return ActionResult(status="recorded", kind=intent.kind,
                        opportunity_thread_id=opportunity_thread_id,
                        loss_code=intent.loss_code, disposition=disp)
