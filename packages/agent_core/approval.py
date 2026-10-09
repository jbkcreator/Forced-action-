"""Universal human send gate: Block Kit approval cards and click handling.

A queued egress action is posted as a card with three buttons: Approve, Revise, Reject. Each
button carries the action id. Clicks arrive over Socket Mode; the logic here is pure with
respect to Slack so it is testable without a workspace. Only a configured approver's click
counts. An empty approver set approves nothing (fail closed).
"""
from __future__ import annotations

import enum
import json
from collections.abc import Set
from dataclasses import dataclass

from .pending_actions import PendingAction, PendingActionQueue, is_expired
from .redaction import slack_safe

_SECTION_TEXT_LIMIT = 2800
_HEADER_LIMIT = 150


class ApprovalAction(enum.Enum):
    APPROVE = "agent_core_approve"
    REVISE = "agent_core_revise"
    REJECT = "agent_core_reject"


_BUTTONS: tuple[tuple[ApprovalAction, str, str | None], ...] = (
    (ApprovalAction.APPROVE, "Approve", "primary"),
    (ApprovalAction.REVISE, "Revise", None),
    (ApprovalAction.REJECT, "Reject", "danger"),
)


@dataclass(frozen=True)
class ClickOutcome:
    accepted: bool
    status_text: str
    dispatch: bool = False


_EXPIRED_TEXT = "This draft expired before it was approved; ask Cora to redraft it."


def _payload_preview(action: PendingAction) -> str:
    rendered = json.dumps(dict(action.payload), indent=2, sort_keys=True, ensure_ascii=False)
    if len(rendered) > _SECTION_TEXT_LIMIT:
        rendered = rendered[:_SECTION_TEXT_LIMIT] + "\n…"
    return f"```{slack_safe(rendered)}```"


def render_action_card(action: PendingAction) -> list[dict]:
    header = f"Approval needed: {action.tool_name}"[:_HEADER_LIMIT]
    summary = slack_safe(action.summary) or "_No summary provided._"
    if action.revision_note:
        summary += f"\n_Revised: {slack_safe(action.revision_note)}_"
    buttons = []
    for button_action, label, style in _BUTTONS:
        button = {"type": "button", "action_id": button_action.value,
                  "text": {"type": "plain_text", "text": label}, "value": str(action.action_id)}
        if style:
            button["style"] = style
        buttons.append(button)
    return [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        {"type": "section", "text": {"type": "mrkdwn", "text": _payload_preview(action)}},
        {"type": "actions", "block_id": f"pending_action::{action.action_id}", "elements": buttons},
        {"type": "context", "elements": [{"type": "mrkdwn",
                                          "text": f"action `{action.action_id}` · channel `{action.channel}`"}]},
    ]


def render_decided_card(action: PendingAction, status_text: str) -> list[dict]:
    """The card after a decision: same content, buttons replaced by the outcome."""
    blocks = [block for block in render_action_card(action) if block["type"] != "actions"]
    blocks.insert(len(blocks) - 1, {"type": "section", "text": {"type": "mrkdwn", "text": status_text}})
    return blocks


def is_approver(approver_user_ids: Set[str], user_id: str) -> bool:
    return bool(user_id) and user_id in approver_user_ids


def parse_action_id(raw: str | None) -> int | None:
    value = (raw or "").strip()
    return int(value) if value.isdigit() else None


def apply_click(queue: PendingActionQueue, action: ApprovalAction, action_id: int, user_id: str,
                approver_user_ids: Set[str]) -> ClickOutcome:
    """Apply one button click. ``dispatch`` tells the caller to hand the action to the relay."""
    if not is_approver(approver_user_ids, user_id):
        return ClickOutcome(accepted=False, status_text="Only an approver can decide this action.")

    if action is ApprovalAction.APPROVE:
        if queue.approve(action_id, user_id):
            return ClickOutcome(accepted=True, status_text=f":white_check_mark: *Approved* by <@{user_id}>, sending…",
                                dispatch=True)
        current = queue.get(action_id)
        if current is not None and is_expired(current, queue.now()):
            return ClickOutcome(accepted=False, status_text=_EXPIRED_TEXT)
        return ClickOutcome(accepted=False, status_text="Already decided; nothing changed.")

    if action is ApprovalAction.REJECT:
        if queue.reject(action_id, user_id):
            return ClickOutcome(accepted=True, status_text=f":x: *Rejected* by <@{user_id}>. Nothing was sent.")
        return ClickOutcome(accepted=False, status_text="Already decided; nothing changed.")

    if queue.request_revision(action_id, user_id):
        return ClickOutcome(
            accepted=True,
            status_text=(f":pencil2: *Revising* for <@{user_id}>: reply with the change you want. "
                         "Say `cancel` to keep the draft as it is."),
        )
    return ClickOutcome(accepted=False, status_text="This action can no longer be revised.")
