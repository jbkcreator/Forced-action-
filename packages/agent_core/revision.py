"""Revise a held draft from an approver's instruction, then put it back in front of them.

Only the fields the host marks editable for the draft's channel (a text body, an email subject) can
change. Recipient fields are never sent to the model and never changed, so a revision cannot
redirect a message. The model returns the revised fields as structured output; the replaced
version is kept in the action's revision history and the redraft needs a fresh approval.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .agent_loop import MessagesClient
from .governance import compile_system_prompt
from .pending_actions import PendingAction, PendingActionQueue
from .send_gate import SendGate

logger = logging.getLogger(__name__)

_NOTE_LIMIT = 500


class RevisionFailed(RuntimeError):
    """The redraft could not be produced; the original draft stays as it was."""


def _schema(fields: Sequence[str]) -> dict[str, Any]:
    return {"type": "object", "properties": {name: {"type": "string"} for name in fields},
            "required": list(fields), "additionalProperties": False}


@dataclass
class DraftReviser:
    client: MessagesClient
    model: str
    queue: PendingActionQueue
    gate: SendGate
    base_prompt: str
    rules_provider: Callable[[], Sequence[str]]
    editable_fields: Mapping[str, tuple[str, ...]]

    def revise(self, action: PendingAction, instruction: str, user_id: str) -> int:
        """Apply the instruction to ``action`` and post a fresh card. Returns the action id."""
        fields = self.editable_fields.get(action.channel)
        if not fields:
            raise RevisionFailed(f"drafts on channel {action.channel} have no editable fields")
        current = {name: str(action.payload.get(name, "")) for name in fields}
        response = self.client.create(
            model=self.model, max_tokens=4000,
            system=compile_system_prompt(self.base_prompt, list(self.rules_provider())),
            messages=[{"role": "user", "content": (
                f"Draft ({action.tool_name}) fields:\n{json.dumps(current, indent=2)}\n\n"
                f"Requested change from <@{user_id}>: {instruction}"
            )}],
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": _schema(fields)}},
        )
        if response.stop_reason == "refusal":
            raise RevisionFailed("the model declined this revision")
        raw = next((block.text for block in response.content if getattr(block, "type", None) == "text"), "")
        try:
            revised = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RevisionFailed("the redraft was not valid JSON") from exc
        if set(revised) != set(fields) or not all(isinstance(value, str) and value.strip() for value in revised.values()):
            raise RevisionFailed("the redraft did not return every editable field")

        payload = {**action.payload, **{name: revised[name].strip() for name in fields}}
        if not self.queue.apply_revision(action.action_id, payload=payload, summary=action.summary,
                                         note=instruction[:_NOTE_LIMIT]):
            raise RevisionFailed("the draft is no longer open for revision")
        self.gate.post_card(action.action_id)
        logger.info("revision: action %s redrafted for %s", action.action_id, user_id)
        return action.action_id
