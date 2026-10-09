"""Tool definitions, Pydantic input validation, and the one place tool calls are dispatched.

Every tool declares a :class:`SafetyLevel`. Read-only and internal-write tools run inline.
An external-egress tool never runs: its handler only *builds* an :class:`EgressDraft`, which the
registry hands to the send gate (pending action + approval card). That interception lives here,
not in each tool, so a new egress tool cannot forget it.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError

from .governance import SafetyLevel, requires_human_approval
from .redaction import mask_long_digit_runs

logger = logging.getLogger(__name__)

_RESULT_CHAR_LIMIT = 12_000


class ToolInputError(Exception):
    """A handler's way to tell the model its request cannot be served (unknown lead, bad range).

    The message is returned to the model as an error result, so keep it free of PII.
    """


@dataclass(frozen=True)
class ToolContext:
    user_id: str
    is_approver: bool
    channel: str
    thread_ts: str
    tool_use_id: str


@dataclass(frozen=True)
class EgressDraft:
    """What an egress tool wants sent. Frozen into ``pending_actions`` and shown on the card."""

    channel: str
    payload: Mapping[str, Any]
    summary: str
    recipient_phone: str | None = None
    recipient_email: str | None = None
    contact_ref: str | None = None
    deal_ref: str | None = None


ToolHandler = Callable[[Any, ToolContext], Any]


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_model: type[BaseModel]
    safety: SafetyLevel
    handler: ToolHandler
    approver_only: bool = False

    def definition(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description,
                "input_schema": self.input_model.model_json_schema()}


@dataclass(frozen=True)
class ToolOutcome:
    content: str
    is_error: bool = False
    action_id: int | None = None


class DraftSubmitter(Protocol):
    def submit(self, draft: EgressDraft, *, tool_name: str, context: ToolContext) -> int: ...


def _render(result: Any) -> str:
    text = result if isinstance(result, str) else json.dumps(result, default=str, sort_keys=True)
    # Tool output reaches the model and, through its answers, Slack: no full phone or account numbers.
    text = mask_long_digit_runs(text)
    return text if len(text) <= _RESULT_CHAR_LIMIT else text[:_RESULT_CHAR_LIMIT] + "\n…(truncated)"


def _validation_message(error: ValidationError) -> str:
    problems = [f"{'.'.join(str(part) for part in issue['loc']) or 'input'}: {issue['msg']}"
                for issue in error.errors()[:5]]
    return "invalid input: " + "; ".join(problems)


@dataclass
class ToolRegistry:
    tools: Iterable[Tool]
    gate: DraftSubmitter
    _by_name: dict[str, Tool] = field(init=False)

    def __post_init__(self) -> None:
        self._by_name = {}
        for tool in self.tools:
            if tool.name in self._by_name:
                raise ValueError(f"duplicate tool name {tool.name!r}")
            self._by_name[tool.name] = tool

    def definitions(self) -> list[dict[str, Any]]:
        """Sorted, so the request prefix (and its cache) is stable from call to call."""
        return [self._by_name[name].definition() for name in sorted(self._by_name)]

    def execute(self, name: str, raw_input: Any, context: ToolContext) -> ToolOutcome:
        tool = self._by_name.get(name)
        if tool is None:
            return ToolOutcome(f"unknown tool {name!r}", is_error=True)
        if tool.approver_only and not context.is_approver:
            return ToolOutcome(f"only an approver can use {name}; tell the user so", is_error=True)
        try:
            arguments = tool.input_model.model_validate(raw_input or {})
        except ValidationError as error:
            return ToolOutcome(_validation_message(error), is_error=True)

        try:
            if requires_human_approval(tool.safety):
                return self._queue_for_approval(tool, arguments, context)
            return ToolOutcome(_render(tool.handler(arguments, context)))
        except ToolInputError as error:
            return ToolOutcome(str(error), is_error=True)
        except Exception as error:
            logger.error("tool %s failed (%s)", name, type(error).__name__)
            return ToolOutcome(f"{name} failed ({type(error).__name__}); nothing was changed", is_error=True)

    def _queue_for_approval(self, tool: Tool, arguments: BaseModel, context: ToolContext) -> ToolOutcome:
        draft = tool.handler(arguments, context)
        if not isinstance(draft, EgressDraft):
            raise TypeError(f"egress tool {tool.name} must return an EgressDraft")
        action_id = self.gate.submit(draft, tool_name=tool.name, context=context)
        return ToolOutcome(
            f"Queued for approval as action #{action_id}. Nothing has been sent; an approver must click "
            "Approve on the card first.",
            action_id=action_id,
        )
