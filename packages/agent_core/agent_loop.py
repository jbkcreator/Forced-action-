"""Native Anthropic Messages API tool-calling loop. No agent framework, by design.

One call to :meth:`AgentLoop.run` answers one Slack message:

    system   = base prompt + safety invariants + <standing_rules> (rebuilt every turn)
    messages = thread history + the new message
    repeat up to ``max_rounds``:
        response = messages.create(... tools ...)
        no tool calls       -> return the text
        tool calls          -> ToolRegistry.execute each (egress is queued, never run)
                            -> all results go back in one user message

The assistant's full ``response.content`` (thinking blocks included) is appended unchanged
within a turn, as the API requires for tool use with thinking.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from .governance import compile_system_prompt
from .tools import ToolContext, ToolRegistry

logger = logging.getLogger(__name__)

DEFAULT_MAX_ROUNDS = 8
DEFAULT_MAX_TOKENS = 16_000

REFUSAL_REPLY = "I can't help with that request."
ROUND_LIMIT_REPLY = ("I ran out of steps before finishing. Try a narrower question, or ask for one thing at a time. "
                     "Nothing was sent.")
TRUNCATED_SUFFIX = "\n\n_(My answer was cut off; ask me to continue.)_"


class MessagesClient(Protocol):
    """The slice of ``anthropic.Anthropic().messages`` the loop uses; a fake stands in for tests."""

    def create(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class AgentReply:
    text: str
    rounds: int
    tool_calls: tuple[str, ...] = ()
    queued_action_ids: tuple[int, ...] = ()
    stop_reason: str = "end_turn"


@dataclass
class AgentLoop:
    client: MessagesClient
    model: str
    registry: ToolRegistry
    base_prompt: str
    rules_provider: Callable[[], Sequence[str]]
    max_rounds: int = DEFAULT_MAX_ROUNDS
    max_tokens: int = DEFAULT_MAX_TOKENS
    effort: str = "medium"
    _tool_definitions: list[dict[str, Any]] = field(init=False)

    def __post_init__(self) -> None:
        self._tool_definitions = self.registry.definitions()

    def run(self, *, history: Sequence[dict[str, Any]], user_text: str,
            context_for: Callable[[str], ToolContext]) -> AgentReply:
        """``context_for(tool_use_id)`` builds the per-call context (idempotency key = tool_use_id)."""
        system = compile_system_prompt(self.base_prompt, list(self.rules_provider()))
        messages: list[dict[str, Any]] = [*history, {"role": "user", "content": user_text}]
        tool_calls: list[str] = []
        queued: list[int] = []

        for round_number in range(1, self.max_rounds + 1):
            response = self.client.create(
                model=self.model, max_tokens=self.max_tokens, system=system, messages=messages,
                tools=self._tool_definitions, output_config={"effort": self.effort},
            )
            text_value = "\n".join(block.text for block in response.content
                                   if getattr(block, "type", None) == "text" and block.text).strip()

            if response.stop_reason == "refusal":
                logger.warning("agent loop: model declined (round %s)", round_number)
                return AgentReply(REFUSAL_REPLY, round_number, tuple(tool_calls), tuple(queued), "refusal")

            tool_uses = [block for block in response.content if getattr(block, "type", None) == "tool_use"]
            if response.stop_reason != "tool_use" or not tool_uses:
                if response.stop_reason == "max_tokens":
                    text_value = (text_value or "") + TRUNCATED_SUFFIX
                return AgentReply(text_value or "(no answer)", round_number, tuple(tool_calls), tuple(queued),
                                  response.stop_reason or "end_turn")

            messages.append({"role": "assistant", "content": response.content})
            results = []
            for block in tool_uses:
                tool_calls.append(block.name)
                outcome = self.registry.execute(block.name, block.input, context_for(block.id))
                if outcome.action_id is not None:
                    queued.append(outcome.action_id)
                results.append({"type": "tool_result", "tool_use_id": block.id, "content": outcome.content,
                                "is_error": outcome.is_error})
            logger.info("agent loop: round %s ran tools %s", round_number, [block.name for block in tool_uses])
            messages.append({"role": "user", "content": results})

        logger.warning("agent loop: stopped at the %s-round limit", self.max_rounds)
        return AgentReply(ROUND_LIMIT_REPLY, self.max_rounds, tuple(tool_calls), tuple(queued), "round_limit")
