"""
StepTracker — progressive Slack message updates for the CC agentic loop.

Updates the "thinking..." placeholder in-place as the agent works through
each step. Steps accumulate visually until the final answer starts streaming,
at which point the message switches to clean answer mode.

Thread-safe: on_answer_chunk is called from the streaming thread.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

_SLACK_UPDATE_INTERVAL = 0.9  # max Slack updates per second

_TOOL_ICONS = {
    "query_db":          "🗄️",
    "evaluate_deal":     "🏠",
    "get_backward_math": "🔢",
    "get_scoreboard":    "📈",
    "search_opportunity":"🔍",
}

_TOOL_LABELS = {
    "query_db":          "Querying database",
    "evaluate_deal":     "Evaluating deal",
    "get_backward_math": "Calculating backward math",
    "get_scoreboard":    "Pulling scoreboard",
    "search_opportunity":"Searching pipeline",
}


def _brief_result(tool_name: str, result_json: str) -> str:
    """One-line summary of a tool result for the step card."""
    import json
    try:
        data = json.loads(result_json[6:] if result_json.startswith("DATA: ") else result_json)
    except Exception:
        return "done"

    if "error" in data:
        return f"error: {str(data['error'])[:60]}"

    if tool_name == "evaluate_deal":
        return data.get("status", "evaluated")

    if tool_name == "get_backward_math":
        stage = data.get("starving_stage", "")
        deficit = data.get("starving_deficit_per_month", "")
        return f"starving stage: {stage} (deficit {deficit}/mo)" if stage else "computed"

    if tool_name == "get_scoreboard":
        outreaches = data.get("outreaches_sent", "?")
        rate = data.get("reply_rate")
        rate_str = f" | reply rate {rate:.1%}" if rate is not None else ""
        return f"{outreaches} outreaches{rate_str}"

    if tool_name == "search_opportunity":
        count = data.get("count", 0)
        return f"{count} match(es) found"

    return "done"


class StepTracker:
    def __init__(self, channel: str, placeholder_ts: str, bot_token: str) -> None:
        self._channel = channel
        self._ts = placeholder_ts
        self._token = bot_token
        self._steps: List[str] = []
        self._current: Optional[str] = None
        self._answer_mode = False
        self._answer_buf: List[str] = []
        self._last_update = 0.0
        self._lock = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def thinking(self) -> None:
        """Initial state — Claude is deciding what to do."""
        self._set_current("🧠", "Thinking...")
        self._push()

    def tool_start(self, tool_name: str, tool_input: dict) -> None:
        """A tool call has been dispatched."""
        icon = _TOOL_ICONS.get(tool_name, "⚙️")
        label = _TOOL_LABELS.get(tool_name, tool_name)
        with self._lock:
            if tool_name == "search_opportunity":
                name = tool_input.get("name", "")[:40]
                self._current = f"{icon} _{label}: *{name}*…_"
            elif tool_name == "evaluate_deal":
                prop = tool_input.get("property_type", "")
                state = tool_input.get("state", "")
                self._current = f"{icon} _{label}: {prop} in {state}…_"
            else:
                self._current = f"{icon} _{label}…_"
        self._push()

    def tool_done(self, tool_name: str, result_json: str) -> None:
        """A tool call completed — move to completed steps."""
        icon = _TOOL_ICONS.get(tool_name, "⚙️")
        label = _TOOL_LABELS.get(tool_name, tool_name)
        brief = _brief_result(tool_name, result_json)
        status_icon = "❌" if brief.startswith("error:") else "✅"
        # Database steps show only the label: the SQL and its result stay out of Slack.
        step = f"{icon} {label}" if tool_name == "query_db" else f"{icon} {label}: {brief}"
        with self._lock:
            self._current = None
            self._steps.append(f"{status_icon} {step}")
        self._push()

    def writing(self) -> None:
        """Final Claude call has started — about to stream the answer."""
        self._set_current("✍️", "Writing answer…")
        self._push()

    def on_answer_chunk(self, chunk: str) -> None:
        """Called for each streamed text token from Claude's final answer."""
        with self._lock:
            self._answer_mode = True
            self._answer_buf.append(chunk)
            now = time.monotonic()
            if now - self._last_update < _SLACK_UPDATE_INTERVAL:
                return
            self._last_update = now
            text = "".join(self._answer_buf) + " ▌"
        self._update_slack(text)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _set_current(self, icon: str, label: str) -> None:
        with self._lock:
            self._current = f"{icon} _{label}_"

    def _render(self) -> str:
        parts = list(self._steps)
        if self._current:
            parts.append(self._current)
        return "\n".join(parts) if parts else "⏳ _Looking that up…_"

    def _push(self) -> None:
        with self._lock:
            if self._answer_mode:
                return
            now = time.monotonic()
            if now - self._last_update < _SLACK_UPDATE_INTERVAL:
                return
            self._last_update = now
            text = self._render()
        self._update_slack(text)

    def _update_slack(self, text: str) -> None:
        try:
            from slack_sdk import WebClient
            WebClient(token=self._token).chat_update(
                channel=self._channel, ts=self._ts, text=text,
            )
        except Exception as exc:
            logger.debug("step_tracker: slack update failed: %s", exc)
