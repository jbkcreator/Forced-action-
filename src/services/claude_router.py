"""
Claude API router — model selection, prompt caching, cost tracking.

All Claude calls in this codebase go through call_claude().
Nothing calls anthropic.messages.create() directly.

Routing logic:
    Haiku  (~80% of calls): sms_copy, classification, command_parsing, batch_summarization
    Sonnet (~18% of calls): conversational_close, complex_reasoning, lead_analysis
    Opus   (~2%  of calls): edge_cases (explicit override only)

Cost tracking:
    Every call writes one row to api_usage_logs. Query that table to monitor
    per-task costs and validate Haiku routing savings over time.

Prompt caching:
    Pass cache=True on any system prompt that is reused across many calls
    (e.g. Lifecycle persona prompt, ZIP stats context). Anthropic caches blocks
    >= 1024 tokens for 5 minutes; cache hits cost ~10% of normal input price.

LangSmith Tracing:
    Set LANGSMITH_API_KEY, LANGSMITH_PROJECT, LANGSMITH_TRACING=true to enable.
    The LangSmith SDK automatically traces LangGraph runs. For raw Anthropic SDK
    calls, use langsmith.wrappers.wrap_anthropic() or @traceable decorator.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from anthropic import Anthropic
from anthropic.types import TextBlock, ToolUseBlock
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import ApiUsageLog
from src.services.vendor_cost_attribution import resolve_pause_target
from src.services.vendor_cost_pause_service import get_active_pause

logger = logging.getLogger(__name__)

# Token cost in USD per 1M tokens (as of Claude 4.x pricing)
_COST_TABLE: dict[str, dict[str, float]] = {
    "haiku":  {"input": 0.80,  "output": 4.00},
    "sonnet": {"input": 3.00,  "output": 15.00},
    "opus":   {"input": 15.00, "output": 75.00},
}

# Task → model tier mapping. Add new task types here as features are built.
_TASK_ROUTING: dict[str, str] = {
    # Haiku — fast, cheap, good enough
    "sms_copy":           "haiku",
    "classification":     "haiku",
    "command_parsing":    "haiku",
    "batch_summarization":"haiku",
    "address_matching":   "haiku",
    "keyword_extraction": "haiku",
    "keyword_variations": "haiku",   # Task 5.1 content-loop Seed Keyword variations
    # Sonnet — contextual reasoning
    "conversational_close":  "sonnet",
    "complex_reasoning":     "sonnet",
    "lead_analysis":         "sonnet",
    "learning_card":         "sonnet",
    "retention_copy":           "sonnet",
    "email_copy":               "sonnet",
    "closer_call_tagging":      "sonnet",   # Closer Cockpit (S1b) transcript tagging
    "loss_autopsy":             "sonnet",   # Phase 3 A1: Loss Autopsy Engine
    "referral_milestone_sms":   "haiku",
    "referral_milestone_email": "sonnet",
    "buyer_entity_match":       "haiku",   # HUNTER-01 (H2.4) — ambiguous-pair tie-break, single classification call
    # Cora — cold-drafting and reply workflows (PR 180 / QUALITY-v2.2 Q2)
    # cora_reply_classify is intentionally haiku: classification only, §1.1.5.
    "cora_outreach_draft": "sonnet",
    "cora_pre_call_brief": "sonnet",
    "cora_reply_classify": "haiku",
    "cora_reply_compose":  "sonnet",
    # Concierge Chat
    "chat_response":  "haiku",   # MD-grounded FAQ reply — Haiku is plenty
    # Command Center — Josh's pipeline chatbot
    "cora_cc_guard":  "haiku",   # intent classification / injection detection
    "cora_cc_query":  "sonnet",  # agentic loop + final answer synthesis
    "cora_cc_compact": "haiku",  # history compaction summary
    # FA Max thread fallback responder (WP-T2-12) — intent classify, Haiku is sufficient
    "fa_max_thread_fallback": "haiku",
    # Opus — explicit override, edge cases only
    "edge_case": "opus",
}


def call_claude(
    task_type: str,
    messages: list[dict],
    system: Optional[str] = None,
    cache_system: bool = False,
    max_tokens: int = 1024,
    subscriber_id: Optional[int] = None,
    graph_name: Optional[str] = None,
    pause_target: Optional[str] = None,
    db: Optional[Session] = None,
    force_tier: Optional[str] = None,
) -> str:
    """
    Route a Claude call to the appropriate model and return the text response.

    Args:
        task_type:      Key from _TASK_ROUTING. Determines Haiku/Sonnet/Opus.
        messages:       Anthropic messages list (role/content dicts).
        system:         Optional system prompt string.
        cache_system:   If True, attach cache_control to the system prompt block
                        so Anthropic caches it across repeated calls (>= 1024 tokens).
        max_tokens:     Max output tokens. Default 1024.
        subscriber_id:  FK to subscribers.id — stored in api_usage_logs for cost attribution.
        db:             SQLAlchemy session. If None, cost is logged but not persisted.
        force_tier:     Override routing — "haiku", "sonnet", or "opus".

    Returns:
        The text content of the first response block.

    Raises:
        anthropic.APIError on API failure (caller decides retry behaviour).
    """
    model_tier = force_tier or _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = _build_client()

    kwargs: dict = {
        "model": model_id,
        "max_tokens": max_tokens,
        "messages": messages,
    }

    if system:
        if cache_system:
            kwargs["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            kwargs["system"] = system

    logger.debug("claude_router: task=%s model=%s", task_type, model_tier)

    # ── Vendor cost pause check (Phase 2) ────────────────────────────────
    resolved_target = pause_target or resolve_pause_target(graph_name=graph_name, task_type=task_type)
    active_pause = get_active_pause(db, "claude", resolved_target) if db and resolved_target else None
    if active_pause:
        logger.warning("claude_router: blocked task=%s pause_target=%s reason=%s",
                       task_type, resolved_target, active_pause.reason)
        _log_usage(None, model_tier, task_type, subscriber_id, db,
                   graph_name=graph_name, pause_target=resolved_target,
                   blocked_by_pause=True, block_reason=f"pause: {active_pause.reason}")
        return f"[BLOCKED] Vendor cost pause active for '{resolved_target}': {active_pause.reason}"

    response = client.messages.create(**kwargs)

    text = _extract_text(response)
    _log_usage(response, model_tier, task_type, subscriber_id, db,
               graph_name=graph_name, pause_target=resolved_target)

    return text


def call_claude_with_usage(
    task_type: str,
    messages: list[dict],
    system: Optional[str] = None,
    cache_system: bool = False,
    max_tokens: int = 1024,
    subscriber_id: Optional[int] = None,
    graph_name: Optional[str] = None,
    pause_target: Optional[str] = None,
    db: Optional[Session] = None,
    force_tier: Optional[str] = None,
    tools: Optional[list[dict]] = None,
    tool_choice: Optional[dict] = None,
    temperature: Optional[float] = None,
) -> dict:
    """
    Same as call_claude() but returns a dict that includes token counts and
    cost alongside the text. Used by Lifecycle graphs that need to track
    per-decision budget consumption.

    Pass `tools` + `tool_choice` to use Anthropic tool use for structured output.
    When a tool_use block is returned, result['tool_input'] contains the parsed
    dict and result['text'] is empty.

    Returns:
        {
            'text':         str,
            'tool_input':   dict | None,
            'model':        'haiku' | 'sonnet' | 'opus',
            'input_tokens': int,
            'output_tokens': int,
            'cost_usd':     float,
        }
    """
    model_tier = force_tier or _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = _build_client()

    kwargs: dict = {
        "model": model_id,
        "max_tokens": max_tokens,
        "messages": messages,
    }

    if system:
        if cache_system:
            kwargs["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            kwargs["system"] = system

    if temperature is not None:
        kwargs["temperature"] = temperature

    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = tool_choice or (
            {"type": "tool", "name": tools[0]["name"]} if len(tools) == 1 else {"type": "auto"}
        )

    # ── Vendor cost pause check (Phase 2) ────────────────────────────────
    resolved_target = pause_target or resolve_pause_target(graph_name=graph_name, task_type=task_type)
    active_pause = get_active_pause(db, "claude", resolved_target) if db and resolved_target else None
    if active_pause:
        logger.warning("claude_router: blocked task=%s pause_target=%s reason=%s",
                       task_type, resolved_target, active_pause.reason)
        _log_usage(None, model_tier, task_type, subscriber_id, db,
                   graph_name=graph_name, pause_target=resolved_target,
                   blocked_by_pause=True, block_reason=f"pause: {active_pause.reason}")
        return {
            "text": f"[BLOCKED] Vendor cost pause active for '{resolved_target}': {active_pause.reason}",
            "tool_input": None,
            "model": model_tier,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
        }

    response = client.messages.create(**kwargs)

    text = _extract_text(response)
    tool_input = _extract_tool_input(response)
    _log_usage(response, model_tier, task_type, subscriber_id, db,
               graph_name=graph_name, pause_target=resolved_target)

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    costs = _COST_TABLE.get(model_tier, _COST_TABLE["sonnet"])
    cost_usd = (input_tokens * costs["input"] + output_tokens * costs["output"]) / 1_000_000

    return {
        "text": text,
        "tool_input": tool_input,
        "model": model_tier,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
    }


def call_claude_for_tool_loop(
    task_type: str,
    messages: list[dict],
    system: str,
    tools: list[dict],
    max_tokens: int = 1024,
    graph_name: Optional[str] = None,
    db: Optional[Session] = None,
    force_tier: Optional[str] = None,
) -> dict:
    """
    Single Claude API call for a multi-turn agentic tool-use loop.

    Unlike call_claude_with_usage(), this returns the full response needed to
    build the next turn's messages: stop_reason, all tool_calls with their ids,
    and assistant_content as plain serializable dicts.  The caller maintains
    the messages list, appends assistant_content after each call, then appends
    tool_result blocks before the next call.

    Returns:
        {
            "stop_reason":        "tool_use" | "end_turn" | "max_tokens",
            "text":               str,          # the answer text when stop_reason == "end_turn"
            "tool_calls":         list[dict],   # [{"id", "name", "input"}, ...] when stop_reason == "tool_use"
            "assistant_content":  list[dict],   # append to messages as {"role": "assistant", "content": this}
            "input_tokens":       int,
            "output_tokens":      int,
            "cost_usd":           float,
            "model":              str,
        }

    Raises:
        anthropic.APIError on API failure — callers must catch and handle.
    """
    model_tier = force_tier or _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)
    client = _build_client()

    kwargs: dict = {
        "model": model_id,
        "max_tokens": max_tokens,
        "messages": messages,
        "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        "tools": tools,
        "tool_choice": {"type": "auto"},
    }

    resolved_target = resolve_pause_target(graph_name=graph_name, task_type=task_type)
    active_pause = get_active_pause(db, "claude", resolved_target) if db and resolved_target else None
    if active_pause:
        logger.warning(
            "claude_router: blocked task=%s pause_target=%s reason=%s",
            task_type, resolved_target, active_pause.reason,
        )
        _log_usage(None, model_tier, task_type, None, db, graph_name=graph_name,
                   pause_target=resolved_target, blocked_by_pause=True,
                   block_reason=f"pause: {active_pause.reason}")
        return {
            "stop_reason": "end_turn",
            "text": f"[BLOCKED] Vendor cost pause active for '{resolved_target}'.",
            "tool_calls": [],
            "assistant_content": [{"type": "text", "text": f"[BLOCKED] Vendor cost pause active."}],
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
            "model": model_tier,
        }

    response = client.messages.create(**kwargs)

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    costs = _COST_TABLE.get(model_tier, _COST_TABLE["sonnet"])
    cost_usd = (input_tokens * costs["input"] + output_tokens * costs["output"]) / 1_000_000

    _log_usage(response, model_tier, task_type, None, db,
               graph_name=graph_name, pause_target=resolved_target)

    stop_reason = getattr(response, "stop_reason", "end_turn") or "end_turn"

    # Convert SDK content blocks to plain serializable dicts — required
    # because these go into LangGraph state (msgpack) and back into
    # messages on the next API call.
    assistant_content: list[dict] = []
    text_parts: list[str] = []
    tool_calls: list[dict] = []

    for block in response.content:
        if isinstance(block, TextBlock):
            assistant_content.append({"type": "text", "text": block.text})
            text_parts.append(block.text)
        elif isinstance(block, ToolUseBlock):
            assistant_content.append({
                "type": "tool_use",
                "id": block.id,
                "name": block.name,
                "input": dict(block.input) if block.input else {},
            })
            tool_calls.append({
                "id": block.id,
                "name": block.name,
                "input": dict(block.input) if block.input else {},
            })

    return {
        "stop_reason": stop_reason,
        "text": "".join(text_parts),
        "tool_calls": tool_calls,
        "assistant_content": assistant_content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
        "model": model_tier,
    }


def call_claude_streaming(
    task_type: str,
    messages: list[dict],
    system: str,
    tools: list[dict],
    max_tokens: int = 1024,
    graph_name: Optional[str] = None,
    db: Optional[Session] = None,
    on_text_chunk=None,  # Callable[[str], None] | None
) -> dict:
    """
    Streaming variant of call_claude_for_tool_loop.

    Identical return shape. When `on_text_chunk` is provided it is called with
    each text delta as Claude streams the response — useful for updating a
    Slack placeholder message progressively.

    During tool-use iterations Claude emits no text so the callback is silent.
    On the final answer iteration the callback fires with each token, letting
    the caller push incremental updates to Slack at their own throttle rate.
    """
    model_tier = _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)
    client = _build_client()

    resolved_target = resolve_pause_target(graph_name=graph_name, task_type=task_type)
    active_pause = get_active_pause(db, "claude", resolved_target) if db and resolved_target else None
    if active_pause:
        logger.warning("claude_router: blocked task=%s pause=%s", task_type, resolved_target)
        _log_usage(None, model_tier, task_type, None, db, graph_name=graph_name,
                   pause_target=resolved_target, blocked_by_pause=True,
                   block_reason=f"pause: {active_pause.reason}")
        blocked_text = f"[BLOCKED] Vendor cost pause active for '{resolved_target}'."
        return {
            "stop_reason": "end_turn", "text": blocked_text,
            "tool_calls": [], "assistant_content": [{"type": "text", "text": blocked_text}],
            "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0, "model": model_tier,
        }

    kwargs: dict = {
        "model": model_id,
        "max_tokens": max_tokens,
        "messages": messages,
        "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        "tools": tools,
        "tool_choice": {"type": "auto"},
    }

    # Run the stream in a background thread so LangGraph's node introspection
    # never sees the streaming context manager (it mistakes it for a generator node).
    import threading as _threading

    _STREAM_TIMEOUT_S = 90  # hard ceiling per streaming call

    _result: list = []
    _exc: list = []

    def _stream_worker():
        try:
            with client.messages.stream(timeout=_STREAM_TIMEOUT_S, **kwargs) as stream:
                if on_text_chunk is not None:
                    for text_delta in stream.text_stream:
                        try:
                            on_text_chunk(text_delta)
                        except Exception:
                            pass
                _result.append(stream.get_final_message())
        except Exception as e:
            _exc.append(e)

    t = _threading.Thread(target=_stream_worker, daemon=True)
    t.start()
    t.join(timeout=_STREAM_TIMEOUT_S + 5)

    if t.is_alive():
        _exc.append(TimeoutError(f"Claude streaming call exceeded {_STREAM_TIMEOUT_S}s timeout"))

    if _exc:
        # Streaming failed — fall back to a regular blocking call.
        logger.warning("claude_router: streaming failed (%s) — falling back to create()", _exc[0])
        response = client.messages.create(**kwargs)
    else:
        response = _result[0]

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    costs = _COST_TABLE.get(model_tier, _COST_TABLE["sonnet"])
    cost_usd = (input_tokens * costs["input"] + output_tokens * costs["output"]) / 1_000_000

    _log_usage(response, model_tier, task_type, None, db,
               graph_name=graph_name, pause_target=resolved_target)

    stop_reason = getattr(response, "stop_reason", "end_turn") or "end_turn"

    assistant_content: list[dict] = []
    text_parts: list[str] = []
    tool_calls: list[dict] = []

    for block in response.content:
        if isinstance(block, TextBlock):
            assistant_content.append({"type": "text", "text": block.text})
            text_parts.append(block.text)
        elif isinstance(block, ToolUseBlock):
            assistant_content.append({
                "type": "tool_use", "id": block.id,
                "name": block.name,
                "input": dict(block.input) if block.input else {},
            })
            tool_calls.append({
                "id": block.id, "name": block.name,
                "input": dict(block.input) if block.input else {},
            })

    return {
        "stop_reason": stop_reason,
        "text": "".join(text_parts),
        "tool_calls": tool_calls,
        "assistant_content": assistant_content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
        "model": model_tier,
    }


def call_claude_batch(
    task_type: str,
    requests: list[dict],
    db: Optional[Session] = None,
) -> str:
    """
    Submit a batch of requests via the Anthropic Batch API (65-75% cost saving).
    Use for non-real-time tasks: learning card generation, weekly summaries, ad copy.

    Args:
        task_type: Used for routing and cost logging.
        requests:  List of Anthropic batch request dicts (each with custom_id + params).
        db:        Session for logging.

    Returns:
        The batch job ID. Poll with anthropic.beta.messages.batches.retrieve(batch_id).
    """
    model_tier = _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = Anthropic(api_key=settings.anthropic_api_key.get_secret_value())

    for req in requests:
        req.setdefault("params", {})["model"] = model_id

    batch = client.beta.messages.batches.create(requests=requests)
    logger.info("claude_router: batch submitted id=%s task=%s count=%d", batch.id, task_type, len(requests))
    return batch.id


def stream_claude(
    task_type: str,
    messages: list[dict],
    system: Optional[str] = None,
    cache_system: bool = False,
    max_tokens: int = 512,
    subscriber_id: Optional[int] = None,
    db: Optional[Session] = None,
    force_tier: Optional[str] = None,
):
    """
    Streaming variant of call_claude(). Yields text chunks as they arrive.

    Does NOT change call_claude() behaviour. Uses the same model routing and
    logs one api_usage_logs row after the stream completes.

    Usage:
        for chunk in stream_claude("chat_response", messages, system=sys, cache_system=True):
            yield chunk  # SSE chunk to client
    """
    import time as _time
    model_tier = force_tier or _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = _build_client()

    kwargs: dict = {
        "model": model_id,
        "max_tokens": max_tokens,
        "messages": messages,
    }

    if system:
        if cache_system:
            kwargs["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            kwargs["system"] = system

    logger.debug("claude_router: stream task=%s model=%s", task_type, model_tier)

    t0 = _time.monotonic()
    with client.messages.stream(**kwargs) as stream_ctx:
        for text in stream_ctx.text_stream:
            yield text

    # Log usage after stream completes
    final = stream_ctx.get_final_message()
    _log_usage(final, model_tier, task_type, subscriber_id, db)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _build_client() -> Anthropic:
    """
    Build the Anthropic client, wrapping it with LangSmith's wrap_anthropic when
    tracing is enabled so every Claude call emits an LLM run to LangSmith.

    Wrapping is best-effort: if the langsmith package or wrapper is unavailable,
    or tracing is off, we return a plain client unchanged (no behaviour change).
    """
    client = Anthropic(api_key=settings.anthropic_api_key.get_secret_value())
    try:
        from src.agents.observability.langsmith import configure_tracing
        # configure_tracing() bridges .env -> os.environ (idempotent) AND returns
        # True when tracing is enabled. The bridge is required: wrap_anthropic
        # only exports runs when LANGSMITH_TRACING is in os.environ, which a bare
        # script/API/cron entrypoint won't have unless we set it here.
        if configure_tracing():
            from langsmith.wrappers import wrap_anthropic
            client = wrap_anthropic(client)
    except Exception as exc:  # pragma: no cover — tracing must never break sends
        logger.debug("claude_router: LangSmith wrap skipped: %s", exc)
    return client


def _model_id(tier: str) -> str:
    mapping = {
        "haiku":  settings.claude_haiku_model,
        "sonnet": settings.claude_sonnet_model,
        "opus":   settings.claude_opus_model,
    }
    return mapping.get(tier, settings.claude_sonnet_model)


def _extract_text(response) -> str:
    for block in response.content:
        if isinstance(block, TextBlock):
            return block.text
    return ""


def _extract_tool_input(response) -> Optional[dict]:
    for block in response.content:
        if isinstance(block, ToolUseBlock):
            return block.input
    return None


def _log_usage(
    response,
    model_tier: str,
    task_type: str,
    subscriber_id: Optional[int],
    db: Optional[Session],
    graph_name: Optional[str] = None,
    pause_target: Optional[str] = None,
    blocked_by_pause: bool = False,
    block_reason: Optional[str] = None,
) -> None:
    if blocked_by_pause:
        # Blocked calls have no response -- log the skip with zero tokens/cost
        input_tokens = 0
        output_tokens = 0
        cost_usd = 0.0
    else:
        usage = getattr(response, "usage", None)
        if not usage:
            return
        input_tokens = getattr(usage, "input_tokens", 0)
        output_tokens = getattr(usage, "output_tokens", 0)
        costs = _COST_TABLE.get(model_tier, _COST_TABLE["sonnet"])
        cost_usd = (input_tokens * costs["input"] + output_tokens * costs["output"]) / 1_000_000

        logger.debug(
            "claude_router: model=%s in=%d out=%d cost=$%.6f",
            model_tier, input_tokens, output_tokens, cost_usd,
        )

    if db is None:
        return

    try:
        db.add(ApiUsageLog(
            service="claude",
            model=model_tier,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
            task_type=task_type,
            graph_name=graph_name,
            pause_target=pause_target,
            blocked_by_pause=blocked_by_pause,
            block_reason=block_reason,
            subscriber_id=subscriber_id,
            created_at=datetime.now(timezone.utc),
        ))
        db.flush()
    except Exception as exc:
        logger.warning("claude_router: failed to log usage: %s", exc)
