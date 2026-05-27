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
    (e.g. Cora persona prompt, ZIP stats context). Anthropic caches blocks
    >= 1024 tokens for 5 minutes; cache hits cost ~10% of normal input price.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from anthropic import Anthropic
from anthropic.types import TextBlock
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
    # Sonnet — contextual reasoning
    "conversational_close":  "sonnet",
    "complex_reasoning":     "sonnet",
    "lead_analysis":         "sonnet",
    "learning_card":         "sonnet",
    "retention_copy":           "sonnet",
    "email_copy":               "sonnet",
    "referral_milestone_sms":   "haiku",
    "referral_milestone_email": "sonnet",
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

    Returns:
        The text content of the first response block.

    Raises:
        anthropic.APIError on API failure (caller decides retry behaviour).
    """
    model_tier = _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = Anthropic(api_key=settings.anthropic_api_key.get_secret_value())

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
) -> dict:
    """
    Same as call_claude() but returns a dict that includes token counts and
    cost alongside the text. Used by Cora graphs that need to track
    per-decision budget consumption.

    Returns:
        {
            'text':         str,
            'model':        'haiku' | 'sonnet' | 'opus',
            'input_tokens': int,
            'output_tokens': int,
            'cost_usd':     float,
        }
    """
    model_tier = _TASK_ROUTING.get(task_type, "sonnet")
    model_id = _model_id(model_tier)

    client = Anthropic(api_key=settings.anthropic_api_key.get_secret_value())

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
            "model": model_tier,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
        }

    response = client.messages.create(**kwargs)

    text = _extract_text(response)
    _log_usage(response, model_tier, task_type, subscriber_id, db,
               graph_name=graph_name, pause_target=resolved_target)

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    costs = _COST_TABLE.get(model_tier, _COST_TABLE["sonnet"])
    cost_usd = (input_tokens * costs["input"] + output_tokens * costs["output"]) / 1_000_000

    return {
        "text": text,
        "model": model_tier,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
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


# ── Internal helpers ──────────────────────────────────────────────────────────


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
