"""
Cora / LangGraph graph for the Quora organic-answer workflow.

Two logical nodes:
    classify_question  — decides if a question is worth answering and how
    generate_answer    — drafts the answer (only when classification approves)

Entry point:
    run_quora_channel(candidate_dict, matched_keyword, generate_answer=False)

The graph does NOT auto-post anything. Output is always a draft for human review.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents.prompts.loader import load_prompt, render
from src.utils.quora_attribution import campaign_slug

logger = logging.getLogger(__name__)

GRAPH_NAME = "quora_channel"
_MIN_PRIORITY_FOR_ANSWER = 70

# ---------------------------------------------------------------------------
# Tool definitions — force structured output, no JSON parsing needed
# ---------------------------------------------------------------------------

_CLASSIFY_TOOL = {
    "name": "submit_classification",
    "description": "Submit the classification result for a Quora question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "recommended_action": {
                "type": "string",
                "enum": ["generate_answer", "skip"],
            },
            "intent_lane": {"type": "string"},
            "priority_score": {"type": "integer"},
            "risk_level": {"type": "string", "enum": ["low", "medium", "high"]},
            "is_relevant": {"type": "boolean"},
            "is_answerable": {"type": "boolean"},
            "reasoning": {"type": "string"},
        },
        "required": ["recommended_action", "intent_lane", "priority_score",
                     "risk_level", "is_relevant", "is_answerable"],
    },
}

_ANSWER_TOOL = {
    "name": "submit_answer_draft",
    "description": "Submit the drafted answer for the Quora question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "qid":             {"type": "integer"},
            "answer_status":   {"type": "string", "enum": ["draft_generated"]},
            "answer_markdown": {"type": "string"},
        },
        "required": ["qid", "answer_status", "answer_markdown"],
    },
}


class QuoraChannelState(TypedDict, total=False):
    # ── Inputs ───────────────────────────────────────────────────────────────
    candidate: dict
    matched_keyword: str
    generate_answer_drafts: bool

    # ── Intermediate ─────────────────────────────────────────────────────────
    cora_classification: Optional[dict]

    # ── Outputs ───────────────────────────────────────────────────────────────
    cora_answer_draft: Optional[dict]
    tokens_used: int
    cost_usd: float
    terminal_status: str
    failure_reason: str


# ---------------------------------------------------------------------------
# Node: classify_question
# ---------------------------------------------------------------------------

def _node_classify_question(state: QuoraChannelState) -> Dict[str, Any]:
    candidate = state.get("candidate") or {}
    keyword   = state.get("matched_keyword") or ""

    # Strip raw_metadata — never send full GQL payload to Claude
    compact = {k: v for k, v in candidate.items() if k != "raw_metadata"}

    context = {
        "candidate_json":  json.dumps(compact, default=str, indent=2),
        "matched_keyword": keyword,
    }

    try:
        data   = load_prompt(GRAPH_NAME, "classify")
        system = render(data.get("system", ""), context)
        user   = render(data.get("user", ""), context)

        from src.services.claude_router import call_claude_with_usage
        result = call_claude_with_usage(
            task_type="quora_classify",
            messages=[{"role": "user", "content": user}],
            system=system,
            max_tokens=512,
            graph_name=GRAPH_NAME,
            tools=[_CLASSIFY_TOOL],
        )

        classification = result.get("tool_input")
        return {
            "cora_classification": classification,
            "tokens_used": (result.get("input_tokens", 0) or 0) + (result.get("output_tokens", 0) or 0),
            "cost_usd":    result.get("cost_usd", 0.0) or 0.0,
        }

    except Exception as exc:
        logger.error("[quora_channel] classify_question failed: %s", exc)
        return {
            "cora_classification": None,
            "terminal_status":     "classify_failed",
            "failure_reason":      str(exc),
        }


# ---------------------------------------------------------------------------
# Node: generate_answer
# ---------------------------------------------------------------------------

def _node_generate_answer(state: QuoraChannelState) -> Dict[str, Any]:
    if state.get("terminal_status"):
        return {}

    classification = state.get("cora_classification") or {}
    candidate      = state.get("candidate") or {}
    keyword        = state.get("matched_keyword") or ""

    if not (
        classification.get("is_relevant")
        and classification.get("is_answerable")
        and classification.get("recommended_action") == "generate_answer"
        and int(classification.get("priority_score", 0)) >= _MIN_PRIORITY_FOR_ANSWER
    ):
        return {"cora_answer_draft": None}

    compact = {k: v for k, v in candidate.items() if k != "raw_metadata"}
    qid     = candidate.get("qid") or ""

    # Build a URL-safe campaign slug from keyword (shared with the tuning worker)
    utm_campaign = campaign_slug(keyword)

    context = {
        "candidate_json":    json.dumps(compact, default=str, indent=2),
        "classification_json": json.dumps(classification, default=str, indent=2),
        "matched_keyword":   keyword,
        "utm_campaign":      utm_campaign,
        "qid":               str(qid),
    }

    try:
        data   = load_prompt(GRAPH_NAME, "answer")
        system = render(data.get("system", ""), context)
        user   = render(data.get("user", ""), context)

        from src.services.claude_router import call_claude_with_usage
        result = call_claude_with_usage(
            task_type="quora_answer",
            messages=[{"role": "user", "content": user}],
            system=system,
            max_tokens=2048,
            graph_name=GRAPH_NAME,
            tools=[_ANSWER_TOOL],
        )

        answer_draft = result.get("tool_input")
        prior_tokens = int(state.get("tokens_used", 0) or 0)
        prior_cost   = float(state.get("cost_usd", 0.0) or 0.0)

        return {
            "cora_answer_draft": answer_draft,
            "tokens_used":  prior_tokens + (result.get("input_tokens", 0) or 0) + (result.get("output_tokens", 0) or 0),
            "cost_usd":     prior_cost + (result.get("cost_usd", 0.0) or 0.0),
        }

    except Exception as exc:
        logger.error("[quora_channel] generate_answer failed: %s", exc)
        return {
            "cora_answer_draft": None,
            "terminal_status":   "answer_failed",
            "failure_reason":    str(exc),
        }


# ---------------------------------------------------------------------------
# Conditional routing
# ---------------------------------------------------------------------------

def _should_generate(state: QuoraChannelState) -> str:
    if state.get("terminal_status"):
        return END

    if not state.get("generate_answer_drafts"):
        return END

    classification = state.get("cora_classification") or {}
    if (
        classification.get("is_relevant")
        and classification.get("is_answerable")
        and classification.get("recommended_action") == "generate_answer"
        and int(classification.get("priority_score", 0)) >= _MIN_PRIORITY_FOR_ANSWER
    ):
        return "generate_answer"

    return END


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------

def build_quora_channel_graph() -> StateGraph:
    g = StateGraph(QuoraChannelState)
    g.add_node("classify_question", _node_classify_question)
    g.add_node("generate_answer",   _node_generate_answer)

    g.add_edge(START, "classify_question")
    g.add_conditional_edges("classify_question", _should_generate)
    g.add_edge("generate_answer", END)
    return g


def run_quora_channel(
    candidate: dict,
    matched_keyword: str,
    generate_answer_drafts: bool = False,
) -> Dict[str, Any]:
    """
    Run the Quora channel graph for a single candidate question.

    candidate must be a plain dict (QuoraResult fields, no raw_metadata).
    Returns the final state dict with cora_classification and optionally cora_answer_draft.
    """
    graph = build_quora_channel_graph().compile()
    final = graph.invoke({
        "candidate":              candidate,
        "matched_keyword":        matched_keyword,
        "generate_answer_drafts": generate_answer_drafts,
    })
    return dict(final)


def run_quora_channel_from_event(
    event_payload: Dict[str, Any],
    subscriber_id: Any,          # always None for this graph — no subscriber target
    decision_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Supervisor-compatible runner.

    Called by the supervisor after receiving a `quora_candidate_classify` event
    from Redis / Postgres. Wraps run_quora_channel and logs the outcome to
    agent_decisions.
    """
    import uuid as _uuid
    decision_id = decision_id or str(_uuid.uuid4())

    candidate      = event_payload.get("candidate") or {}
    matched_keyword = event_payload.get("matched_keyword") or ""
    generate_drafts = bool(event_payload.get("generate_answer_drafts", False))

    result = run_quora_channel(
        candidate=candidate,
        matched_keyword=matched_keyword,
        generate_answer_drafts=generate_drafts,
    )

    summary = {
        "qid":                 candidate.get("qid"),
        "matched_keyword":     matched_keyword,
        "cora_classification": result.get("cora_classification"),
        "cora_answer_draft":   result.get("cora_answer_draft"),
        "failure_reason":      result.get("failure_reason"),
    }

    write_confirmed = False
    try:
        from src.agents.tools.write_tools import log_decision
        log_decision(
            decision_id=decision_id,
            graph_name=GRAPH_NAME,
            subscriber_id=None,
            event_type="quora_candidate_classify",
            terminal_status=result.get("terminal_status") or "completed",
            tokens_used=int(result.get("tokens_used", 0) or 0),
            cost_usd=float(result.get("cost_usd", 0.0) or 0.0),
            summary=summary,
        )
        write_confirmed = True
    except Exception as exc:
        logger.warning("[quora_channel] log_decision failed: %s", exc)

    # Dispatch Redis event keyed by decision_id so the miner can collect results
    # without polling the DB. Fires whether or not the DB write succeeded — the
    # miner needs the result either way; write_confirmed lets it know DB is queryable.
    try:
        from src.core.redis_client import get_redis, redis_available
        if redis_available():
            get_redis().publish("cora:quora:results", json.dumps({
                "decision_id":         decision_id,
                "qid":                 candidate.get("qid"),
                "terminal_status":     result.get("terminal_status") or "completed",
                "write_confirmed":     write_confirmed,
                "cora_classification": result.get("cora_classification"),
                "cora_answer_draft":   result.get("cora_answer_draft"),
            }, default=str))
    except Exception as exc:
        logger.warning("[quora_channel] Redis dispatch failed: %s", exc)

    return result


