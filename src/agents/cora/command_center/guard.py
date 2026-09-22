"""
Command Center guard node — two-phase intent check before the agentic loop.

Phase 1 (Python, ~0ms):
    Fast pattern scan for obvious PII field names and injection markers.
    Blocks immediately without spending any tokens.

Phase 2 (Haiku, ~200ms):
    Structured intent classification.  Blocks non-pipeline questions and
    injection attempts that slipped through phase 1.

Valid intents proceed to the agentic loop.  All blocked intents return a
safe, user-facing message and set state["blocked"] = True so the graph
short-circuits to the emit node.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ── Phase 1 patterns ──────────────────────────────────────────────────────────

_PII_PATTERNS: re.Pattern = re.compile(
    r'\b(social.?security|ssn|credit.?score|fico|bank.?statement|'
    r'tax.?return|w-?2|1099|income.?verification|debt.?to.?income|dti)\b',
    re.IGNORECASE,
)

_NUMERIC_FOLLOWUP_PATTERNS: re.Pattern = re.compile(
    r'^[\s\d\$\%\,\.\-\+kKmMbB\/\(\)]*'
    r'(arv|ltv|ltc|purchase|price|rehab|repair|loan|amount|down|equity|'
    r'value|cost|sqft|sq\s*ft|acres?|beds?|baths?|units?|floors?|year|built|'
    r'single.family|sfr|duplex|triplex|fourplex|condo|state|county|zip|fl|tx|ca|ga|az|nc|sc|'
    r'yes|no|sure|ok|okay|correct|right|confirmed?)?'
    r'[\s\d\$\%\,\.\-\+kKmMbB\/\(\)]*$',
    re.IGNORECASE,
)

_GREETING_PATTERNS: re.Pattern = re.compile(
    r'^\s*(<@[A-Z0-9]+>\s*)?'
    r'(hi+|hy|hey+|heyy+|hello+|helo+|howdy|yo+|sup|'
    r'good\s+(morning|afternoon|evening|day)|'
    r'what\'?s\s+up|greetings|hiya|hola|namaste|salut|ciao|'
    r'hows?\s+it\s+going|how\s+are\s+you|how\'?s\s+everything|'
    r'morning|evening|afternoon|'       # bare time-of-day greetings
    r'heya|hii+|hiii+|hai|hellow)'     # common typos / alt spellings
    r'\W*\s*$',
    re.IGNORECASE,
)

_IDENTITY_PATTERNS: re.Pattern = re.compile(
    r'^\s*(<@[A-Z0-9]+>\s*)?'
    r'(who\s+are\s+you|what\s+are\s+you|what\s+(can|do)\s+you\s+do|'
    r'what\s+is\s+this|what\'?s\s+this|tell\s+me\s+about\s+yourself|'
    r'introduce\s+yourself|what\s+is\s+cora|who\s+is\s+cora|'
    r'help|\/help|commands?|capabilities?)'
    r'\W*\s*$',
    re.IGNORECASE,
)

_IDENTITY_REPLY = (
    "I'm *Cora* — your pipeline intelligence assistant. Here's what I can do:\n\n"
    "• 📊 *Pipeline health* — outreach counts, reply rates, conversion by stage\n"
    "• 🏠 *Deal eligibility* — check if a deal fits Backflip's lending box\n"
    "• 🔢 *Backward math* — how many outreaches to hit a deal target\n"
    "• 🐋 *Whale targets* — look up buyer entities and their activity\n"
    "• 📈 *Scoreboard* — recent outreach volume and reply rate\n\n"
    "What do you need?"
)

_INJECTION_PATTERNS: re.Pattern = re.compile(
    r'(ignore\s+(previous|all|above|prior)\s+(instructions?|prompts?|rules?)|'
    r'system\s+prompt|jailbreak|pretend\s+(you\s+are|to\s+be)|'
    r'forget\s+(everything|your|the)|act\s+as\s+(if|a|an)\s+(?!account))',
    re.IGNORECASE,
)

_PRICING_PATTERNS: re.Pattern = re.compile(
    r'\b(interest\s+rate|apr|annual\s+percentage\s+rate|origination\s+fee|'
    r'loan\s+term|rate\s+sheet|points?\s+(on|charged)|lender\s+fee|'
    r'closing\s+cost|prepayment\s+penalty|draw\s+fee|extension\s+fee)\b',
    re.IGNORECASE,
)

# ── Phase 2 ───────────────────────────────────────────────────────────────────

_GUARD_SYSTEM = (
    "You are a security classifier for a pipeline intelligence chatbot used by a "
    "hard-money lending Account Executive. Classify the user's question into exactly "
    "one intent. The chatbot can only answer questions about: pipeline health, "
    "outreach counts, reply rates, whale targets, deal eligibility evaluation, "
    "backward math (how many outreaches to hit a deal target), and program rules. "
    "It cannot discuss interest rates, APR, fees, loan terms, or borrower financial "
    "data (credit, income, bank statements, tax returns, SSNs)."
)

_GUARD_TOOL: Dict[str, Any] = {
    "name": "classify_intent",
    "description": "Classify the intent of the user's question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "enum": [
                    "valid_pipeline_query",
                    "greeting",
                    "pricing_question",
                    "borrower_financial",
                    "injection_attempt",
                    "out_of_scope",
                ],
            },
            "reason": {
                "type": "string",
                "description": "One-line reason for the classification.",
            },
        },
        "required": ["intent", "reason"],
    },
}

_GREETING_REPLIES = [
    "Hey! What can I pull up for you today — pipeline numbers, a deal check, or backward math?",
    "Hi there! Ready when you are. Pipeline health, deal eligibility, or something else?",
    "Hello! What do you need — scoreboard, backward math, or a deal eval?",
    "Hey, good to hear from you. What are we looking at today?",
]

_BLOCK_MESSAGES: Dict[str, str] = {
    "greeting": (
        "Hey! What can I pull up for you — pipeline numbers, a deal check, or backward math?"
    ),
    "pricing_question": (
        "I can't discuss interest rates, fees, or loan terms — that's Backflip's "
        "domain. I can tell you whether a deal fits Backflip's lending box, or "
        "help with pipeline counts and backward math."
    ),
    "borrower_financial": (
        "I don't handle borrower financial data (credit scores, income, bank "
        "statements, SSNs). Ask Josh to refer that directly to Backflip's portal."
    ),
    "injection_attempt": (
        "That request can't be processed."
    ),
    "out_of_scope": (
        "I can only answer questions about the Forced Action pipeline: outreach "
        "counts, reply rates, whale targets, deal eligibility, and backward math. "
        "What would you like to know?"
    ),
}


def run_guard(question: str, db: Optional[Session], history: Optional[list] = None, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns {"blocked": bool, "block_reason": str | None, "intent": str | None}.

    Never raises. A classifier failure blocks (fail-closed): an unclassified
    question could be an injection attempt or a request for borrower financial
    data, so an outage must not become a way past the guard. Greetings and the
    Phase 1 pattern rules answer without the classifier, so they keep working
    while it is down.
    """
    # Phase 1 — pattern scan

    # Fast-path: numeric/financial follow-up with no injection markers and prior history.
    # Saves the ~2s Haiku call for messages like "purchase price $280k, rehab $45k, ARV $420k".
    if (
        history
        and len(question) < 200
        and _NUMERIC_FOLLOWUP_PATTERNS.fullmatch(question)
        and not _PII_PATTERNS.search(question)
        and not _INJECTION_PATTERNS.search(question)
    ):
        return {"blocked": False, "block_reason": None, "intent": "valid_pipeline_query", "cost_usd": 0.0}

    if _GREETING_PATTERNS.match(question):
        import hashlib
        import random
        rng = random.Random(hashlib.md5(question.encode()).hexdigest())
        return {
            "blocked": True,
            "block_reason": "greeting",
            "block_message": rng.choice(_GREETING_REPLIES),
            "intent": "greeting",
        }

    if _IDENTITY_PATTERNS.match(question):
        return {
            "blocked": True,
            "block_reason": "greeting",
            "block_message": _IDENTITY_REPLY,
            "intent": "greeting",
        }

    if _PII_PATTERNS.search(question):
        return {
            "blocked": True,
            "block_reason": "borrower_financial",
            "block_message": _BLOCK_MESSAGES["borrower_financial"],
            "intent": "borrower_financial",
        }
    if _PRICING_PATTERNS.search(question):
        return {
            "blocked": True,
            "block_reason": "pricing_question",
            "block_message": _BLOCK_MESSAGES["pricing_question"],
            "intent": "pricing_question",
        }
    if _INJECTION_PATTERNS.search(question):
        return {
            "blocked": True,
            "block_reason": "injection_attempt",
            "block_message": _BLOCK_MESSAGES["injection_attempt"],
            "intent": "injection_attempt",
        }

    # Phase 2 — haiku classification
    # Check Redis cache first — repeated edge-case questions (e.g. a greeting
    # that slipped past Phase 1) shouldn't pay for a second Haiku call.
    import hashlib as _hashlib
    _norm_q = re.sub(r"\s+", " ", question.strip().lower())
    _session_scope = session_id or "global"
    _cache_key = f"cc:guard_intent:{_session_scope}:{_hashlib.md5(_norm_q.encode()).hexdigest()}"
    _cached_intent: Optional[str] = None
    try:
        from src.core.redis_client import get_redis, redis_available
        if redis_available():
            _cached_intent = get_redis().get(_cache_key)
    except Exception:
        pass

    if _cached_intent:
        logger.info("guard.cache: hit intent=%r question=%r", _cached_intent, question[:60])
        intent = _cached_intent
        guard_cost = 0.0
    else:
        # Include up to the last 2 prior turns so the classifier can detect follow-ups.
        prior = []
        if history:
            for msg in history[-2:]:
                role = msg.get("role", "")
                content = msg.get("content", "")
                if role in ("user", "assistant") and content:
                    text_content = (
                        content if isinstance(content, str)
                        else next((b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"), "")
                    )
                    if text_content:
                        prior.append({"role": role, "content": text_content[:300]})

        messages_for_guard = prior + [{"role": "user", "content": question}]

        from src.services.claude_router import call_claude_with_usage

        try:
            result = call_claude_with_usage(
                task_type="cora_cc_guard",
                messages=messages_for_guard,
                system=_GUARD_SYSTEM,
                max_tokens=120,
                graph_name="cora_command_center",
                db=db,
                tools=[_GUARD_TOOL],
                tool_choice={"type": "tool", "name": "classify_intent"},
            )
        except Exception as exc:
            logger.error("guard.classify: Claude call failed (%s) — blocking request (fail-closed)", exc)
            return {
                "blocked": True,
                "block_reason": "classifier_unavailable",
                "block_message": "I'm having trouble processing that right now. Please try again in a moment.",
                "intent": "unknown",
                "cost_usd": 0.0,
            }

        tool_input = result.get("tool_input") or {}
        intent = tool_input.get("intent", "valid_pipeline_query")
        guard_cost = float(result.get("cost_usd") or 0)

        # Cache the classification for 5 minutes — guard intent is stateless.
        try:
            from src.core.redis_client import get_redis, redis_available
            if redis_available():
                get_redis().set(_cache_key, intent, ex=300)
        except Exception:
            pass

    if intent == "valid_pipeline_query":
        return {"blocked": False, "block_reason": None, "intent": intent, "cost_usd": guard_cost}

    logger.info("guard: blocked intent=%r question=%r", intent, question[:60])
    return {
        "blocked": True,
        "block_reason": intent,
        "block_message": _BLOCK_MESSAGES.get(intent, _BLOCK_MESSAGES["out_of_scope"]),
        "intent": intent,
        "cost_usd": guard_cost,
    }


def _make_node_guard():
    def _node_guard(state: Dict[str, Any]) -> Dict[str, Any]:
        question = state.get("question", "").strip()
        db = state.get("_db")

        # Strip Slack @-mention prefix before any processing
        question = re.sub(r'^<@[A-Z0-9]+>\s*', '', question).strip()

        if not question:
            return {
                "blocked": True,
                "block_reason": "empty_question",
                "block_message": "Please ask a question.",
                "terminal_status": "completed",
            }

        # Truncate very long inputs before sending anywhere
        if len(question) > 2000:
            question = question[:2000]

        outcome = run_guard(question, db, history=state.get("messages", []), session_id=state.get("session_id"))

        if outcome["blocked"]:
            return {
                "blocked": True,
                "block_reason": outcome["block_reason"],
                "answer": outcome.get("block_message", "Request blocked."),
                "terminal_status": "completed",
                "reject_reason": outcome["block_reason"],
                "_cost_usd": outcome.get("cost_usd", 0.0),
            }

        return {"blocked": False, "block_reason": None, "_cost_usd": outcome.get("cost_usd", 0.0)}

    return _node_guard
