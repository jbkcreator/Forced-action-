"""
Inbound reply classifier for the FA Max Portal Concierge (WP-T2-4).

One Haiku call returns:
  - intent category
  - KB topic match (if intent == clarifying_question)
  - confidence score

Pricing pre-filter runs before the LLM call — any message that contains a
pricing signal immediately returns classification='pricing_escalate' at
confidence=1.0 without spending a token.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = 0.65

_PRICING_RE = re.compile(
    r"\b(interest\s+rate|apr|annual\s+percentage|origination\s+fee|loan\s+term|"
    r"rate\s+sheet|points?\s+(on|charged)|lender\s+fee|closing\s+cost|"
    r"prepayment\s+penalty|draw\s+fee|extension\s+fee|what('s|s|\s+is)\s+(the\s+)?"
    r"rate|how\s+much\s+(do\s+you\s+charge|is\s+the\s+(fee|rate|cost)))\b",
    re.IGNORECASE,
)

_OPT_OUT_RE = re.compile(
    r"\b(stop|unsubscribe|remove\s+me|opt\s*out|don'?t\s+contact|"
    r"take\s+me\s+off|no\s+more\s+(emails?|messages?|texts?))\b",
    re.IGNORECASE,
)

INTENT_CATEGORIES = (
    "interested",
    "not_interested",
    "clarifying_question",
    "wrong_person",
    "opt_out",
    "out_of_office",
    "referral",
)

KB_TOPIC_KEYS = (
    "prequal_process",
    "documents_needed",
    "eligible_property_loan_types",
    "funding_timeline",
    "referral_relationship",
    "none",
)

_SYSTEM = """\
You are a reply classifier for a hard-money lending referral business. You classify \
inbound emails/messages from prospective borrowers.

Return a JSON object with EXACTLY these fields:
  intent        — one of: interested | not_interested | clarifying_question | \
wrong_person | opt_out | out_of_office | referral
  kb_topic      — one of: prequal_process | documents_needed | \
eligible_property_loan_types | funding_timeline | referral_relationship | none
                  (only populate when intent == clarifying_question; else "none")
  confidence    — float 0.0–1.0, your confidence in the intent classification
  reasoning     — one sentence max, your basis for the classification

KB topic definitions:
  prequal_process              — questions about how pre-qualification works or what to expect
  documents_needed             — questions about what paperwork or docs are required
  eligible_property_loan_types — questions about which property types or loan types qualify
  funding_timeline             — questions about how long funding takes after submission
  referral_relationship        — questions about how the referral or business relationship works

Rules:
  - If the message asks about rate, APR, fees, loan terms, or specific deal numbers, \
return intent="clarifying_question" kb_topic="none" — the caller will escalate it.
  - If the message is clearly automated (out-of-office autoresponder), return \
intent="out_of_office".
  - Return ONLY the JSON object — no markdown, no prose.
"""


@dataclass
class ClassificationResult:
    intent: str
    kb_topic: str
    confidence: float
    reasoning: str
    cost_usd: float = 0.0


def classify_inbound(text: str) -> ClassificationResult:
    """
    Classify an inbound message. Pricing pre-filter and opt-out regex run
    before the LLM call so common cases cost nothing.
    """
    stripped = text.strip()

    # Opt-out regex (regex beats LLM — must be 100% reliable)
    if _OPT_OUT_RE.search(stripped):
        return ClassificationResult(
            intent="opt_out",
            kb_topic="none",
            confidence=1.0,
            reasoning="opt-out signal detected by regex pre-filter",
        )

    # Pricing pre-filter — force escalate, no LLM needed
    if _PRICING_RE.search(stripped):
        return ClassificationResult(
            intent="clarifying_question",
            kb_topic="none",
            confidence=1.0,
            reasoning="pricing signal detected by pre-filter — escalate unconditionally",
        )

    try:
        from src.services.claude_router import call_claude_with_usage

        result = call_claude_with_usage(
            task_type="concierge_classify",
            messages=[{"role": "user", "content": stripped[:2000]}],
            system=_SYSTEM,
            max_tokens=200,
            graph_name="reply_concierge",
            db=None,
        )
        raw = (result.get("text") or "").strip()
        cost = float(result.get("cost_usd") or 0.0)

        parsed = json.loads(raw)
        intent = parsed.get("intent", "").lower()
        kb_topic = parsed.get("kb_topic", "none").lower()
        confidence = float(parsed.get("confidence", 0.0))
        reasoning = str(parsed.get("reasoning", ""))

        if intent not in INTENT_CATEGORIES:
            logger.warning("classifier: unrecognised intent=%r — defaulting to below_threshold", intent)
            intent = "below_threshold"
        if kb_topic not in KB_TOPIC_KEYS:
            kb_topic = "none"

        return ClassificationResult(
            intent=intent,
            kb_topic=kb_topic,
            confidence=confidence,
            reasoning=reasoning,
            cost_usd=cost,
        )

    except Exception as exc:
        logger.error("classifier: LLM call failed (%s) — defaulting to below_threshold", exc)
        return ClassificationResult(
            intent="below_threshold",
            kb_topic="none",
            confidence=0.0,
            reasoning=f"classifier error: {exc}",
        )
