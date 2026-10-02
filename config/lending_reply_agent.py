"""WP-GL-10 reply agent: what counts as a rate / terms question and as a quoted number (values only).

The GHL Conversation AI never quotes a rate, term, point or fee (playbook golden rule: "Never quote a
rate or a term. Not a range, not a ballpark. Always bridge to a booked call with Josh."). It answers
other questions, offers a booking slot, and hands rate / terms questions to Josh. These lists drive two
checks on our side of the GHL webhook: route the handoff to Slack, and flag an AI reply that quoted a
number. They are word lists for a safety net, not the agent's own instructions (those live in GHL, see
docs/lending/ghl-conversation-ai.md). Josh / counsel should confirm them.
"""
from __future__ import annotations

# Inbound words / phrases that mean the borrower is asking about price or terms (matched as whole
# words on the lower-cased message; "%" and "$" anywhere also count).
RATE_TERMS_WORDS = frozenset({
    "rate", "rates", "apr", "interest", "points", "fee", "fees", "terms", "term", "ltv", "ltc", "arv",
    "pricing", "price", "prices", "cost", "costs", "charge", "charges", "payment", "payments",
    "origination", "prepayment", "prepay", "quote", "quotes", "percent", "percentage", "amortization",
})
RATE_TERMS_PHRASES = (
    "how much", "what do you charge", "closing costs", "how much will it cost", "what s the rate",
    "down payment", "interest only", "how many points",
)

# Outbound (AI reply) patterns that look like a quoted number: money amounts, percentages, "N points".
QUOTED_NUMBER_PATTERNS = (
    r"\$\s?\d",                                  # $5,000  $ 5k
    r"\d\s?%",                                   # 9.5%
    r"\b\d+(?:\.\d+)?\s?(?:percent|pct|bps|basis points)\b",
    r"\b\d+(?:\.\d+)?\s+points?\b",              # 2 points
    r"\b(?:rate|rates|apr)\b[^.?!]{0,40}\b\d",   # "rate of 9"
)

SNIPPET_CHARS = 300
KIND_RATE_TERMS = "rate_terms_handoff"
KIND_AI_HANDOFF = "ai_handoff"
KIND_AI_QUOTED_NUMBERS = "ai_quoted_numbers"
