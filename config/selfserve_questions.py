"""WP-7 WI-5 — the self-serve flow's confirmation questions.

v1 fallback list, locked 2026-09-16 (plan §7 Q4). Not yet Backflip's or the
client's own list — build against this, tighten once Backflip answers.
Swapping in the real list later is an edit to this file, not a schema or
code change.
"""
from __future__ import annotations

SELFSERVE_QUESTIONS: list[dict] = [
    {"key": "purchase_price", "label": "Purchase price / under contract?", "type": "text"},
    {"key": "rehab_budget", "label": "Rehab budget", "type": "currency"},
    {"key": "exit_strategy", "label": "Exit strategy", "type": "select", "options": ["flip", "hold"]},
    {"key": "timeline_to_close", "label": "Timeline to close", "type": "text"},
    {"key": "prior_flip_count", "label": "Number of prior flips", "type": "integer"},
    {"key": "entity_name", "label": "Entity name the loan closes in", "type": "text"},
    {"key": "already_owned", "label": "Is the property already owned?", "type": "boolean"},
]

CONSENT_COPY = (
    "I agree Forced Action may contact me by email and text message about this inquiry."
)
CONSENT_CHANNELS = ("email", "sms")
