"""Curated subscriber-tag suggestions for the operator CRM (fa045).

Operators can create any free-form tag (limited to 50 chars by the schema).
This list powers the autocomplete chips in the Notes & Tags surface and
documents the tags that carry behavioral side-effects in other services.
"""

# Tag → human label + optional side-effect description.
SUGGESTED_TAGS: dict[str, dict[str, str]] = {
    "vip":          {"label": "VIP",          "effect": ""},
    "at_risk":      {"label": "At Risk",      "effect": ""},
    "power_user":   {"label": "Power User",   "effect": ""},
    "coach_weekly": {"label": "Coach Weekly", "effect": ""},
    "founder_call": {"label": "Founder Call", "effect": ""},
    "needs_followup": {"label": "Needs Follow-up", "effect": ""},
    "do_not_text":  {
        "label": "Do Not Text",
        "effect": "sms_compliance.can_send_with_type blocks marketing SMS",
    },
    "do_not_call":  {"label": "Do Not Call",  "effect": ""},
    "billing_issue": {"label": "Billing Issue", "effect": ""},
    "test_account": {"label": "Test Account", "effect": ""},
}


def suggestion_list() -> list[dict]:
    """Return suggestions as a UI-friendly list."""
    return [
        {"tag": tag, "label": meta["label"], "effect": meta["effect"]}
        for tag, meta in SUGGESTED_TAGS.items()
    ]
