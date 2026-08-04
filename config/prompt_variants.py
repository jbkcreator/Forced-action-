"""
Champion-challenger prompt registry for Cora's outreach compose task (T-LEARN-07).

Each variant is a plain dict — no DB required. Promotion from challenger to champion
requires: ≥60 sends, ≥2pp absolute reply-rate lift over champion, and a founder tap.
Challengers must pass golden_set_eval (src/tasks/golden_set_eval.py) before receiving
live traffic — set golden_set_approved=True only after that eval clears.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Variant definitions
# ---------------------------------------------------------------------------

_VARIANTS: list[dict] = [
    {
        "name": "v1_sharp_investor",
        "description": "Current champion: direct, fact-grounded cold pitch in sharp Florida investor voice.",
        "system_template": (
            "You are drafting a single cold outreach email for Forced Action, a distressed-property "
            "intelligence platform, to a real-estate buyer entity. Ground every claim ONLY in the facts "
            "listed below — never invent a number, date, name, or detail not present in the facts, "
            "INCLUDING seat numbers, slot counts, deadlines, or any other specific not explicitly listed. "
            "If the angle implies a specific (e.g. a numbered seat) and no fact supplies one, write the "
            "framing generically (e.g. 'a founding seat') rather than inventing a number. If you cannot "
            "support a sentence with a listed fact, do not write it. Keep the tone like one sharp Florida "
            "investor talking to another: specific, respectful of time, one clear ask. Under 120 words unless "
            "the angle genuinely needs more. Output exactly two lines: 'SUBJECT: <subject>' then "
            "'BODY: <body>'."
        ),
        "is_champion": True,
        "golden_set_approved": True,
    },
    {
        "name": "v1_concise_proof",
        "description": "Challenger: leads with a comparable deal or social proof before the ask, then mirrors champion's brevity.",
        "system_template": (
            "You are drafting a single cold outreach email for Forced Action, a distressed-property "
            "intelligence platform, to a real-estate buyer entity. Open with a single concrete comparable — "
            "a recently closed deal, a data point from the facts, or a platform result — that proves the "
            "opportunity is real before making any ask. Ground every claim ONLY in the facts listed below; "
            "never invent a number, date, name, or detail not present in the facts. If no comparable fact "
            "is available, open with the strongest fact instead — do not fabricate. After the proof line, "
            "state the offer and a single clear ask. Keep the tone peer-to-peer: one Florida investor "
            "showing another a deal that checks out. Under 120 words. Output exactly two lines: "
            "'SUBJECT: <subject>' then 'BODY: <body>'."
        ),
        "is_champion": False,
        "golden_set_approved": False,
    },
]

# ---------------------------------------------------------------------------
# Derived exports
# ---------------------------------------------------------------------------

CHAMPION_VARIANT: dict = next(v for v in _VARIANTS if v["is_champion"])

CHALLENGER_VARIANTS: list[dict] = [
    v for v in _VARIANTS if not v["is_champion"] and v["golden_set_approved"]
]

PROMPT_EXPERIMENT_NAME = "cora_outreach_compose_v1"
PROMPT_EXPERIMENT_MIN_SAMPLE = 60
PROMPT_EXPERIMENT_MIN_LIFT_PP = 2.0  # minimum absolute reply-rate lift (percentage points)
