"""
Best-effort extraction of ZIP + vertical from a Synthflow call transcript.

Primary capture path is Synthflow `collected_variables` (slots defined in a Flow
Designer flow). A flat-prompt inbound agent has no flow, so it emits no slots —
this module parses the raw transcript as a fallback so ZIP/vertical still land
when the structured slots are absent.

Used by SynthflowInboundPayload.resolved_zip / resolved_vertical AFTER the
structured-slot lookups fail. Pure functions, no I/O, never raise.
"""
from __future__ import annotations

import re
from typing import Any, Optional

# Spoken term -> canonical scoring vertical (config/scoring.py keys).
# get_sample_leads() indexes DistressScore.vertical_scores by these exact keys,
# so anything not mapped here must stay None (downstream defaults to roofing).
_VERTICAL_KEYWORDS: list[tuple[str, str]] = [
    ("roof", "roofing"),
    ("restoration", "restoration"),
    ("water damage", "restoration"),
    ("fire damage", "restoration"),
    ("remediation", "restoration"),
    ("mold", "restoration"),
    ("public adjuster", "public_adjusters"),
    ("adjuster", "public_adjusters"),
    ("wholesal", "wholesalers"),
    ("fix and flip", "fix_flip"),
    ("fix-and-flip", "fix_flip"),
    ("fix & flip", "fix_flip"),
    ("flipping", "fix_flip"),
    ("flip", "fix_flip"),
    ("attorney", "attorneys"),
    ("lawyer", "attorneys"),
    ("probate", "attorneys"),
]

# FL ZIPs in the served footprint start 33xxx/34xxx — prefer those, then any 5-digit.
_FL_ZIP_RE = re.compile(r"\b(3[34]\d{3})\b")
_ANY_ZIP_RE = re.compile(r"\b(\d{5})\b")


def transcript_to_text(transcript: Any) -> str:
    """Flatten a Synthflow transcript (string, or list of turn dicts) to one string."""
    if not transcript:
        return ""
    if isinstance(transcript, str):
        return transcript
    if isinstance(transcript, list):
        parts: list[str] = []
        for turn in transcript:
            if isinstance(turn, dict):
                parts.append(str(turn.get("text") or turn.get("content") or turn.get("message") or ""))
            else:
                parts.append(str(turn))
        return " ".join(p for p in parts if p)
    return str(transcript)


def extract_zip(transcript: Any) -> Optional[str]:
    """Pull a 5-digit ZIP from the transcript — prefer the FL footprint (33/34xxx)."""
    text = transcript_to_text(transcript)
    if not text:
        return None
    m = _FL_ZIP_RE.search(text) or _ANY_ZIP_RE.search(text)
    return m.group(1) if m else None


def extract_vertical(transcript: Any) -> Optional[str]:
    """Map the first recognised trade keyword in the transcript to a canonical vertical."""
    text = transcript_to_text(transcript).lower()
    if not text:
        return None
    # Earliest occurrence wins so "I do roofing, not flipping" → roofing.
    best: Optional[tuple[int, str]] = None
    for keyword, canonical in _VERTICAL_KEYWORDS:
        idx = text.find(keyword)
        if idx != -1 and (best is None or idx < best[0]):
            best = (idx, canonical)
    return best[1] if best else None
