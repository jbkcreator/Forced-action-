"""High-intent inbound classifier (Block 11 / B11-02).

Additive score over 4 signals with a single hard cutoff — see wayfinder
map .scratch/block-11-inbound-velocity/SPEC.md (ticket 01) for the decision
record. Not related to `high_intent_no_convert` in synthflow_voice_drop.py,
which is an outbound trigger.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

INTENT_SLOT_POINTS = 50
TRANSCRIPT_INTENT_POINTS = 30
KNOWN_CALLER_POINTS = 15
LIVE_TERRITORY_POINTS = 10

HOT_CUTOFF = 50

# Keyword list is a starting default, not a locked decision — tune from
# reported false-positive/negative rates (B11-04 report-only optimization).
_TRANSCRIPT_INTENT_KEYWORDS = (
    "pricing", "price", "cost", "how much",
    "subscribe", "subscription", "sign up", "signup",
    "buy", "purchase", "interested in leads", "want leads",
    "ready to start", "ready to buy",
)


class IntentScoreResult(TypedDict):
    is_hot: bool
    score: int
    matched_signals: List[str]


def _matches_transcript_intent(transcript: Optional[str]) -> bool:
    if not transcript:
        return False
    lowered = transcript.lower()
    return any(keyword in lowered for keyword in _TRANSCRIPT_INTENT_KEYWORDS)


def _is_known_caller(phone: Optional[str], db: Session) -> bool:
    if not phone:
        return False
    row = db.execute(
        text("SELECT id FROM subscribers WHERE phone = :phone LIMIT 1"),
        {"phone": phone},
    ).first()
    return row is not None


def _is_live_territory(zip_code: Optional[str], vertical: Optional[str], db: Session) -> bool:
    if not zip_code or not vertical:
        return False
    row = db.execute(
        text(
            "SELECT id FROM zip_territories "
            "WHERE zip_code = :zip_code AND vertical = :vertical LIMIT 1"
        ),
        {"zip_code": zip_code, "vertical": vertical},
    ).first()
    return row is not None


def score_inbound(
    phone: Optional[str],
    zip_code: Optional[str],
    vertical: Optional[str],
    transcript: Optional[str],
    intent_slot: bool,
    db: Session,
) -> IntentScoreResult:
    """
    Score an inbound call for high intent. Cheap signals only — this runs
    inside the sub-60s inbound-response window (B11-01).

    Returns {is_hot, score, matched_signals}. Consent is NOT part of this
    score — TCPA/PEWC gating happens at the callback trigger (B11-03).
    """
    score = 0
    matched: List[str] = []

    if intent_slot:
        score += INTENT_SLOT_POINTS
        matched.append("intent_slot")

    if _matches_transcript_intent(transcript):
        score += TRANSCRIPT_INTENT_POINTS
        matched.append("transcript_intent")

    if _is_known_caller(phone, db):
        score += KNOWN_CALLER_POINTS
        matched.append("known_caller")

    if _is_live_territory(zip_code, vertical, db):
        score += LIVE_TERRITORY_POINTS
        matched.append("live_territory")

    is_hot = score >= HOT_CUTOFF

    logger.info(
        "inbound_intent scored phone_last4=%s score=%d is_hot=%s signals=%s",
        (phone or "")[-4:], score, is_hot, matched,
    )

    return {"is_hot": is_hot, "score": score, "matched_signals": matched}
