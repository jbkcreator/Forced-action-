"""
Closer-call transcript tagging (Closer Cockpit, Sprint S1b).

Single-shot enrichment: a closer-call transcript in → structured domain tags out
(objections / objection_resolved / call_outcome / follow_ups). Sentiment & topics
come from Aircall natively, so Claude only produces the domain-specific fields.

Runs through claude_router.call_claude (Sonnet tier via task_type) so cost is
logged to api_usage_logs. Deliberately NOT a LangGraph graph and writes NO
agent_decisions row — it is post-call enrichment, not a Cora Touch (ADR
"closer-telemetry-separate-from-agent-decisions").

call_claude returns plain text (no tool/JSON mode), so we prompt for strict JSON,
extract+validate it, coerce to the controlled vocabularies, and retry once.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

from config.closer import (
    CALL_OUTCOMES_SET,
    CLOSER_CALL_TAGGING_TASK_TYPE,
    OBJECTION_RESOLUTIONS_SET,
    OBJECTION_TAXONOMY,
    OBJECTION_TAXONOMY_SET,
)
from src.services.claude_router import call_claude

logger = logging.getLogger(__name__)

_MAX_TRANSCRIPT_CHARS = 24000  # guard the prompt; calls rarely exceed this

_SYSTEM_PROMPT = (
    "You are a sales-call analyst for a distressed-property lead platform. A human "
    "closer called a contractor/investor subscriber to convert them on a paid plan. "
    "Read the transcript and extract structured tags. Respond with ONLY a single "
    "JSON object, no prose, no markdown fences.\n\n"
    "Schema:\n"
    "{\n"
    '  "objections": [<zero or more of: ' + ", ".join(OBJECTION_TAXONOMY) + ">],\n"
    '  "objection_resolved": "resolved" | "unresolved" | "none",\n'
    '  "call_outcome": "committed" | "callback_scheduled" | "undecided" | '
    '"declined" | "no_meaningful_conversation",\n'
    '  "follow_ups": [{"commitment": <string>, "due_date": <YYYY-MM-DD or null>}]\n'
    "}\n\n"
    "Rules: use ONLY the listed objection keys (no free text). objection_resolved "
    "is 'none' when no objection was raised. Resolve relative dates (e.g. "
    "'next Tuesday') against the call date provided. Empty lists are valid."
)


def _extract_json(text: str) -> Optional[dict]:
    """Pull the first JSON object out of an LLM text response."""
    if not text:
        return None
    cleaned = text.strip()
    # Strip ```json ... ``` fences if present.
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE).strip()
    try:
        return json.loads(cleaned)
    except ValueError:
        pass
    m = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except ValueError:
            return None
    return None


def _coerce_tags(raw: dict) -> dict:
    """Validate & coerce a parsed LLM object to the controlled vocabularies."""
    objections = [
        o for o in (raw.get("objections") or [])
        if isinstance(o, str) and o in OBJECTION_TAXONOMY_SET
    ]

    resolved = raw.get("objection_resolved")
    if resolved not in OBJECTION_RESOLUTIONS_SET:
        resolved = "none"

    outcome = raw.get("call_outcome")
    if outcome not in CALL_OUTCOMES_SET:
        outcome = None  # leave null rather than guess; sweep/manual can correct

    follow_ups: list[dict] = []
    for fu in (raw.get("follow_ups") or []):
        if isinstance(fu, dict) and fu.get("commitment"):
            follow_ups.append({
                "commitment": str(fu["commitment"])[:500],
                "due_date": fu.get("due_date") if isinstance(fu.get("due_date"), str) else None,
            })

    return {
        "objections": objections,
        "objection_resolved": resolved,
        "call_outcome": outcome,
        "follow_ups": follow_ups,
    }


def tag_transcript(
    transcript_text: str,
    call_ended_at: Optional[datetime] = None,
    subscriber_id: Optional[int] = None,
    db: Optional[Any] = None,
) -> Optional[dict]:
    """Tag a transcript. Returns coerced tags, or None if the LLM output could
    not be parsed after one retry (caller leaves the row untagged)."""
    if not transcript_text or not transcript_text.strip():
        return None

    transcript = transcript_text[:_MAX_TRANSCRIPT_CHARS]
    date_hint = (
        f"Call date: {call_ended_at.date().isoformat()}." if call_ended_at else ""
    )
    user_content = f"{date_hint}\n\nTranscript:\n{transcript}".strip()

    for attempt in (1, 2):
        try:
            text = call_claude(
                task_type=CLOSER_CALL_TAGGING_TASK_TYPE,
                messages=[{"role": "user", "content": user_content}],
                system=_SYSTEM_PROMPT,
                max_tokens=700,
                subscriber_id=subscriber_id,
                db=db,
            )
        except Exception as exc:
            logger.error("[closer_tagging] call_claude failed (attempt %s): %s", attempt, exc)
            continue
        parsed = _extract_json(text)
        if parsed is not None:
            return _coerce_tags(parsed)
        logger.warning("[closer_tagging] unparseable LLM output (attempt %s)", attempt)

    return None


def tag_closer_call(aircall_call_id: Optional[str]) -> bool:
    """Load the closer_calls row, tag it, and persist. Idempotent: a row that is
    already tagged or has no transcript is skipped. Returns True if tags written."""
    if not aircall_call_id:
        return False

    from sqlalchemy import select

    from src.core.database import Database
    from src.core.models import CloserCall

    db = Database()
    with db.session_scope() as session:
        row = session.execute(
            select(CloserCall).where(CloserCall.aircall_call_id == str(aircall_call_id))
        ).scalar_one_or_none()
        if row is None:
            logger.info("[closer_tagging] no row for call_id=%s", aircall_call_id)
            return False
        if row.tagged_at is not None:
            return False  # already tagged (idempotent)
        if not row.transcript_text:
            logger.info("[closer_tagging] no transcript yet call_id=%s", aircall_call_id)
            return False

        tags = tag_transcript(
            row.transcript_text,
            call_ended_at=row.ended_at,
            subscriber_id=row.subscriber_id,
            db=session,
        )
        if tags is None:
            logger.warning(
                "[closer_tagging] leaving call_id=%s untagged (LLM parse failed)",
                aircall_call_id,
            )
            return False

        row.objections = tags["objections"]
        row.objection_resolved = tags["objection_resolved"]
        row.call_outcome = tags["call_outcome"]
        row.follow_ups = tags["follow_ups"]
        row.tagged_at = datetime.now(timezone.utc)
        logger.info("[closer_tagging] tagged call_id=%s outcome=%s", aircall_call_id, tags["call_outcome"])
        return True
