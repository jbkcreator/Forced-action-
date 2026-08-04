"""
LEARN-v2.2 Layer 5 (T-LEARN-08) — lesson retrieval with status gate + scope filter.

Spec §9.4: "Memory pruning: contradicted, low-value, superseded lessons archive
out of retrieval; every lesson carries a portability score."

The write side (playbook_writer.py, learning_hygiene_sweep, mark_contradicted,
supersede_recommendation) was built in PRs #201/#203. This module is the missing
read side — the thing that ensures marked lessons actually stop surfacing.

THREE DESIGN DECISIONS (do not change without updating config):

1. DEFAULT-DENY STATUS FILTER.
   Only 'recommended' and 'adopted' lessons enter retrieval. Any future status
   ('experimental', 'deprecated', etc.) is excluded until explicitly added here.
   A grow-then-filter approach is the spec miss this module fixes.

2. SCOPE = SUBSET MATCH.
   A lesson with scope {"offer": "founder_tier"} matches a thread context of
   {"offer": "founder_tier", "avenue": "flipper"} — all lesson scope keys must
   be present with identical values in the thread context. A lesson with a
   narrower scope matches more threads than a broad one; a null scope matches
   everything (fleet-level lesson).

3. LOW-VALUE TRIGGER STAYS OUT.
   The hygiene sweep (L4 Step 12) reports staleness but never mutates on value.
   "Low-value" is undefined in the spec numerically. Adding it here before that
   is defined would make retrieval depend on a signal that does not exist.

Usage:
    from src.services.playbook_retrieval import fetch_lessons

    lessons = fetch_lessons(db, agent_domain="cora", context={"offer": "founder_tier"})
    # Returns list[dict] with keys: id, lesson, entry_kind, confidence, scope
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Statuses that may enter retrieval (decision 1 — default-deny).
RETRIEVAL_STATUSES = ("recommended", "adopted")

# Max lessons returned per call. Keeps prompt injection bounded.
MAX_LESSONS = 5

_FETCH_SQL = """
SELECT id, lesson, entry_kind, confidence, scope
FROM lifecycle_playbook
WHERE agent_domain = :agent_domain
  AND status IN :statuses
  AND (scope IS NULL OR scope = '{}'::jsonb OR scope <@ CAST(:context AS jsonb))
ORDER BY
  CASE status WHEN 'adopted' THEN 0 ELSE 1 END,
  confidence DESC,
  authored_at DESC
LIMIT :limit
"""


def fetch_lessons(
    db: Session,
    *,
    agent_domain: str,
    context: Optional[dict] = None,
    max_lessons: int = MAX_LESSONS,
) -> list[dict]:
    """Return active lessons for `agent_domain` that match `context` scope.

    `context` should contain the keys the current outreach/reply carries:
    offer, avenue, angle, buyer_type — any subset is fine. A lesson whose
    scope is a subset of `context` is returned; null-scope lessons are always
    returned (they apply fleet-wide).

    Returns a list of dicts (id, lesson, entry_kind, confidence, scope).
    Never raises — a retrieval failure must not block draft generation.
    """
    if context is None:
        context = {}

    import json

    try:
        rows = db.execute(
            text(_FETCH_SQL),
            {
                "agent_domain": agent_domain,
                "statuses": tuple(RETRIEVAL_STATUSES),
                "context": json.dumps(context),
                "limit": max_lessons,
            },
        ).fetchall()
        return [
            {
                "id": r.id,
                "lesson": r.lesson,
                "entry_kind": r.entry_kind,
                "confidence": r.confidence,
                "scope": r.scope,
            }
            for r in rows
        ]
    except Exception:
        logger.exception(
            "[playbook_retrieval] fetch failed for domain=%s context=%s — returning empty",
            agent_domain, context,
        )
        return []


def format_lessons_for_prompt(lessons: list[dict]) -> str:
    """Format retrieved lessons as a compact block for system prompt injection.

    Returns empty string if lessons is empty (caller skips the block entirely).
    """
    if not lessons:
        return ""
    lines = ["Lessons learned from past outreach (apply where relevant):"]
    for lesson in lessons:
        kind = lesson.get("entry_kind", "lesson")
        conf = lesson.get("confidence", 0)
        text_body = lesson.get("lesson", "")
        lines.append(f"- [{kind}, confidence {conf}%] {text_body}")
    return "\n".join(lines)
