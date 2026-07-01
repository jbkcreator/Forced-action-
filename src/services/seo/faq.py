"""Fetch the best published Quora answer for a vertical's FAQ section."""
import logging
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


def best_faq(db: Session, vertical: str) -> Optional[dict]:
    """Return {title, answer_draft} for the best published Quora answer, or None.

    ponytail: quora_topics has no cluster/vertical column yet; returns the global
    best published answer. Update when cluster column is added to filter by vertical.
    FAQ is additive — None does not block page publication.
    """
    row = db.execute(
        text("""
            SELECT title, answer_draft
            FROM quora_questions
            WHERE answer_status = 'published'
              AND answer_draft IS NOT NULL
            ORDER BY published_at DESC NULLS LAST,
                     priority_score  DESC NULLS LAST
            LIMIT 1
        """),
    ).mappings().fetchone()

    if row is None:
        return None
    return {"title": row["title"], "answer_draft": row["answer_draft"]}
