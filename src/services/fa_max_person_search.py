"""Fuzzy search backend for existing FA Max borrower lookup.

Addendum to WP-T2-6 -- fuzzy search over fa_max_persons for the Backflip
submission-logging modal's live "search existing borrowers" field (Task 18's
external_select). Read-only, no side effects.

Follows this codebase's established property/owner-name matching waterfall
(src/loaders/base.py, CLAUDE.md's loader-matching convention) scaled down
to one step: pg_trgm similarity via ILIKE-backed trigram index, which is
fast enough for a live Slack search-as-you-type call (must respond within
Slack's ~3 second block_suggestion window) without needing rapidfuzz's
heavier in-Python scoring pass on a small already-narrowed candidate set.

Matching against name is deliberately imprecise -- the caller (Task 18) is
responsible for showing disambiguating details (email, phone, last known
stage) so a human, not this function, makes the final identity call. This
function never claims a single "best" match; it returns candidates.
"""
from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

_SEARCH_SQL = text("""
    SELECT
        p.person_id::text,
        p.full_name,
        p.email,
        p.phone,
        (
            SELECT o.current_stage
            FROM fa_max_opportunities o
            WHERE o.person_id = p.person_id
            ORDER BY o.updated_at DESC
            LIMIT 1
        ) AS last_stage
    FROM fa_max_persons p
    WHERE p.merged_into_id IS NULL
      AND (
          p.full_name ILIKE '%' || :query || '%'
          OR p.email ILIKE '%' || :query || '%'
          OR p.phone = :query
      )
    ORDER BY similarity(COALESCE(p.full_name, ''), :query) DESC
    LIMIT :limit
""")


def search_fa_max_persons(session: Session, query: str, *, limit: int = 5) -> List[Dict[str, Any]]:
    """Search fa_max_persons by fuzzy name/email/phone match.

    Args:
        session: SQLAlchemy session.
        query: Search string (name, email, or phone). Empty/whitespace returns [].
        limit: Max results to return (default 5).

    Returns:
        List of dicts with keys: person_id, full_name, email, phone, last_stage.
    """
    query = (query or "").strip()
    if not query:
        return []

    rows = session.execute(_SEARCH_SQL, {"query": query, "limit": limit}).fetchall()
    return [dict(row._mapping) for row in rows]
