"""
Durable, auditable record of identity-resolution decisions that correctly
refused to auto-merge (WP-4, WI-4).

record_exception()       — upsert one exception row (a rare event -- a
                          multi-anchor conflict -- so a single round trip
                          is fine).
record_exceptions_batch() — upsert many rows in ONE round trip. Required
                          for ambiguous-pair recording: run_incremental
                          scores the ENTIRE anchor table against every new
                          candidate (pre-existing design, not new here), so
                          a single sweep can produce hundreds of exception
                          rows -- looping record_exception() per row would
                          be one INSERT round trip per row, exactly the
                          anti-pattern this codebase forbids at scale (see
                          CLAUDE.md: "a loop that calls session.execute()
                          once per item is always wrong at scale").
list_open_exceptions()   — the EXCEPTIONS queue read, oldest first.
resolve_exception()      — mark a row merged/rejected/stale. Does NOT call
                          merge_entities() itself -- callers that resolve to
                          'merged' are expected to have already called
                          merge_entities() and pass its merge_log_id here,
                          in the same transaction.

Client spec: "Identity resolution is uncertain. Records stay separate and a
possible-match flag routes to EXCEPTIONS. Never auto-merged below a
confidence threshold."
"""
from __future__ import annotations

import json
import logging
from typing import Optional, TypedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Per-run cap on new exception rows -- the first full backfill over ~810k
# existing entities must not write a million exception rows in one sweep.
# Idempotent upsert means this only limits NEW pairs per run; already-seen
# pairs keep getting their last_seen_at bumped regardless of this cap.
DEFAULT_MAX_NEW_EXCEPTIONS_PER_RUN = 500


class _ExceptionRowRequired(TypedDict):
    kind: str
    left_ref: str
    right_ref: str
    explanation: str


class ExceptionRow(_ExceptionRowRequired, total=False):
    entity_ids: Optional[list]
    name_score: Optional[int]
    address_score: Optional[int]


def record_exceptions_batch(session: Session, rows: list[ExceptionRow]) -> None:
    """
    Upsert many exception rows in ONE INSERT statement (one round trip
    regardless of len(rows)). Idempotent on (kind, left_ref, right_ref) --
    a re-seen pair from a later sweep bumps last_seen_at; it does not
    create a duplicate row or reset a status a human already set
    (merged/rejected).

    kind must be one of 'ambiguous_pair', 'multi_anchor_conflict',
    'llm_different' (enforced by the DB CHECK constraint).

    No-op for an empty list. Does not commit — caller controls the
    transaction boundary.
    """
    if not rows:
        return

    value_clauses = []
    params: dict = {}
    for i, row in enumerate(rows):
        value_clauses.append(
            f"(:kind_{i}, :left_ref_{i}, :right_ref_{i}, CAST(:entity_ids_{i} AS jsonb), "
            f":name_score_{i}, :address_score_{i}, :explanation_{i}, now(), now())"
        )
        entity_ids = row.get("entity_ids")
        params.update({
            f"kind_{i}": row["kind"],
            f"left_ref_{i}": row["left_ref"],
            f"right_ref_{i}": row["right_ref"],
            f"entity_ids_{i}": json.dumps(entity_ids) if entity_ids is not None else None,
            f"name_score_{i}": row.get("name_score"),
            f"address_score_{i}": row.get("address_score"),
            f"explanation_{i}": row["explanation"],
        })

    session.execute(
        text(f"""
            INSERT INTO buyer_entity_match_exception
                (kind, left_ref, right_ref, entity_ids, name_score, address_score,
                 explanation, first_seen_at, last_seen_at)
            VALUES {", ".join(value_clauses)}
            ON CONFLICT (kind, left_ref, right_ref) DO UPDATE
                SET last_seen_at = now()
        """),
        params,
    )


def record_exception(
    session: Session,
    *,
    kind: str,
    left_ref: str,
    right_ref: str,
    explanation: str,
    entity_ids: Optional[list] = None,
    name_score: Optional[int] = None,
    address_score: Optional[int] = None,
) -> None:
    """
    Upsert a single exception row -- for the rare, one-off case (a
    multi-anchor conflict) where a batch call would be overkill. For
    anything that can produce more than a handful of rows per call, use
    record_exceptions_batch() instead.

    Does not commit — caller controls the transaction boundary.
    """
    record_exceptions_batch(session, [ExceptionRow(
        kind=kind, left_ref=left_ref, right_ref=right_ref, explanation=explanation,
        entity_ids=entity_ids, name_score=name_score, address_score=address_score,
    )])


def list_open_exceptions(session: Session, limit: int = 100) -> list[dict]:
    """The EXCEPTIONS queue read: open rows, oldest first (the ones waiting
    longest for review surface first)."""
    rows = session.execute(
        text("""
            SELECT id, kind, left_ref, right_ref, entity_ids, name_score,
                   address_score, explanation, first_seen_at, last_seen_at
            FROM buyer_entity_match_exception
            WHERE status = 'open'
            ORDER BY first_seen_at ASC
            LIMIT :limit
        """),
        {"limit": limit},
    )
    return [dict(row._mapping) for row in rows]


def resolve_exception(
    session: Session,
    exception_id: int,
    status: str,
    resolved_by: str,
    merge_log_id: Optional[int] = None,
) -> None:
    """
    Mark an exception row resolved. status must be 'merged', 'rejected', or
    'stale'. Does NOT call merge_entities() -- a caller resolving to
    'merged' must have already done that and pass its merge_log_id here, in
    the same transaction, so the exception row and the merge log stay
    consistent.

    Raises ValueError if status is not a valid terminal state or the row
    does not exist. Does not commit — caller controls the transaction
    boundary.
    """
    if status not in ("merged", "rejected", "stale"):
        raise ValueError(f"invalid resolution status: {status!r}")
    if status == "merged" and merge_log_id is None:
        raise ValueError("merge_log_id is required when resolving to 'merged'")

    result = session.execute(
        text("""
            UPDATE buyer_entity_match_exception
               SET status = :status, resolved_by = :resolved_by,
                   resolved_at = now(), merge_log_id = :merge_log_id
             WHERE id = :id
        """),
        {
            "status": status, "resolved_by": resolved_by,
            "merge_log_id": merge_log_id, "id": exception_id,
        },
    )
    if result.rowcount == 0:
        raise ValueError(f"buyer_entity_match_exception id={exception_id} not found")

    logger.info(
        "resolve_exception: id=%d -> %s by %s (merge_log_id=%s)",
        exception_id, status, resolved_by, merge_log_id,
    )
