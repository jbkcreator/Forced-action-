"""
Shared writer for `lifecycle_playbook` recommendations (fa036).

Centralises the source-key dedupe + author-attribution rules so every Lifecycle
write path (ab_engine.complete_test, lifecycle_self_healing kill-recommendation,
future explicit recommendations) goes through the same helper. The unique
partial index `idx_lifecycle_playbook_source_key_unique` enforces the dedupe at
the DB level; this helper just calls INSERT … ON CONFLICT DO NOTHING so
re-running the upstream task (e.g. ab_rollback_check on day 2 for an
already-recommended test) silently skips.

All DB I/O is raw SQL via sa_text — repo convention.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def upsert_recommendation(
    session: Session,
    *,
    name: str,
    description: str,
    pattern: dict,
    source_type: str,           # 'ab_test' | 'self_healing_kill' | future...
    source_id: str,
    authored_by: str,           # 'lifecycle' for autonomous paths; <operator handle> for manual
    decision_id: Optional[str] = None,
) -> Optional[int]:
    """INSERT a `lifecycle_playbook` row idempotently keyed by source_key.

    Returns the new id, or None if a row with the same source_key already
    existed (the second call's INSERT hit ON CONFLICT DO NOTHING).

    Args:
      session:      live SQLAlchemy session.
      name:         short label, indexed via existing `idx_lifecycle_playbook_authored`.
      description:  free-text reason this recommendation exists.
      pattern:      JSONB payload — the actual pattern definition (variant
                    config, feature flag name, threshold, etc.).
      source_type:  category — 'ab_test' or 'self_healing_kill' today.
                    New sources just pass their own string; no enum constraint.
      source_id:    unique identifier within source_type (test_name,
                    metric_name, etc.). Concatenated into source_key.
      authored_by:  'lifecycle' for autonomous paths, <operator> for manual.
                    The Metric 5 ("net new playbooks Lifecycle authored")
                    aggregation filters on `authored_by = 'lifecycle'`.
      decision_id:  optional link to the triggering `agent_decisions` row.

    The dedupe contract:
      Two calls with the same (source_type, source_id) → only ONE row exists.
      The second call returns None.
    """
    source_key = f"{source_type}:{source_id}"
    row = session.execute(sa_text("""
        INSERT INTO lifecycle_playbook (
            name, description, pattern_json,
            authored_by, authored_at, status,
            source_type, source_id, source_key,
            decision_id, created_at, updated_at
        ) VALUES (
            :name, :description, CAST(:pattern AS jsonb),
            :authored_by, NOW(), 'recommended',
            :source_type, :source_id, :source_key,
            :decision_id, NOW(), NOW()
        )
        ON CONFLICT (source_key) WHERE source_key IS NOT NULL
        DO NOTHING
        RETURNING id
    """), {
        "name":         name,
        "description":  description,
        "pattern":      json.dumps(pattern),
        "authored_by":  authored_by,
        "source_type":  source_type,
        "source_id":    source_id,
        "source_key":   source_key,
        "decision_id":  decision_id,
    }).first()

    if row is None:
        logger.debug(
            "[playbook] dedupe — recommendation already exists for source_key=%s",
            source_key,
        )
        return None

    new_id = int(row.id)
    logger.info(
        "[playbook] new recommendation id=%d source_key=%s authored_by=%s",
        new_id, source_key, authored_by,
    )
    return new_id


def transition_status(
    session: Session,
    playbook_id: int,
    *,
    to_status: str,
    actor: str,
    reason: Optional[str] = None,
) -> bool:
    """Transition a playbook to adopted / rejected / retired.

    Returns True if the row was updated, False if no such row or already
    in a terminal state for this transition. Idempotent — calling adopt
    twice doesn't change adopted_at.
    """
    if to_status == "adopted":
        result = session.execute(sa_text("""
            UPDATE lifecycle_playbook
            SET status      = 'adopted',
                adopted_at  = COALESCE(adopted_at, NOW()),
                adopted_by  = COALESCE(adopted_by, :actor),
                updated_at  = NOW()
            WHERE id = :id AND status = 'recommended'
        """), {"id": playbook_id, "actor": actor})
    elif to_status == "rejected":
        result = session.execute(sa_text("""
            UPDATE lifecycle_playbook
            SET status            = 'rejected',
                rejected_at       = COALESCE(rejected_at, NOW()),
                rejected_by       = COALESCE(rejected_by, :actor),
                rejection_reason  = COALESCE(rejection_reason, :reason),
                updated_at        = NOW()
            WHERE id = :id AND status = 'recommended'
        """), {"id": playbook_id, "actor": actor, "reason": reason})
    elif to_status == "retired":
        result = session.execute(sa_text("""
            UPDATE lifecycle_playbook
            SET status     = 'retired',
                retired_at = COALESCE(retired_at, NOW()),
                retired_by = COALESCE(retired_by, :actor),
                updated_at = NOW()
            WHERE id = :id AND status = 'adopted'
        """), {"id": playbook_id, "actor": actor})
    else:
        raise ValueError(
            f"to_status must be one of 'adopted','rejected','retired', got {to_status!r}"
        )

    return result.rowcount > 0
