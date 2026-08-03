"""
Shared writer for `lifecycle_playbook` recommendations (fa036, widened CLONE-v2.2).

Centralises the source-key dedupe + author-attribution rules so every write
path into this table — Lifecycle's (ab_engine.complete_test,
lifecycle_self_healing kill-recommendation, lifecycle_holdout_check) and any
other agent's (Vera/Cora/Hunter/fleet-wide, via the `agent_domain` and
`entry_kind` kwargs added in CLONE-v2.2) — goes through the same helper. The
unique partial index `idx_lifecycle_playbook_source_key_unique` enforces the
dedupe at the DB level; this helper just calls INSERT … ON CONFLICT DO NOTHING
so re-running the upstream task (e.g. ab_rollback_check on day 2 for an
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
    source_type: str,           # 'ab_test' | 'self_healing_kill' | 'holdout_test' | future...
    source_id: str,
    authored_by: str,           # 'lifecycle' for autonomous paths; <operator handle> for manual
    decision_id: Optional[str] = None,
    agent_domain: str = "lifecycle",   # CLONE-v2.2: 'lifecycle' | 'vera' | 'cora' | 'hunter' | 'fleet'
    entry_kind: str = "playbook",      # 'playbook' | 'anti_playbook'
    confidence: Optional[int] = None,  # LEARN-v2.2: 0-100
    scope: Optional[dict] = None,      # LEARN-v2.2: e.g. {"buyer_type": "...", "offer": "..."}
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
      agent_domain: which agent/domain authored this entry. Defaults to
                    'lifecycle' so every existing caller (ab_engine,
                    lifecycle_self_healing, lifecycle_holdout_check) is
                    byte-identical in behavior — same source_key format,
                    same dedupe. Non-'lifecycle' domains (Vera/Cora/Hunter/
                    fleet-wide) get their agent_domain prefixed into
                    source_key so their namespace can never collide with
                    Lifecycle's or each other's, without needing to touch
                    the existing unique index (still just source_key).
      entry_kind:   'playbook' (proven pattern) or 'anti_playbook'
                    (documented failure) — see docs/constitutions/*.md's
                    "playbooks at 3+ proofs; anti-playbooks at 3+ failures."
      confidence:   LEARN-v2.2 — 0-100, how strongly the evidence supports
                    this entry. None (default) leaves it unset; not every
                    caller has a confidence score to give.
      scope:        LEARN-v2.2 — portability dimensions this entry applies
                    to (e.g. {"buyer_type": "buy_and_hold", "offer":
                    "founder_tier"}) — the spec's "prospect / vertical /
                    county / offer / fleet" portability score. None
                    (default) leaves it unset — a fleet-wide entry with no
                    narrower scope.

    The dedupe contract:
      Two calls with the same (agent_domain, source_type, source_id) → only
      ONE row exists for non-'lifecycle' domains. For 'lifecycle' (the
      default), the contract is unchanged from before this was widened:
      same (source_type, source_id) → only ONE row. The second call
      returns None either way.
    """
    if entry_kind not in ("playbook", "anti_playbook"):
        raise ValueError(f"entry_kind must be 'playbook' or 'anti_playbook', got {entry_kind!r}")

    source_key = (
        f"{source_type}:{source_id}" if agent_domain == "lifecycle"
        else f"{agent_domain}:{source_type}:{source_id}"
    )
    row = session.execute(sa_text("""
        INSERT INTO lifecycle_playbook (
            name, description, pattern_json,
            authored_by, authored_at, status,
            source_type, source_id, source_key,
            agent_domain, entry_kind, confidence, scope, version,
            decision_id, created_at, updated_at
        ) VALUES (
            :name, :description, CAST(:pattern AS jsonb),
            :authored_by, NOW(), 'recommended',
            :source_type, :source_id, :source_key,
            :agent_domain, :entry_kind, :confidence, CAST(:scope AS jsonb), 1,
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
        "agent_domain": agent_domain,
        "entry_kind":   entry_kind,
        "confidence":   confidence,
        "scope":        json.dumps(scope) if scope is not None else None,
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


def supersede_recommendation(
    session: Session,
    old_id: int,
    new_id: int,
) -> bool:
    """Mark an older lesson as superseded by a newer, validated version.

    LEARN-v2.2 Layer 4 (Step 11) — memory pruning building block. Preserves
    audit history (old_id's row is never deleted or overwritten) rather
    than losing the superseded content. Only transitions from
    'recommended' or 'adopted' — a row already 'rejected'/'retired' is a
    closed decision, not something a later experiment should silently
    override.
    """
    result = session.execute(sa_text("""
        UPDATE lifecycle_playbook
        SET status            = 'superseded',
            superseded_by_id  = :new_id,
            updated_at        = NOW()
        WHERE id = :old_id AND status IN ('recommended', 'adopted')
    """), {"old_id": old_id, "new_id": new_id})
    return result.rowcount > 0


def mark_contradicted(session: Session, playbook_id: int) -> bool:
    """Mark a lesson as contradicted by accumulated counter-evidence.

    LEARN-v2.2 Layer 4 (Step 11) — distinct from transition_status's
    'rejected' (a human's judgment call): 'contradicted' is the automatic,
    threshold-driven outcome per the fleet constitutions' "anti-playbooks
    at 3+ failures" rule (docs/constitutions/*.md) — the caller (a
    scheduled pruning job) is responsible for deciding the threshold was
    met; this just records the terminal state once it has.
    """
    result = session.execute(sa_text("""
        UPDATE lifecycle_playbook
        SET status      = 'contradicted',
            updated_at  = NOW()
        WHERE id = :id AND status IN ('recommended', 'adopted')
    """), {"id": playbook_id})
    return result.rowcount > 0
