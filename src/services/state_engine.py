"""
FA Max Durable State Engine (WP-1).

Single write path for all FA Max entity state transitions. Every call to
`transition()` is:
  1. Protected by pg_advisory_xact_lock (correctness — survives Redis outage).
  2. Optionally wrapped by a Redis SET-NX-EX contention-avoidance lock
     (performance — avoided at the caller's request via acquire_redis_lock=False
     for high-throughput batch paths that already hold their own Redis lock).
  3. Idempotent — ON CONFLICT (idempotency_key) DO NOTHING on the event row.
  4. CAS-guarded — the UPDATE only applies when current_state = from_state;
     a stale call is detected and returned as TransitionResult.already_advanced.

Caller contract
---------------
Every caller must supply a deterministic `idempotency_key` built from stable
inputs (entity_uuid + from_state + to_state + actor + clock-epoch-minute, or
an external event ID). Never auto-generate it inside this function; the caller
owns the key so it can re-supply the same one on retry.

Callers must open their own session via get_db_context() and pass it in.
This function does NOT open or commit a session — it participates in the
caller's transaction so the state-column update and the event-row insert are
atomic.

WP-1 "Done When" guarantee
---------------------------
A worker killed after the advisory lock is acquired but before commit will
roll back automatically (transaction-scoped lock releases on disconnect).
The next worker re-runs the transition from the last committed state.
No partial state is possible.

Boundary — what WP-1 does NOT include
--------------------------------------
This module is infrastructure only. It does not include an FA Max LangGraph
graph, Slack orchestration, or any decision logic. The first FA Max workflow
graph is a downstream work package's responsibility. When that graph is
built, it must:

  1. Register each concrete event type as its own exact key in
     src/agents/router.py's EVENT_TO_GRAPH dict — the router does an exact
     dict lookup (`EVENT_TO_GRAPH.get(event_type)`), not a prefix/wildcard
     match, so "fa_max.*" is not a valid registration; every FA Max event
     type needs its own entry (unregistered event types correctly fail
     closed rather than falling through to the wrong graph).
  2. Adapt to the existing runner contract
     `runner(event_payload, subscriber_id, decision_id=...)`, which is
     subscriber_id-oriented. FA Max identifies people by person_id, not
     subscriber_id. Follow the existing `_run_retention_adapter` precedent
     in router.py: write a thin adapter that pulls person_id out of
     event_payload and tolerates the unused subscriber_id argument, rather
     than changing the shared router/supervisor contract.
  3. Load current state via get_person_state()/get_person_history() as the
     graph's first business node (basic event-payload validation may run
     before it), then request every lifecycle change through transition()
     below — never write to lifecycle_state/current_stage directly.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

_REDIS_LOCK_PREFIX = "lock:state:"
_REDIS_LOCK_TTL = 30  # seconds — short; Postgres advisory lock is the true guard


class TransitionOutcome(str, Enum):
    succeeded = "succeeded"
    already_advanced = "already_advanced"  # CAS miss: current_state != from_state
    idempotent_skip = "idempotent_skip"    # idempotency_key already exists (no-op)
    invalid_transition = "invalid_transition"  # to_state not in allowed_next


@dataclass
class TransitionResult:
    outcome: TransitionOutcome
    current_state: Optional[str] = None  # state after the call
    event_id: Optional[str] = None       # UUID of the written event row, or None on skip


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def transition(
    *,
    session: Session,
    entity_type: str,
    entity_uuid: str,
    from_state: str,
    to_state: str,
    actor: str,
    source_component: str,
    idempotency_key: str,
    person_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    acquire_redis_lock: bool = True,
    validate_allowed_next: bool = True,
) -> TransitionResult:
    """Atomically transition an FA Max entity's state.

    Parameters
    ----------
    session:
        Active SQLAlchemy session. Caller owns commit/rollback.
    entity_type:
        One of: person, property, opportunity, partner, interaction.
    entity_uuid:
        UUID from fa_max_entity_registry for this entity.
    from_state / to_state:
        Expected current state and desired next state.
    actor:
        'agent:<name>', 'user:josh', or 'system:<component>'.
    source_component:
        Module path of the caller. Used for tracing in the event row.
    idempotency_key:
        Caller-supplied deterministic key. Must be stable across retries.
    person_id:
        If the entity is (or is associated with) a person, supply their
        person_id as the borrower-history partition key.
    decision_id:
        FK into agent_decisions, when applicable.
    context:
        Arbitrary JSONB payload for audit context.
    acquire_redis_lock:
        Set False only when the caller already holds a Redis lock for this
        entity (to avoid double-locking). Postgres advisory lock always runs.
    validate_allowed_next:
        When True (default), rejects to_state not listed in allowed_next for
        the entity type's stage config table. Set False for admin overrides
        (only call sites that explicitly document why the bypass is safe).
    """
    if context is None:
        context = {}

    _acquire_pg_advisory_lock(session, entity_uuid)

    redis_lock_acquired = False
    if acquire_redis_lock:
        redis_lock_acquired = _try_acquire_redis_lock(entity_type, entity_uuid)

    try:
        return _do_transition(
            session=session,
            entity_type=entity_type,
            entity_uuid=entity_uuid,
            from_state=from_state,
            to_state=to_state,
            actor=actor,
            source_component=source_component,
            idempotency_key=idempotency_key,
            person_id=person_id,
            decision_id=decision_id,
            context=context,
            validate_allowed_next=validate_allowed_next,
        )
    finally:
        if redis_lock_acquired:
            _release_redis_lock(entity_type, entity_uuid)


def get_person_state(*, session: Session, person_id: str) -> Optional[Dict[str, Any]]:
    """Load the current lifecycle state for a person. Returns None if not found.

    Intended for use as the first node in any FA Max LangGraph: load state,
    abort if None (person not yet registered), then proceed with decision logic.

    Mirrors the _node_load_profile abort-if-not-found pattern from
    src/agents/graphs/retention.py.
    """
    row = session.execute(
        text("""
            SELECT
                p.person_id::text,
                p.lifecycle_state,
                p.source,
                p.source_reference,
                p.created_at,
                p.updated_at,
                p.merged_into_id::text AS merged_into_id
            FROM fa_max_persons p
            WHERE p.person_id = :person_id ::uuid
        """),
        {"person_id": person_id},
    ).fetchone()

    if row is None:
        return None

    return dict(row._mapping)


def get_person_history(
    *, session: Session, person_id: str, limit: int = 100
) -> list[Dict[str, Any]]:
    """Return ordered state-transition history for a person.

    Includes all entity types associated with this person (opportunities,
    properties, interactions) via the person_id partition key.

    Satisfies WP-1 Done When: 'complete ordered history of a borrower can
    be queried.'
    """
    rows = session.execute(
        text("""
            SELECT
                e.event_id::text,
                e.entity_uuid::text,
                e.entity_type,
                e.from_state,
                e.to_state,
                e.actor,
                e.source_component,
                e.decision_id::text AS decision_id,
                e.context,
                e.occurred_at
            FROM fa_max_state_transition_events e
            WHERE e.person_id = :person_id ::uuid
            ORDER BY e.occurred_at ASC
            LIMIT :limit
        """),
        {"person_id": person_id, "limit": limit},
    ).fetchall()

    return [dict(r._mapping) for r in rows]


def ensure_entity_registry(
    *,
    session: Session,
    entity_type: str,
    native_id: str,
) -> str:
    """Return the entity_uuid for (entity_type, native_id), creating if absent.

    Idempotent. Multiple concurrent callers on the same (type, native_id) are
    safe — INSERT ... ON CONFLICT DO NOTHING + SELECT.
    """
    session.execute(
        text("""
            INSERT INTO fa_max_entity_registry (entity_type, native_id)
            VALUES (:entity_type, :native_id)
            ON CONFLICT (entity_type, native_id) DO NOTHING
        """),
        {"entity_type": entity_type, "native_id": str(native_id)},
    )
    row = session.execute(
        text("""
            SELECT entity_uuid::text
            FROM fa_max_entity_registry
            WHERE entity_type = :entity_type AND native_id = :native_id
        """),
        {"entity_type": entity_type, "native_id": str(native_id)},
    ).fetchone()
    return row.entity_uuid  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _acquire_pg_advisory_lock(session: Session, entity_uuid: str) -> None:
    """Transaction-scoped advisory lock on entity_uuid.

    Uses hashtext() of the UUID string (consistent within a Postgres session).
    Auto-releases on commit/rollback/disconnect — no explicit release needed.
    Blocks until the lock is available (FIFO queue across concurrent callers).

    Pattern sourced from src/services/lead_exclusivity.py.
    """
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": str(entity_uuid)},
    )


def _try_acquire_redis_lock(entity_type: str, entity_uuid: str) -> bool:
    """Optimistic Redis contention-avoidance lock. Fails open on Redis unavail.

    Returns True if the lock was acquired (or Redis is unavailable — fail-open
    matches cora/locks.py convention). Correctness is always guaranteed by the
    Postgres advisory lock; Redis is only a performance optimization to avoid
    queuing at the Postgres lock point.
    """
    if not redis_available():
        return False  # didn't acquire (no Redis), but proceed — Postgres guards
    key = f"{_REDIS_LOCK_PREFIX}{entity_type}:{entity_uuid}"
    try:
        result = get_redis().set(key, "1", nx=True, ex=_REDIS_LOCK_TTL)
        return bool(result)
    except Exception:
        logger.warning("Redis lock acquire failed for %s:%s — proceeding without", entity_type, entity_uuid)
        return False


def _release_redis_lock(entity_type: str, entity_uuid: str) -> None:
    if not redis_available():
        return
    key = f"{_REDIS_LOCK_PREFIX}{entity_type}:{entity_uuid}"
    try:
        get_redis().delete(key)
    except Exception:
        logger.warning("Redis lock release failed for %s:%s — lock will expire naturally", entity_type, entity_uuid)


_ENTITY_STATE_COLUMN: Dict[str, tuple[str, str, str]] = {
    # entity_type -> (table, pk_column, state_column)
    "person": ("fa_max_persons", "person_id", "lifecycle_state"),
    "opportunity": ("fa_max_opportunities", "opportunity_id", "current_stage"),
}

_ENTITY_STAGE_CONFIG_TABLE: Dict[str, str] = {
    "person": "fa_max_person_lifecycle_stage_config",
    "opportunity": "fa_max_opportunity_stage_config",
}


def _do_transition(
    *,
    session: Session,
    entity_type: str,
    entity_uuid: str,
    from_state: str,
    to_state: str,
    actor: str,
    source_component: str,
    idempotency_key: str,
    person_id: Optional[str],
    decision_id: Optional[str],
    context: Dict[str, Any],
    validate_allowed_next: bool,
) -> TransitionResult:
    import json as _json

    # 1. Validate allowed_next when required.
    if validate_allowed_next and entity_type in _ENTITY_STAGE_CONFIG_TABLE:
        # config_table is looked up from the fixed _ENTITY_STAGE_CONFIG_TABLE
        # dict above — never caller/request-supplied — so the f-string below
        # cannot be used for SQL injection.
        config_table = _ENTITY_STAGE_CONFIG_TABLE[entity_type]
        config_row = session.execute(
            text(f"SELECT allowed_next FROM {config_table} WHERE stage_key = :stage"),
            {"stage": from_state},
        ).fetchone()
        if config_row is None:
            logger.warning(
                "Invalid transition %s->%s for %s: from_state not in %s",
                from_state, to_state, entity_type, config_table,
            )
            return TransitionResult(
                outcome=TransitionOutcome.invalid_transition,
                current_state=from_state,
            )
        allowed: list = config_row.allowed_next or []
        if to_state not in allowed:
            logger.warning(
                "Invalid transition %s->%s for %s (allowed: %s)",
                from_state, to_state, entity_type, allowed,
            )
            return TransitionResult(
                outcome=TransitionOutcome.invalid_transition,
                current_state=from_state,
            )

    # 2. CAS update on the owning entity table (if this entity type has one).
    if entity_type in _ENTITY_STATE_COLUMN:
        # table/pk_col/state_col come from the fixed _ENTITY_STATE_COLUMN dict
        # above — never caller/request-supplied.
        table, pk_col, state_col = _ENTITY_STATE_COLUMN[entity_type]
        result = session.execute(
            text(f"""
                UPDATE {table}
                SET {state_col} = :to_state, updated_at = NOW()
                WHERE {pk_col} = (
                    SELECT native_id::uuid
                    FROM fa_max_entity_registry
                    WHERE entity_uuid = :entity_uuid ::uuid
                )
                AND {state_col} = :from_state
            """),
            {"to_state": to_state, "from_state": from_state, "entity_uuid": entity_uuid},
        )
        if result.rowcount == 0:
            # Either already advanced past from_state or entity not found.
            logger.info(
                "CAS miss for %s %s: expected state %s not found",
                entity_type, entity_uuid, from_state,
            )
            return TransitionResult(
                outcome=TransitionOutcome.already_advanced,
                current_state=None,
            )

    # 3. Insert event row — idempotent via ON CONFLICT DO NOTHING.
    event_row = session.execute(
        text("""
            INSERT INTO fa_max_state_transition_events (
                entity_uuid, person_id, entity_type,
                from_state, to_state, actor, source_component,
                decision_id, context, idempotency_key, occurred_at
            ) VALUES (
                :entity_uuid ::uuid,
                :person_id ::uuid,
                :entity_type,
                :from_state, :to_state, :actor, :source_component,
                :decision_id,
                :context ::jsonb,
                :idempotency_key,
                NOW()
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING event_id::text, occurred_at
        """),
        {
            "entity_uuid": entity_uuid,
            "person_id": person_id,
            "entity_type": entity_type,
            "from_state": from_state,
            "to_state": to_state,
            "actor": actor,
            "source_component": source_component,
            "decision_id": decision_id,
            "context": _json.dumps(context),
            "idempotency_key": idempotency_key,
        },
    ).fetchone()

    if event_row is None:
        # ON CONFLICT triggered — idempotent skip.
        logger.info("Idempotent skip for idempotency_key=%s", idempotency_key)
        return TransitionResult(
            outcome=TransitionOutcome.idempotent_skip,
            current_state=to_state,
        )

    logger.info(
        "State transition: %s %s %s->%s by %s (event=%s)",
        entity_type, entity_uuid, from_state, to_state, actor, event_row.event_id,
    )
    return TransitionResult(
        outcome=TransitionOutcome.succeeded,
        current_state=to_state,
        event_id=event_row.event_id,
    )


def make_idempotency_key(
    entity_uuid: str, from_state: str, to_state: str, actor: str, epoch_minute: Optional[int] = None
) -> str:
    """Deterministic idempotency key for a transition.

    Callers that have an external event ID should pass it directly as
    idempotency_key instead of using this helper — external IDs are more
    stable. This helper exists for system-initiated transitions with no
    external ID.

    epoch_minute defaults to the current UTC minute, so retries within the
    same minute are idempotent and retries after 60s generate a fresh key
    (preventing stale-key collisions across genuinely distinct events).
    """
    import time
    if epoch_minute is None:
        epoch_minute = int(time.time()) // 60
    raw = f"{entity_uuid}:{from_state}:{to_state}:{actor}:{epoch_minute}"
    return hashlib.sha256(raw.encode()).hexdigest()[:64]
