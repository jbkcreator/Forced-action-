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
import uuid as _uuid_mod
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
    contended = "contended"  # another caller holds this entity's Redis lock —
    # returned WITHOUT ever attempting the Postgres advisory lock, so the
    # caller can back off/retry instead of queuing there. Only possible when
    # acquire_redis_lock=True; see _try_acquire_redis_lock's docstring.


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
    state_version: Optional[int] = None,
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
        One of: person, opportunity, partner.
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
    state_version:
        Expected current state_version integer. Required for all entity types
        that carry state_version (person, opportunity, partner). The CAS guard
        is: WHERE state_col = :from_state AND state_version = :expected. Pass
        the value returned by get_person_state() / get_opportunity_state().
        Legacy callers that omit it are upgraded safely: the engine loads the
        current version before applying the same state+version CAS.
    person_id:
        Ignored — person_id is now always derived server-side.
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
        Admin overrides require actor to contain 'admin' or be 'user:josh',
        and context must contain a 'reason' key explaining the exceptional jump.
    """
    if context is None:
        context = {}

    # Admin override validation: when validate_allowed_next=False the caller is
    # claiming an exceptional jump. Require an admin/josh actor and a 'reason'
    # in context so exceptional jumps are always attributed and explained.
    if not validate_allowed_next:
        actor_lower = actor.lower()
        is_admin_actor = "admin" in actor_lower or actor_lower == "user:josh"
        if not is_admin_actor or "reason" not in context:
            logger.error(
                "transition(): validate_allowed_next=False requires actor to contain "
                "'admin' or be 'user:josh' AND context['reason'] to be set — "
                "rejecting exceptional jump %s->%s for %s actor=%r",
                from_state, to_state, entity_type, actor,
            )
            return TransitionResult(
                outcome=TransitionOutcome.invalid_transition,
                current_state=from_state,
            )

    # Check Redis BEFORE attempting the Postgres advisory lock — this is
    # what makes Redis an actual contention-avoidance layer rather than a
    # no-op. A prior version acquired the (blocking) Postgres lock
    # unconditionally first, so a Redis "someone else holds this" result
    # never actually prevented queuing at pg_advisory_xact_lock — the one
    # thing Redis was supposed to avoid (code-review finding). "Contended"
    # (another caller holds the Redis lock) and "unavailable" (Redis is
    # down/erroring) are now distinguished: only "contended" backs off
    # early; "unavailable" still fails open and proceeds to Postgres, since
    # Postgres remains the correctness guarantee regardless of Redis.
    redis_lock_token: Optional[str] = None
    if acquire_redis_lock:
        lock_state, redis_lock_token = _try_acquire_redis_lock(entity_type, entity_uuid)
        if lock_state == _REDIS_CONTENDED:
            logger.info(
                "Redis lock contended for %s:%s — backing off before "
                "reaching the Postgres advisory lock",
                entity_type, entity_uuid,
            )
            return TransitionResult(outcome=TransitionOutcome.contended, current_state=None)

    _acquire_pg_advisory_lock(session, entity_uuid)

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
            state_version=state_version,
            person_id=person_id,
            decision_id=decision_id,
            context=context,
            validate_allowed_next=validate_allowed_next,
        )
    finally:
        if redis_lock_token is not None:
            _release_redis_lock(entity_type, entity_uuid, redis_lock_token)
        # Do not leave the database write gate enabled for unrelated SQL in
        # the caller-owned transaction.
        session.execute(text("SELECT set_config('fa_max.allow_state_write', 'off', true)"))


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
                p.state_version,
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


def get_opportunity_state(*, session: Session, opportunity_id: str) -> Optional[Dict[str, Any]]:
    """Load the current stage for an opportunity. Returns None if not found.

    Callers (e.g. admin_router._handle_relay_decision's fa_max_transition
    branch) must load current_stage here BEFORE calling transition() with
    entity_type='opportunity' — transition() is a CAS write keyed on the
    caller-supplied from_state, so a stale/guessed from_state produces a
    silent already_advanced outcome instead of the intended transition.

    Mirrors get_person_state's shape and the _node_load_profile
    abort-if-not-found pattern from src/agents/graphs/retention.py.

    loan_amount_cents/maturity_months are intentionally excluded from the
    returned dict — internal-only scenario fields, never surfaced outside
    the module that computes them (SOT.md: no pricing/term output).
    """
    row = session.execute(
        text("""
            SELECT
                o.opportunity_id::text,
                o.person_id::text,
                o.opportunity_type,
                o.current_stage,
                o.state_version,
                o.outcome,
                o.source,
                o.source_reference,
                o.created_at,
                o.updated_at
            FROM fa_max_opportunities o
            WHERE o.opportunity_id = :opportunity_id ::uuid
        """),
        {"opportunity_id": opportunity_id},
    ).fetchone()

    if row is None:
        return None

    return dict(row._mapping)


def get_person_history(
    *, session: Session, person_id: str, limit: int = 100, after_seq: Optional[int] = None
) -> Dict[str, Any]:
    """Return a page of ordered state-transition history for a person.

    Includes all entity types associated with this person (currently person
    and opportunity — property/partner/interaction are not yet supported by
    transition(), see _SUPPORTED_TRANSITION_ENTITY_TYPES) via the person_id
    partition key, which is derived server-side by _do_transition — never
    trusted from caller input — so every row here genuinely belongs to this
    borrower.

    Ordered by `seq`, not `occurred_at`: occurred_at uses NOW() (frozen at
    transaction start), so a transaction that waits at the advisory lock and
    commits later can carry an earlier occurred_at than one that started
    later — ORDER BY occurred_at alone can then contradict real transition
    order. seq is allocated at actual INSERT execution time (after the lock
    is held), so it reflects true execution order.

    Cursor pagination (code-review fix — a plain LIMIT with no cursor
    silently hid every event past the first `limit`, and hid them in the
    worst direction: since results are oldest-first, a high-volume borrower
    would NEVER see their MOST RECENT events, only their oldest):

        after_seq: fetch events with seq > after_seq (None = start from the
                   beginning, i.e. the oldest event).

    Returns {"events": [...], "has_more": bool, "next_cursor": Optional[int]}.
    A caller that needs the COMPLETE history (not just a page) must loop:
    call once with after_seq=None, then repeatedly with
    after_seq=result["next_cursor"] while result["has_more"] is True. This
    is what makes 'complete ordered history of a borrower can be queried'
    actually true for a borrower with more than `limit` events, not just
    for one under the limit.

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
                e.occurred_at,
                e.seq
            FROM fa_max_state_transition_events e
            WHERE e.person_id = :person_id ::uuid
              AND (:after_seq ::bigint IS NULL OR e.seq > :after_seq ::bigint)
            ORDER BY e.seq ASC
            LIMIT :fetch_limit
        """),
        {"person_id": person_id, "after_seq": after_seq, "fetch_limit": limit + 1},
    ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    events = [dict(r._mapping) for r in page]
    next_cursor = events[-1]["seq"] if (has_more and events) else None

    return {"events": events, "has_more": has_more, "next_cursor": next_cursor}


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


_REDIS_ACQUIRED = "acquired"
_REDIS_UNAVAILABLE = "unavailable"  # fail-open: caller proceeds to Postgres anyway
_REDIS_CONTENDED = "contended"      # another caller holds it: caller backs off


def _try_acquire_redis_lock(entity_type: str, entity_uuid: str) -> tuple[str, Optional[str]]:
    """Optimistic Redis contention-avoidance lock. Fails open on Redis unavail.

    Returns (state, token):
      (_REDIS_ACQUIRED, token)    — lock acquired; token is a unique
                                    ownership string for _release_redis_lock.
      (_REDIS_UNAVAILABLE, None) — Redis is down/erroring; caller proceeds
                                    to the Postgres advisory lock anyway
                                    (fail-open — correctness never depends
                                    on Redis).
      (_REDIS_CONTENDED, None)   — another caller currently holds this
                                    entity's lock; caller should back off
                                    WITHOUT attempting the Postgres lock —
                                    distinguishing this from UNAVAILABLE is
                                    what makes Redis an actual contention-
                                    avoidance layer instead of a no-op
                                    (code-review finding: previously both
                                    cases returned the same falsy value and
                                    the caller proceeded to Postgres either
                                    way, so Redis never avoided queuing
                                    there — the one thing it existed to do).
    """
    if not redis_available():
        return _REDIS_UNAVAILABLE, None
    key = f"{_REDIS_LOCK_PREFIX}{entity_type}:{entity_uuid}"
    token = str(_uuid_mod.uuid4())
    try:
        result = get_redis().set(key, token, nx=True, ex=_REDIS_LOCK_TTL)
        if result:
            return _REDIS_ACQUIRED, token
        return _REDIS_CONTENDED, None
    except Exception:
        logger.warning("Redis lock acquire failed for %s:%s — proceeding without", entity_type, entity_uuid)
        return _REDIS_UNAVAILABLE, None


def _release_redis_lock(entity_type: str, entity_uuid: str, token: str) -> None:
    """Release the lock ONLY if it still holds this caller's own token.

    A prior version used a shared value ("1") and unconditional DELETE: if
    this caller's TTL had already expired and a different caller acquired
    the same key in the meantime, releasing unconditionally would delete
    the NEW holder's lock instead of a no-op — the classic distributed-lock
    correctness bug (code-review finding). Verifying ownership first closes
    that window (a small GET-then-DELETE gap remains, which is acceptable:
    this lock is a contention-avoidance optimization only, never the
    correctness guarantee — pg_advisory_xact_lock is).
    """
    if not redis_available():
        return
    key = f"{_REDIS_LOCK_PREFIX}{entity_type}:{entity_uuid}"
    try:
        r = get_redis()
        current = r.get(key)
        if current is not None:
            current_str = current.decode() if isinstance(current, bytes) else current
            if current_str == token:
                r.delete(key)
    except Exception:
        logger.warning("Redis lock release failed for %s:%s — lock will expire naturally", entity_type, entity_uuid)


_ENTITY_STATE_COLUMN: Dict[str, tuple[str, str, str]] = {
    # entity_type -> (table, pk_column, state_column)
    "person": ("fa_max_persons", "person_id", "lifecycle_state"),
    "opportunity": ("fa_max_opportunities", "opportunity_id", "current_stage"),
    "partner": ("fa_max_partners", "partner_id", "status"),
}

_ENTITY_STAGE_CONFIG_TABLE: Dict[str, str] = {
    "person": "fa_max_person_lifecycle_stage_config",
    "opportunity": "fa_max_opportunity_stage_config",
    # partner uses inline validation (see _PARTNER_STATUS_TRANSITIONS below)
    # — no separate DB config table for a 3-value status set.
}

# Allowed partner status transitions — inline, no DB config table needed.
_PARTNER_STATUS_TRANSITIONS: Dict[str, list] = {
    "identified": ["active"],
    "active": ["inactive"],
    "inactive": ["active"],
}

# Supported entity types for transition() — property and interaction are NOT
# state machines; their transitions are rejected (code-review finding: a prior
# version silently wrote event rows with no state mutation for these types).
_SUPPORTED_TRANSITION_ENTITY_TYPES = frozenset({"person", "opportunity", "partner"})


def _derive_person_id(
    session: Session, *, entity_type: str, entity_uuid: str
) -> tuple[Optional[str], Optional[str]]:
    """Resolve (native_id, person_id) for an entity from the registry/owning
    table — never trust a caller-supplied person_id.

    A caller-supplied person_id was previously optional and unchecked: an
    omitted value silently dropped the event from get_person_history(), and
    an incorrect value could file the event under the WRONG borrower's
    history with no validation at all (code-review finding). Deriving it
    server-side from the entity's own registration makes both failure modes
    structurally impossible for the two supported entity types.

    Returns (None, None) if the entity isn't registered, or the opportunity
    row referenced by the registry doesn't exist.
    """
    registry_row = session.execute(
        text("""
            SELECT native_id::text AS native_id
            FROM fa_max_entity_registry
            WHERE entity_uuid = :entity_uuid ::uuid AND entity_type = :entity_type
        """),
        {"entity_uuid": entity_uuid, "entity_type": entity_type},
    ).fetchone()
    if registry_row is None:
        return None, None

    native_id = registry_row.native_id
    if entity_type == "person":
        return native_id, native_id

    if entity_type == "opportunity":
        opp_row = session.execute(
            text("SELECT person_id::text AS person_id FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"),
            {"oid": native_id},
        ).fetchone()
        if opp_row is None:
            return native_id, None
        return native_id, opp_row.person_id

    # entity_type == "partner"
    partner_row = session.execute(
        text("SELECT person_id::text AS person_id FROM fa_max_partners WHERE partner_id = :pid ::uuid"),
        {"pid": native_id},
    ).fetchone()
    if partner_row is None:
        return native_id, None
    return native_id, partner_row.person_id


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
    state_version: Optional[int],
    person_id: Optional[str],
    decision_id: Optional[str],
    context: Dict[str, Any],
    validate_allowed_next: bool,
) -> TransitionResult:
    import json as _json

    # Set the session-local GUC so the immutability trigger allows this write.
    # SET LOCAL scopes the change to this transaction — it is automatically
    # cleared on commit or rollback, so a caller cannot accidentally inherit
    # a previous session's 'on' value. Any UPDATE/DELETE on the event table
    # outside a transition() call (e.g., a direct psql edit) will be blocked
    # by the trigger because this GUC is never set outside this function.
    session.execute(text("SET LOCAL fa_max.allow_state_write = 'on'"))

    if entity_type not in _SUPPORTED_TRANSITION_ENTITY_TYPES:
        logger.error(
            "transition() rejected: entity_type=%r has no durable state table "
            "yet (WP-1 implements person and opportunity only) — refusing "
            "rather than silently writing a state-less event row",
            entity_type,
        )
        return TransitionResult(outcome=TransitionOutcome.invalid_transition, current_state=None)

    native_id, derived_person_id = _derive_person_id(
        session, entity_type=entity_type, entity_uuid=entity_uuid
    )
    if native_id is None:
        logger.error(
            "transition(): entity_uuid=%s not found in registry for type=%s",
            entity_uuid, entity_type,
        )
        return TransitionResult(outcome=TransitionOutcome.invalid_transition, current_state=None)
    # Ignore any caller-supplied person_id — the derived value is
    # authoritative. person_id kwarg is retained on the public signature for
    # backward compatibility but is no longer trusted.
    del person_id

    # 1. Validate allowed_next when required.
    if validate_allowed_next:
        if entity_type == "partner":
            # Inline validation for the 3-value partner status machine.
            allowed = _PARTNER_STATUS_TRANSITIONS.get(from_state, [])
            if to_state not in allowed:
                logger.warning(
                    "Invalid partner transition %s->%s (allowed: %s)",
                    from_state, to_state, allowed,
                )
                return TransitionResult(
                    outcome=TransitionOutcome.invalid_transition,
                    current_state=from_state,
                )
        elif entity_type in _ENTITY_STAGE_CONFIG_TABLE:
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
            allowed = config_row.allowed_next or []
            if to_state not in allowed:
                logger.warning(
                    "Invalid transition %s->%s for %s (allowed: %s)",
                    from_state, to_state, entity_type, allowed,
                )
                return TransitionResult(
                    outcome=TransitionOutcome.invalid_transition,
                    current_state=from_state,
                )

    table, pk_col, state_col = _ENTITY_STATE_COLUMN[entity_type]

    # Preserve compatibility with callers created before state_version while
    # still enforcing a three-column CAS. The common engine itself performs
    # the required durable-state load; new callers should pass the version
    # returned by get_*_state so stale work is rejected rather than refreshed.
    if state_version is None:
        state_version = session.execute(
            text(f"SELECT state_version FROM {table} WHERE {pk_col} = :native_id ::uuid"),
            {"native_id": native_id},
        ).scalar()
        if state_version is None:
            return TransitionResult(
                outcome=TransitionOutcome.invalid_transition,
                current_state=None,
            )

    # 2+3. Claim the idempotency key AND apply the CAS update atomically via
    # a SAVEPOINT — either both happen or neither does. Previously the CAS
    # UPDATE ran first and the idempotency-key conflict was checked only
    # after, so a retried/duplicate request whose from_state happened to
    # still match current state would silently mutate state a second time
    # and then report idempotent_skip, hiding the mutation with no new event
    # row to explain it (code-review finding — a genuine idempotency
    # violation of WP-1's own "a retried event must be a no-op" contract).
    savepoint = session.begin_nested()
    try:
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
                "person_id": derived_person_id,
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
            # ON CONFLICT triggered — a row with this idempotency_key already
            # exists. Verify it's actually the SAME operation before treating
            # it as a safe no-op: a caller that accidentally reuses a key
            # across two different operations must be told about it, not
            # silently skipped (code-review finding).
            savepoint.rollback()
            existing = session.execute(
                text("""
                    SELECT entity_uuid::text AS entity_uuid, from_state, to_state
                    FROM fa_max_state_transition_events
                    WHERE idempotency_key = :key
                """),
                {"key": idempotency_key},
            ).fetchone()
            current = session.execute(
                text(f"SELECT {state_col} FROM {table} WHERE {pk_col} = :native_id ::uuid"),
                {"native_id": native_id},
            ).scalar()
            if existing is not None and (
                existing.entity_uuid != entity_uuid
                or existing.from_state != from_state
                or existing.to_state != to_state
            ):
                logger.error(
                    "idempotency_key=%s reused for a DIFFERENT operation "
                    "(existing: entity=%s %s->%s; requested: entity=%s %s->%s) "
                    "— rejecting rather than silently skipping",
                    idempotency_key, existing.entity_uuid, existing.from_state, existing.to_state,
                    entity_uuid, from_state, to_state,
                )
                return TransitionResult(outcome=TransitionOutcome.invalid_transition, current_state=current)
            logger.info(
                "Idempotent skip for idempotency_key=%s — no state mutation", idempotency_key
            )
            return TransitionResult(outcome=TransitionOutcome.idempotent_skip, current_state=current)

        cas_sql = f"""
            UPDATE {table}
            SET {state_col} = :to_state,
                state_version = state_version + 1,
                updated_at = NOW()
            WHERE {pk_col} = :native_id ::uuid
              AND {state_col} = :from_state
              AND state_version = :expected_version
        """
        cas_params = {
            "to_state": to_state, "from_state": from_state,
            "native_id": native_id, "expected_version": state_version,
        }

        result = session.execute(text(cas_sql), cas_params)
        if result.rowcount == 0:
            # Already advanced past from_state, or entity not found — roll
            # back the event insert too, so no phantom event row records a
            # transition that never actually took effect.
            savepoint.rollback()
            logger.info(
                "CAS miss for %s %s: expected state %s not found",
                entity_type, entity_uuid, from_state,
            )
            current = session.execute(
                text(f"SELECT {state_col} FROM {table} WHERE {pk_col} = :native_id ::uuid"),
                {"native_id": native_id},
            ).scalar()
            return TransitionResult(outcome=TransitionOutcome.already_advanced, current_state=current)

        savepoint.commit()
    except Exception:
        savepoint.rollback()
        raise

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


# ---------------------------------------------------------------------------
# Interaction writer (write-once)
# ---------------------------------------------------------------------------

def write_interaction(
    *,
    session: Session,
    person_id: str,
    channel: str,
    direction: str,
    actor: str,
    approved_bool: Optional[bool] = None,
    autonomy_tier_at_time: Optional[str] = None,
    body_redacted: Optional[str] = None,
) -> str:
    """Append a single interaction record. Write-once — never use this to
    update or replace an existing interaction.

    Returns the new interaction_id as a string.

    Compliance: no body/PII content stored. body_redacted is for content-free
    summaries only (e.g., 'initial outreach email'). No rate/term/commitment
    content may appear in body_redacted.
    """
    row = session.execute(
        text("""
            INSERT INTO fa_max_interactions
                (person_id, channel, direction, actor,
                 approved_bool, autonomy_tier_at_time, body_redacted, occurred_at)
            VALUES
                (:person_id ::uuid, :channel, :direction, :actor,
                 :approved_bool, :autonomy_tier_at_time, :body_redacted, NOW())
            RETURNING interaction_id::text
        """),
        {
            "person_id": person_id,
            "channel": channel,
            "direction": direction,
            "actor": actor,
            "approved_bool": approved_bool,
            "autonomy_tier_at_time": autonomy_tier_at_time,
            "body_redacted": body_redacted,
        },
    ).fetchone()
    return row.interaction_id  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Property association writer (temporal, no FSM)
# ---------------------------------------------------------------------------

def write_property_association(
    *,
    session: Session,
    person_id: str,
    property_id: int,
    role: str = "subject",
    opportunity_id: Optional[str] = None,
    source: Optional[str] = None,
) -> int:
    """Open a new temporal association between a person and a property.

    Returns the new association id (bigint).

    Does not close an existing open association first — callers should call
    close_property_association() before writing a new association when the
    intent is to supersede rather than add.
    """
    row = session.execute(
        text("""
            INSERT INTO fa_max_property_associations
                (person_id, property_id, opportunity_id, role, source, valid_from)
            VALUES
                (:person_id ::uuid, :property_id, :opportunity_id ::uuid, :role, :source, NOW())
            RETURNING id
        """),
        {
            "person_id": person_id,
            "property_id": property_id,
            "opportunity_id": opportunity_id,
            "role": role,
            "source": source,
        },
    ).fetchone()
    return row.id  # type: ignore[union-attr]


def close_property_association(
    *,
    session: Session,
    association_id: int,
) -> bool:
    """Close an open property association by setting valid_to = NOW().

    Returns True if a row was closed, False if already closed or not found.
    """
    result = session.execute(
        text("""
            UPDATE fa_max_property_associations
            SET valid_to = NOW()
            WHERE id = :id AND valid_to IS NULL
        """),
        {"id": association_id},
    )
    return result.rowcount > 0


# ---------------------------------------------------------------------------
# Unified cursor-paginated borrower timeline
# ---------------------------------------------------------------------------

def get_borrower_timeline(
    *,
    session: Session,
    person_id: str,
    limit: int = 100,
    after_seq: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a cursor-paginated unified timeline for a borrower.

    Merges three event sources across a single consistent seq ordering:
      - state_transition_events  (event_kind='state_transition')
      - interactions             (event_kind='interaction')
      - property_association opens/closes (event_kind='property_association')

    Each item has a unified shape:
      event_kind, id (source-table PK), person_id, actor, occurred_at,
      seq (for cursor pagination), and kind-specific extra fields.

    All three sources receive `global_seq` from the shared
    fa_max_timeline_seq sequence, so the cursor is unique and monotonic across
    event kinds. occurred_at remains the human-readable event timestamp.

    Cursor parameter: after_seq operates on the global union seq, not
    per-source seq — pass next_cursor from a prior call.

    Returns {"events": [...], "has_more": bool, "next_cursor": Optional[int]}.
    """
    rows = session.execute(
        text("""
            WITH unified AS (
                -- State transitions
                SELECT
                    'state_transition'::text AS event_kind,
                    e.event_id::text         AS id,
                    e.person_id::text        AS person_id,
                    e.actor,
                    e.occurred_at,
                    e.timeline_seq           AS global_seq,
                    jsonb_build_object(
                        'entity_type',   e.entity_type,
                        'entity_uuid',   e.entity_uuid::text,
                        'from_state',    e.from_state,
                        'to_state',      e.to_state,
                        'source_component', e.source_component
                    )                        AS extra
                FROM fa_max_state_transition_events e
                WHERE e.person_id = :person_id ::uuid

                UNION ALL

                -- Interactions
                SELECT
                    'interaction'::text      AS event_kind,
                    i.interaction_id::text   AS id,
                    i.person_id::text        AS person_id,
                    i.actor,
                    i.occurred_at,
                    i.timeline_seq           AS global_seq,
                    jsonb_build_object(
                        'channel',     i.channel,
                        'direction',   i.direction,
                        'approved_bool', i.approved_bool,
                        'autonomy_tier', i.autonomy_tier_at_time
                    )                        AS extra
                FROM fa_max_interactions i
                WHERE i.person_id = :person_id ::uuid

                UNION ALL

                -- Property association events (open and close)
                SELECT
                    'property_association'::text AS event_kind,
                    pa.id::text                  AS id,
                    pa.person_id::text           AS person_id,
                    COALESCE(pa.source, 'system:data_loader') AS actor,
                    pa.created_at                AS occurred_at,
                    pa.timeline_seq              AS global_seq,
                    jsonb_build_object(
                        'property_id',    pa.property_id,
                        'role',           pa.role,
                        'valid_from',     pa.valid_from,
                        'valid_to',       pa.valid_to,
                        'opportunity_id', pa.opportunity_id::text
                    )                            AS extra
                FROM fa_max_property_associations pa
                WHERE pa.person_id = :person_id ::uuid
            )
            SELECT *
            FROM unified
            WHERE (:after_seq ::bigint IS NULL OR global_seq > :after_seq ::bigint)
            ORDER BY global_seq ASC
            LIMIT :fetch_limit
        """),
        {"person_id": person_id, "after_seq": after_seq, "fetch_limit": limit + 1},
    ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    events = []
    for r in page:
        row_dict = dict(r._mapping)
        # Deserialize JSONB extra field if returned as string
        import json as _json
        extra = row_dict.pop("extra", {})
        if isinstance(extra, str):
            extra = _json.loads(extra)
        row_dict["extra"] = extra
        events.append(row_dict)

    next_cursor = events[-1]["global_seq"] if (has_more and events) else None
    return {"events": events, "has_more": has_more, "next_cursor": next_cursor}


# ---------------------------------------------------------------------------
# Durable work queue
# ---------------------------------------------------------------------------

def enqueue_work_item(
    *,
    session: Session,
    queue_name: str,
    payload: Dict[str, Any],
    idempotency_key: Optional[str] = None,
    person_id: Optional[str] = None,
    available_at: Optional[str] = None,
) -> Optional[str]:
    """Enqueue a new work item. Returns work_item_id, or None on idempotent skip.

    available_at: ISO timestamp string for deferred execution. None = NOW().
    """
    import json as _json
    row = session.execute(
        text("""
            INSERT INTO fa_max_work_queue
                (person_id, queue_name, payload, idempotency_key, available_at)
            VALUES
                (:person_id ::uuid,
                 :queue_name,
                 :payload ::jsonb,
                 :idempotency_key,
                 COALESCE(:available_at ::timestamptz, NOW()))
            ON CONFLICT (idempotency_key)
                WHERE idempotency_key IS NOT NULL
                DO NOTHING
            RETURNING work_item_id::text
        """),
        {
            "person_id": person_id,
            "queue_name": queue_name,
            "payload": _json.dumps(payload),
            "idempotency_key": idempotency_key,
            "available_at": available_at,
        },
    ).fetchone()
    return row.work_item_id if row else None  # type: ignore[union-attr]


def claim_next_work_item(
    *,
    session: Session,
    queue_name: str,
    worker_id: str,
    lease_seconds: int = 300,
) -> Optional[Dict[str, Any]]:
    """Claim the next available work item for this worker.

    Uses FOR UPDATE SKIP LOCKED so concurrent workers compete without
    blocking each other. Returns the claimed row dict, or None if the queue
    is empty.

    The caller must commit after completing the work and calling
    complete_work_item() — or simply let the lease expire (reclaim_expired_work_items
    will return the item to the pool after lease_seconds).
    """
    row = session.execute(
        text("""
            UPDATE fa_max_work_queue
            SET status           = 'claimed',
                claimed_at       = NOW(),
                lease_expires_at = NOW() + (:lease_seconds * INTERVAL '1 second'),
                worker_id        = :worker_id,
                attempt_count    = attempt_count + 1,
                updated_at       = NOW()
            WHERE work_item_id = (
                SELECT work_item_id
                FROM fa_max_work_queue
                WHERE queue_name = :queue_name
                  AND status     = 'available'
                  AND available_at <= NOW()
                ORDER BY available_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                work_item_id::text,
                person_id::text,
                queue_name,
                payload,
                status,
                idempotency_key,
                attempt_count,
                lease_expires_at,
                worker_id
        """),
        {"queue_name": queue_name, "worker_id": worker_id, "lease_seconds": lease_seconds},
    ).fetchone()
    return dict(row._mapping) if row else None


def complete_work_item(
    *,
    session: Session,
    work_item_id: str,
    worker_id: str,
    status: str = "done",
) -> bool:
    """Mark a claimed work item as done (or failed). Returns True if updated.

    status must be 'done' or 'failed'. Only the owning worker_id may complete
    an item — this prevents a reclaimed item from being double-completed.
    """
    if status not in ("done", "failed"):
        raise ValueError(f"status must be 'done' or 'failed', got {status!r}")
    result = session.execute(
        text("""
            UPDATE fa_max_work_queue
            SET status     = :status,
                done_at    = NOW(),
                updated_at = NOW()
            WHERE work_item_id = :work_item_id ::uuid
              AND worker_id    = :worker_id
              AND status       = 'claimed'
        """),
        {"work_item_id": work_item_id, "worker_id": worker_id, "status": status},
    )
    return result.rowcount > 0


def reclaim_expired_work_items(
    *,
    session: Session,
    queue_name: Optional[str] = None,
) -> int:
    """Return expired claimed items back to 'available'. Returns count reclaimed.

    Called by a recovery monitor (or the worker on startup) to re-offer items
    whose workers died or timed out. Reclaim also increments attempt_count so
    repeated lease failures remain visible to permanent-failure routing.
    """
    result = session.execute(
        text("""
            UPDATE fa_max_work_queue
            SET status           = 'available',
                claimed_at       = NULL,
                lease_expires_at = NULL,
                worker_id        = NULL,
                attempt_count    = attempt_count + 1,
                updated_at       = NOW()
            WHERE status           = 'claimed'
              AND lease_expires_at < NOW()
              AND (:queue_name IS NULL OR queue_name = :queue_name)
        """),
        {"queue_name": queue_name},
    )
    count = result.rowcount
    if count:
        logger.info("reclaim_expired_work_items: returned %d items to available", count)
    return count
