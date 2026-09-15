"""
Relay approval queue — the single read/write seam for relay_approval_queue
(RELAY-v2.2 sub-task R1).

This module IS "Josh's queue" (build spec §1.1.13): enqueue() is the one
entry point Cora (Phase 2, not yet built) will call to write a proposed
action as a 'pending' row; R1's --seed CLI (src/services/relay/__main__.py)
calls this exact function today to build/prove the engine — identical
schema and call shape, zero change when Cora lands. record_decision() is
called by the Slack decision webhook (src/api/admin_router.py). The
execution engine (src/services/relay/engine.py) claims rows via
try_claim_for_batch() and reports outcomes via mark_sent/mark_failed/
mark_skipped.

Per CLAUDE.md: all data retrieval uses sqlalchemy.text(), never the ORM
query API. The RelayApprovalQueueItem ORM model (src.core.models) is used
only for the single-row INSERT in enqueue().

THE BATCH-INTAKE CONTRACT (RELAY-v2.2 sub-task R4). enqueue() is the one and
only way a new action enters Relay -- this is that contract, made explicit
rather than left implicit in the function signature alone:

    Required:
        idempotency_key: str  -- globally unique. A retry of the same
                                  proposed action must reuse the same key;
                                  enqueue() is itself idempotent on this --
                                  a duplicate key returns the existing row,
                                  never a duplicate insert (see below).
        channel: str           -- must have a registered dispatcher
                                  (src.services.relay.channels.DISPATCHERS)
                                  by the time the row is approved, or
                                  execution fails with
                                  'unknown_channel:{channel}'.
        recipient: str         -- email address today (or E.164 phone once
                                  an sms/voice channel registers).
        payload: dict           -- channel-specific. For channel='email':
                                  {"subject": str, "body": str} -- both
                                  read by
                                  src.services.relay.channels_email.send_email().

    Optional:
        thread_id: str | None  -- Opportunity Thread ID (OPP-YYYY-#####).
                                  Nullable in Phase 1 -- no Hunter yet to
                                  mint one (dev split §6b). Stamped onto the
                                  completion receipt unchanged if supplied.

THE COMPLETION RECEIPT is the same row, read back after execution
(RelayApprovalQueueItem.status/dispatched_at/channel/thread_id -- no
separate receipt table):
    status == 'sent'   -- the only success state.
    dispatched_at      -- sent timestamp, set only by mark_sent().
    channel, thread_id -- unchanged from intake.
'approved' is not complete; 'sent' with a populated dispatched_at is.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from config.venture_template import DEFAULT_VENTURE_KEY
from src.core.database import get_db_context
from src.core.models import RelayApprovalQueueItem
from src.services.relay.config import (
    STATUS_APPROVED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_SENT,
    STATUS_SKIPPED,
)


@dataclass
class QueueItem:
    """Read-only view of one relay_approval_queue row."""
    id: int
    idempotency_key: str
    batch_id: Optional[str]
    thread_id: Optional[str]
    channel: str
    recipient: str
    payload: dict
    status: str
    slack_message_ts: Optional[str]
    decided_by: Optional[str]
    decided_at: Optional[datetime]
    error: Optional[str]
    dispatched_at: Optional[datetime]
    created_at: datetime
    # Which venture proposed this action (CLONE-v2.2 / CL3). Defaulted, and
    # last in the field order, so every existing construction of QueueItem
    # keeps working unchanged; _COLUMNS_SQL below picks the column up
    # automatically from the dataclass fields.
    venture_key: str = DEFAULT_VENTURE_KEY
    # FA Max WP-2: operating lane, acting agent, autonomy tier. All three are
    # nullable so non-FA-Max items (venture_key != 'fa_max_lending') are
    # unaffected; their columns return NULL and the defaults below keep every
    # existing call site working with zero changes.
    lane: Optional[str] = None            # MONEY | EXCEPTIONS | RELATIONSHIPS
    agent_name: Optional[str] = None      # e.g. "vera", "hunter", "cora"
    autonomy_tier_at_send: Optional[str] = None  # A | B | C
    person_id: Optional[str] = None
    autonomy_gate_reason: Optional[str] = None
    decision_interaction_id: Optional[str] = None
    send_interaction_id: Optional[str] = None


_QUEUE_ITEM_COLUMNS = tuple(f.name for f in fields(QueueItem))
_COLUMNS_SQL = ", ".join(_QUEUE_ITEM_COLUMNS)


def _row_to_item(row: dict) -> QueueItem:
    # Old/fake rows used by non-FA-Max callers may predate optional WP-2
    # columns. Dataclass defaults preserve that compatibility.
    return QueueItem(**{col: row[col] for col in _QUEUE_ITEM_COLUMNS if col in row})


def enqueue(
    *,
    idempotency_key: str,
    channel: str,
    recipient: str,
    payload: dict,
    thread_id: Optional[str] = None,
    venture_key: str = DEFAULT_VENTURE_KEY,
    lane: Optional[str] = None,
    agent_name: Optional[str] = None,
    autonomy_tier_at_send: Optional[str] = None,
    person_id: Optional[str] = None,
    skip_contract_validation: bool = False,
) -> QueueItem:
    """Write a new 'pending' row. Called by Cora/THROUGH (Phase 2) and by
    R1's --seed CLI today.

    QUALITY-v2.2 Q3: every call is validated against
    src.agents.contracts.cora_to_relay.CoraRelayHandoff before anything is
    written -- an incomplete handoff (missing thread_id, an unregistered
    channel, empty subject/body) is rejected here instead of landing
    'pending' and only failing at dispatch time (the exact gap R4's audit
    found). skip_contract_validation exists ONLY for R1's --seed CLI, an
    explicit founder/dev manual-testing tool that has always allowed
    thread_id to be omitted -- every other caller is validated by default.

    If idempotency_key already exists (e.g. a caller retries the same
    proposed action), returns the existing row instead of raising or
    creating a duplicate.

    `venture_key` decides which Slack channel this is posted to, which
    Instantly campaign it sends through, and whose daily ceiling it counts
    against — see src/utils/venture_config.py.
    """
    if not skip_contract_validation:
        from pydantic import ValidationError

        from src.agents.contracts.cora_to_relay import reject_handoff, validate_handoff

        try:
            validate_handoff(
                idempotency_key=idempotency_key, channel=channel, recipient=recipient,
                payload=payload, thread_id=thread_id,
            )
        except ValidationError as exc:
            errors = [f"{'.'.join(str(p) for p in e['loc']) or '<handoff>'}: {e['msg']}" for e in exc.errors()]
            with get_db_context() as reject_session:
                rejected = reject_handoff(
                    reject_session, idempotency_key=idempotency_key,
                    errors=errors, payload_snapshot=payload or {},
                )
            raise rejected from exc

    try:
        with get_db_context() as session:
            gate_reason = None
            if venture_key == "fa_max_lending":
                from src.services.fa_max_autonomy import check_tier_gate
                from src.services.fa_max_send_governance import (
                    GovernanceBlocked,
                    LANES,
                    require_consent,
                    suppression_reason,
                    validate_safe_payload,
                )

                missing = [name for name, value in (
                    ("lane", lane), ("agent_name", agent_name),
                    ("autonomy_tier_at_send", autonomy_tier_at_send),
                    ("person_id", person_id),
                ) if not value]
                if missing:
                    raise GovernanceBlocked("missing_governance_fields:" + ",".join(missing))
                if lane not in LANES:
                    raise GovernanceBlocked(f"invalid_lane:{lane}")
                validate_safe_payload(payload)
                consent = require_consent(
                    session, person_id=str(person_id), channel=channel,
                )
                if not consent.allowed:
                    raise GovernanceBlocked(consent.reason)
                suppressed = suppression_reason(
                    session, recipient=recipient, channel=channel,
                )
                if suppressed:
                    raise GovernanceBlocked(f"suppressed:{suppressed}")
                gate = check_tier_gate(str(agent_name), str(autonomy_tier_at_send), session)
                gate_reason = gate.outcome.value
                if not gate.allowed:
                    raise GovernanceBlocked(f"autonomy_gate:{gate_reason}")

            item = RelayApprovalQueueItem(
                idempotency_key=idempotency_key,
                channel=channel,
                recipient=recipient,
                payload=payload,
                thread_id=thread_id,
                status=STATUS_PENDING,
                venture_key=venture_key,
                lane=lane,
                agent_name=agent_name,
                autonomy_tier_at_send=autonomy_tier_at_send,
                person_id=person_id,
                autonomy_gate_reason=gate_reason,
            )
            session.add(item)
            session.flush()
            item_id = item.id
    except IntegrityError:
        existing = get_item_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing
        raise
    item = get_item(item_id)
    assert item is not None  # just inserted in the same call
    return item


def get_item(item_id: int) -> Optional[QueueItem]:
    """Fetch one row by id, or None."""
    with get_db_context() as session:
        row = session.execute(
            text(f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue WHERE id = :id"),
            {"id": item_id},
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def get_item_by_idempotency_key(idempotency_key: str) -> Optional[QueueItem]:
    """Fetch one row by its idempotency key, or None."""
    with get_db_context() as session:
        row = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                "WHERE idempotency_key = :key"
            ),
            {"key": idempotency_key},
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def set_slack_message_ts(item_id: int, slack_message_ts: str) -> None:
    """Record the posted Slack message's ts so the decision webhook can
    edit that message in place once Josh responds."""
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET slack_message_ts = :ts, "
                "updated_at = now() WHERE id = :id"
            ),
            {"ts": slack_message_ts, "id": item_id},
        )


def get_item_for_update(item_id: int, *, session) -> Optional[QueueItem]:
    """Lock and return one queue row inside the caller's transaction."""
    row = session.execute(
        text(f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue WHERE id = :id FOR UPDATE"),
        {"id": item_id},
    ).mappings().first()
    return _row_to_item(dict(row)) if row else None


def record_decision(
    item_id: int, *, approved: bool, decided_by: str, session=None,
) -> Optional[QueueItem]:
    """Flip a 'pending' row to approved/rejected. Called by the Slack
    decision webhook.

    Returns None (no-op) if the row was not 'pending' at the moment of
    the update — guards a double button-press or a stale/duplicate Slack
    retry from re-deciding an already-decided row.
    """
    new_status = STATUS_APPROVED if approved else STATUS_REJECTED
    def apply(decision_session) -> Optional[QueueItem]:
        row = decision_session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET status = :new_status, decided_by = :decided_by, "
                "    decided_at = now(), updated_at = now() "
                "WHERE id = :id AND status = :pending "
                f"RETURNING {_COLUMNS_SQL}"
            ),
            {
                "new_status": new_status,
                "decided_by": decided_by,
                "id": item_id,
                "pending": STATUS_PENDING,
            },
        ).mappings().first()
        if row is None:
            return None
        values = dict(row)
        if values.get("venture_key") == "fa_max_lending" and values.get("person_id"):
            from src.services.state_engine import write_interaction

            interaction_id = write_interaction(
                session=decision_session,
                person_id=str(values["person_id"]),
                channel="slack",
                direction="inbound",
                actor=f"slack_approver:{decided_by}",
                approved_bool=approved,
                autonomy_tier_at_time=values.get("autonomy_tier_at_send"),
                body_redacted="relay action approved" if approved else "relay action rejected",
            )
            decision_session.execute(
                text("UPDATE relay_approval_queue SET decision_interaction_id = CAST(:interaction_id AS uuid) "
                     "WHERE id = :id"),
                {"interaction_id": interaction_id, "id": item_id},
            )
            values["decision_interaction_id"] = interaction_id
        return _row_to_item(values)

    if session is not None:
        return apply(session)
    with get_db_context() as owned_session:
        return apply(owned_session)


def approved_batch(limit: int = 50, *, venture_key: Optional[str] = None) -> list[QueueItem]:
    """All status='approved' rows, oldest first — what the cron sweep
    hands to the execution engine.

    `venture_key=None` returns every venture's rows (the pre-CL3 behavior,
    kept so any caller that does not care about ventures is unaffected).
    Pass a key to restrict the batch to one venture, which the sweep does —
    a batch must be homogeneous, since one resolved VentureConfig governs
    the send window, ceiling and channel for every item in it.
    """
    where = "status = :status"
    params: dict = {"status": STATUS_APPROVED, "limit": limit}
    if venture_key is not None:
        where += " AND venture_key = :venture_key"
        params["venture_key"] = venture_key

    with get_db_context() as session:
        rows = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                f"WHERE {where} ORDER BY created_at ASC LIMIT :limit"
            ),
            params,
        ).mappings().all()
        return [_row_to_item(dict(r)) for r in rows]


def try_claim_for_batch(item_id: int, batch_id: str, *, stale_after_minutes: int = 10) -> bool:
    """Atomically claim an 'approved' row for a batch run.

    Guards against a double sweep pickup — e.g. two overlapping cron runs
    both selecting the same approved row before either dispatches it — via
    the batch_id IS NULL half of the WHERE clause. The staleness half
    additionally allows reclaiming a row whose PREVIOUS claim never
    resolved to sent/failed (e.g. the claiming process crashed between
    the claim and mark_sent/mark_failed) — without it, that row would be
    permanently stuck in 'approved' with a non-null batch_id, since no
    future sweep could ever claim it again. stale_after_minutes must stay
    well above any single dispatch's realistic duration.

    Returns True if this call claimed the row, False if it was already
    claimed by another still-live run (or moved out of 'approved').
    """
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET batch_id = :batch_id, updated_at = now() "
                "WHERE id = :id AND status = :approved "
                "AND (batch_id IS NULL OR updated_at < now() - make_interval(mins => :stale_after))"
            ),
            {
                "batch_id": batch_id, "id": item_id, "approved": STATUS_APPROVED,
                "stale_after": stale_after_minutes,
            },
        )
        return result.rowcount > 0


def sent_counts_today(now: datetime, *, timezone_name: str) -> dict[str, int]:
    """Per-channel count of rows already 'sent' since midnight in
    `timezone_name`. Reporting only -- NOT the daily-ceiling enforcement
    mechanism (see PR #179 review finding #2: a per-batch local snapshot
    like this one cannot enforce a cap across concurrent workers, since
    two overlapping sweeps would each read the same snapshot and neither
    would see the other's in-flight sends). The real-time ceiling gate is
    src.services.relay.guards.reserve_daily_slot(), an atomic Redis
    counter. This function remains for dashboards/audits that want the
    actual historical sent count."""
    from zoneinfo import ZoneInfo

    local_midnight = now.astimezone(ZoneInfo(timezone_name)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    day_start_utc = local_midnight.astimezone(timezone.utc)
    with get_db_context() as session:
        rows = session.execute(
            text(
                "SELECT channel, count(*) AS n FROM relay_approval_queue "
                "WHERE status = :status AND dispatched_at >= :day_start "
                "GROUP BY channel"
            ),
            {"status": STATUS_SENT, "day_start": day_start_utc},
        ).mappings().all()
        return {r["channel"]: r["n"] for r in rows}


def mark_sent(item_id: int, *, batch_id: str) -> None:
    """Transitions a claimed row to 'sent' -- guarded on both
    'status = approved' AND 'batch_id = <this worker's batch_id>' (PR #179
    review finding #1). The status guard alone isn't sufficient: if a
    dispatch runs long enough to outlive try_claim_for_batch's staleness
    window, a DIFFERENT worker can legitimately reclaim the same row as
    stale and dispatch it again while the first dispatch is still in
    flight. Requiring the caller's own batch_id to still match means only
    whichever worker currently owns the row can finalize it -- the other
    worker's call simply no-ops (0 rows match) instead of silently
    overwriting a completed receipt."""
    with get_db_context() as session:
        row = session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "dispatched_at = now(), updated_at = now() "
                "WHERE id = :id AND status = :approved AND batch_id = :batch_id"
                " RETURNING venture_key, person_id::text, channel, agent_name, "
                "autonomy_tier_at_send, payload"
            ),
            {"status": STATUS_SENT, "id": item_id, "approved": STATUS_APPROVED, "batch_id": batch_id},
        ).mappings().first()
        if row and row["venture_key"] == "fa_max_lending" and row["person_id"]:
            from src.services.state_engine import write_interaction

            interaction_id = write_interaction(
                session=session,
                person_id=row["person_id"],
                channel=row["channel"],
                direction="outbound",
                actor=f"agent:{row['agent_name']}",
                approved_bool=not bool((row["payload"] or {}).get("edited_before_approval")),
                autonomy_tier_at_time=row["autonomy_tier_at_send"],
                body_redacted="relay outbound send",
            )
            session.execute(
                text("UPDATE relay_approval_queue SET send_interaction_id = CAST(:iid AS uuid) WHERE id = :id"),
                {"iid": interaction_id, "id": item_id},
            )


def mark_failed(item_id: int, error: str, *, batch_id: str) -> None:
    """Transitions a claimed row to 'failed' -- guarded identically to
    mark_sent(), same reasoning."""
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "error = :error, updated_at = now() "
                "WHERE id = :id AND status = :approved AND batch_id = :batch_id"
            ),
            {
                "status": STATUS_FAILED, "error": error, "id": item_id,
                "approved": STATUS_APPROVED, "batch_id": batch_id,
            },
        )


def mark_skipped(item_id: int, reason: str) -> None:
    """Transitions an 'approved' row to 'skipped' -- guarded so a row that
    has already reached a terminal state (sent/failed/skipped) can never be
    downgraded. Without this guard, calling mark_skipped() on a row an
    earlier run already sent (e.g. a crashed sweep re-picking up the same
    id, or engine.execute_batch's own "claim_lost_to_concurrent_run" path
    firing after the row already completed) silently corrupts the
    completion receipt from 'sent' back to 'skipped' -- found by RELAY-v2.2
    R4's real-Postgres forced-retry test, which the in-memory fake-backend
    idempotency test (test_relay_engine.py) could not catch, since that
    fake models 'sent' and 'skipped' as two independent dicts rather than
    one mutually-exclusive status column."""
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "error = :error, updated_at = now() "
                "WHERE id = :id AND status = :approved"
            ),
            {"status": STATUS_SKIPPED, "error": reason, "id": item_id, "approved": STATUS_APPROVED},
        )
