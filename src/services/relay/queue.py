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

import logging
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
    STATUS_UNCERTAIN,
)

logger = logging.getLogger(__name__)


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
    slack_post_attempted_at: Optional[datetime] = None
    slack_post_lease_until: Optional[datetime] = None
    # WP-T2-2: Snooze / Revise support. eligible_at is distinct from
    # fa_max_work_queue.available_at (a different table's deferred-execution
    # column) -- this one governs whether THIS approval-queue row is
    # currently postable/dispatchable.
    eligible_at: Optional[datetime] = None
    original_draft: Optional[str] = None
    final_content: Optional[str] = None
    revision_count: int = 0
    last_revised_by: Optional[str] = None
    last_revised_at: Optional[datetime] = None
    material_edit: Optional[bool] = None


_QUEUE_ITEM_COLUMNS = tuple(f.name for f in fields(QueueItem))
_COLUMNS_SQL = ", ".join(_QUEUE_ITEM_COLUMNS)


def _row_to_item(row: dict) -> QueueItem:
    # Old/fake rows used by non-FA-Max callers may predate optional WP-2
    # columns. Dataclass defaults preserve that compatibility.
    return QueueItem(**{col: row[col] for col in _QUEUE_ITEM_COLUMNS if col in row})


def _mask_recipient(recipient: Optional[str]) -> str:
    """Minimal recipient masking for a Slack alert body — CLAUDE.md: never
    log raw PII, only IDs/masked representations. Keeps just enough of the
    tail to let Josh recognize which draft this is without exposing the
    full email/phone in a channel."""
    if not recipient:
        return "<none>"
    return f"...{recipient[-4:]}" if len(recipient) > 4 else "***"


def _alert_pre_enqueue_governance_refusal(
    *, reason: str, idempotency_key: str, recipient: Optional[str],
    agent_name: Optional[str], lane: Optional[str], autonomy_tier_at_send: Optional[str],
) -> None:
    """Surface a GovernanceBlocked refusal that happened BEFORE any
    relay_approval_queue row exists (WP-T2-2 review fix).

    SOT.md's "a blocked send is logged with a reason and surfaced to Josh"
    was previously only true for a block that happens AFTER an item exists
    (Relay's own dispatch-time block posts a Slack notice on that row —
    see slack_post.py). A refusal raised HERE, inside enqueue() itself —
    missing governance fields, an invalid lane, an unsupported channel,
    withdrawn/absent consent, suppression, an unknown tier, or a send
    attempt whose claim already expired — creates no row at all, so that
    path never fires. The agent loop still logs the refusal into
    fa_max_tool_call_log (status='blocked') either way — that satisfies
    "logged with a reason" — but nothing was surfacing it to Josh.

    Reuses the SAME durable, crash-safe EXCEPTIONS-lane delivery pattern
    src.tasks.fa_max_send_health_monitor and
    src.tasks.fa_max_weekly_edit_rate_report already use
    (exceptions_alert_queue.enqueue_and_attempt) rather than a second,
    ad-hoc Slack-posting path — the alert is committed durably before Slack
    is contacted, so a Slack outage or crash mid-attempt leaves it
    recoverable by the existing drain worker, not silently lost.

    rule is scoped to the reason's category (text before the first ':',
    e.g. 'suppressed' from 'suppressed:email_opt_out') rather than the
    full reason string, so a genuinely new failure MODE always alerts while
    repeated instances of the SAME misconfiguration within the dedup window
    don't produce a Slack flood — the full reason and recipient still
    appear in the message body every time.

    Never raises — an alerting failure must not turn a governance refusal
    into an unhandled exception that masks the refusal itself; the caller
    (enqueue()) re-raises the original GovernanceBlocked regardless of
    whether this alert succeeds.
    """
    try:
        from src.services.relay import exceptions_alert_queue

        reason_category = reason.split(":", 1)[0]
        message = (
            "*FA Max send blocked before it reached the approval queue*\n"
            f"  • reason: `{reason}`\n"
            f"  • idempotency_key: `{idempotency_key}`\n"
            f"  • recipient: `{_mask_recipient(recipient)}`\n"
            f"  • agent: `{agent_name or '<none>'}`  ·  lane: `{lane or '<none>'}`  ·  "
            f"tier: `{autonomy_tier_at_send or '<none>'}`"
        )
        exceptions_alert_queue.enqueue_and_attempt(
            venture_key="fa_max_lending",
            rule=f"fa_max_pre_enqueue_governance_refusal:{reason_category}",
            message=message,
        )
    except Exception:
        logger.warning(
            "[Relay] failed to alert on pre-enqueue governance refusal "
            "idempotency_key=%s reason=%s", idempotency_key, reason, exc_info=True,
        )


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
    auto_authorize: bool = False,
    send_attempt_log_id: Optional[int] = None,
) -> QueueItem:
    """Write a new 'pending' row. Called by Cora/THROUGH (Phase 2) and by
    R1's --seed CLI today.

    auto_authorize (WP-T2-2): when True, and venture_key == 'fa_max_lending',
    after the row insert -- in the SAME transaction, before commit -- this
    re-verifies check_tier_gate() and suppression_reason() FRESH (never
    trusting any prior caller-side check, since the caller's check may be
    stale by the time this write lands). If BOTH pass, this writes a
    system-actor authorizing interaction (write_interaction with
    actor='system:autonomous') and sets the row's decision_interaction_id,
    decided_by=f'system:autonomous:{tier}', and status='approved' -- all
    before commit, so the row is already terminal-decided when the caller's
    transaction lands. If either check fails, the row is left 'pending' (the
    normal human-approval path) -- auto_authorize never blocks the write,
    it only ever skips the human decision step when both gates are green.

    auto_authorize=True does NOT bypass the tier-gate / suppression checks
    already run earlier in this function for every fa_max_lending item
    (missing-field / lane / channel / consent / payload-safety / initial
    suppression) -- it is an ADDITIONAL, fresh re-check immediately before
    the row would be marked approved, closing the window between the
    caller's own check and this write actually committing.

    post_for_approval() (called below, unconditionally, for every
    fa_max_lending item) already no-ops for a non-'pending' item -- so a
    row auto-authorized to 'approved' here is correctly never posted to
    Slack; only a row still 'pending' after this block gets a card.

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

    # Autonomous dispatch must originate from an audited agent-loop send
    # attempt. Direct Relay callers can still create a human-approval draft.
    if venture_key == "fa_max_lending" and auto_authorize and send_attempt_log_id is None:
        auto_authorize = False

    try:
        with get_db_context() as session:
            if send_attempt_log_id is not None:
                from config.agents import get_agents_settings
                from src.services.fa_max_send_governance import GovernanceBlocked

                # The send attempt and the queue insert share this transaction.
                # A thread that wakes after its deadline cannot enqueue merely
                # because it obtained a claim before the agent loop timed out.
                live = session.execute(text(
                    "SELECT 1 FROM fa_max_tool_call_log WHERE id = :log_id "
                    "AND tool_name = 'send' AND status = 'claimed' "
                    "AND input->>'idempotency_key' = :idempotency_key "
                    "AND agent_name = :agent_name "
                    "AND created_at + (:timeout_seconds * interval '1 second') > clock_timestamp() "
                    "FOR UPDATE"
                ), {"log_id": send_attempt_log_id,
                    "idempotency_key": idempotency_key, "agent_name": agent_name,
                    "timeout_seconds": get_agents_settings().fa_max_agent_tool_timeout_seconds}).scalar()
                if not live:
                    raise GovernanceBlocked("send_attempt_expired")
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
                if channel not in ("email", "sms"):
                    raise GovernanceBlocked(f"unsupported_fa_max_channel:{channel}")
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
                # A warm introduction can be a first contact. Recipient
                # context is checked before autonomous authorization below;
                # unverified A/B claims remain pending for human approval.
                gate = check_tier_gate(str(agent_name), str(autonomy_tier_at_send), session)
                gate_reason = gate.outcome.value
                # Pending items need a Slack human decision. Graduation gates
                # autonomous sends, not drafts Josh explicitly approves.
                if gate_reason == "unknown_tier":
                    raise GovernanceBlocked("unknown_autonomy_tier")

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

            if auto_authorize and venture_key == "fa_max_lending":
                from config.settings import get_settings as _get_fa_max_settings
                from src.services.fa_max_autonomy import check_tier_gate as _fresh_tier_gate
                from src.services.fa_max_send_governance import suppression_reason as _fresh_suppression_reason
                from src.services.fa_max_send_governance import autonomous_tier_context_verified
                from src.services.state_engine import write_interaction as _write_authorizing_interaction

                # Fail-closed gate (WP-T2-2 item 10): autonomous dispatch is
                # withheld entirely -- regardless of tier graduation --
                # until this flag is explicitly confirmed true. See
                # docs/constitutions/cora_autonomy_amendment_proposed.md;
                # the flag stays False until Josh signs off on that
                # amendment.
                if not _get_fa_max_settings().fa_max_autonomous_dispatch_confirmed:
                    logger.info(
                        "[Relay] auto_authorize requested for item %s but "
                        "fa_max_autonomous_dispatch_confirmed is False -- "
                        "leaving pending for human approval",
                        item_id,
                    )
                    fresh_gate = None
                    fresh_suppressed = "autonomous_dispatch_not_confirmed"
                else:
                    fresh_gate = _fresh_tier_gate(str(agent_name), str(autonomy_tier_at_send), session)
                    fresh_suppressed = _fresh_suppression_reason(
                        session, recipient=recipient, channel=channel,
                    )
                context_verified = autonomous_tier_context_verified(
                    session, person_id=str(person_id),
                    tier=str(autonomy_tier_at_send), thread_id=thread_id,
                )
                if fresh_gate is not None and fresh_gate.allowed and not fresh_suppressed and context_verified:
                    tier = str(autonomy_tier_at_send)
                    interaction_id = _write_authorizing_interaction(
                        session=session,
                        person_id=str(person_id),
                        channel=channel,
                        direction="outbound",
                        actor="system:autonomous",
                        approved_bool=True,
                        autonomy_tier_at_time=tier,
                        body_redacted="autonomous authorization (auto_authorize)",
                        agent_name=agent_name,
                    )
                    session.execute(
                        text(
                            "UPDATE relay_approval_queue SET "
                            "status = :status, decided_by = :decided_by, "
                            "decided_at = now(), "
                            "decision_interaction_id = CAST(:interaction_id AS uuid), "
                            "updated_at = now() "
                            "WHERE id = :id"
                        ),
                        {
                            "status": STATUS_APPROVED,
                            "decided_by": f"system:autonomous:{tier}",
                            "interaction_id": interaction_id,
                            "id": item_id,
                        },
                    )
                else:
                    logger.info(
                        "[Relay] auto_authorize declined for item %s: "
                        "tier_gate_allowed=%s suppression_reason=%s context_verified=%s -- leaving pending",
                        item_id, (fresh_gate.allowed if fresh_gate is not None else None), fresh_suppressed,
                        context_verified,
                    )
    except IntegrityError:
        existing = get_item_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing
        raise
    except Exception as exc:
        from src.services.fa_max_send_governance import GovernanceBlocked

        if venture_key == "fa_max_lending" and isinstance(exc, GovernanceBlocked):
            _alert_pre_enqueue_governance_refusal(
                reason=exc.reason, idempotency_key=idempotency_key,
                recipient=recipient, agent_name=agent_name, lane=lane,
                autonomy_tier_at_send=autonomy_tier_at_send,
            )
        raise
    item = get_item(item_id)
    assert item is not None  # just inserted in the same call
    if venture_key == "fa_max_lending":
        # The row is committed before contacting Slack. A failed post stays
        # pending and the posting sweep can retry it after a restart.
        from src.services.relay.slack_post import post_for_approval
        post_for_approval(item)
    return item


def unposted_fa_max_items(limit: int = 50) -> list[QueueItem]:
    """Pending FA Max cards eligible for a posting retry."""
    with get_db_context() as session:
        rows = session.execute(
            text(f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                 "WHERE venture_key = 'fa_max_lending' AND status = 'pending' "
                 "AND slack_message_ts IS NULL "
                 "AND (slack_post_lease_until IS NULL OR slack_post_lease_until < now()) "
                 "AND (eligible_at IS NULL OR eligible_at <= now()) "
                 "ORDER BY created_at LIMIT :limit"),
            {"limit": limit},
        ).mappings().all()
        return [_row_to_item(dict(row)) for row in rows]


def claim_slack_post(item_id: int) -> Optional[datetime]:
    """Lease one unposted card across concurrent posting workers."""
    with get_db_context() as session:
        return session.execute(
            text("UPDATE relay_approval_queue SET "
                 "slack_post_attempted_at = now(), "
                 "slack_post_lease_until = now() + interval '10 minutes', updated_at = now() "
                 "WHERE id = :id AND venture_key = 'fa_max_lending' "
                 "AND status = 'pending' AND slack_message_ts IS NULL "
                 "AND (slack_post_lease_until IS NULL OR slack_post_lease_until < now()) "
                 "RETURNING slack_post_lease_until"),
            {"id": item_id},
        ).scalar_one_or_none()


def release_slack_post(item_id: int, lease_until: datetime) -> None:
    with get_db_context() as session:
        session.execute(
            text("UPDATE relay_approval_queue SET slack_post_lease_until = NULL "
                 "WHERE id = :id AND slack_post_lease_until = :lease_until "
                 "AND slack_message_ts IS NULL"),
            {"id": item_id, "lease_until": lease_until},
        )


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


def get_item_by_slack_message_ts(slack_message_ts: str) -> Optional[QueueItem]:
    """Resolve the root Slack card for a thread reply.

    Slack identifies a thread by the root message's ``ts``.  Looking up that
    persisted value keeps thread actions on the same durable queue row as the
    button action; it never trusts an item id supplied in free-form text.
    """
    with get_db_context() as session:
        row = session.execute(
            text(
                f"SELECT {_COLUMNS_SQL} FROM relay_approval_queue "
                "WHERE slack_message_ts = :ts"
            ),
            {"ts": slack_message_ts},
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def set_slack_message_ts(
    item_id: int, slack_message_ts: str, *, lease_until: Optional[datetime] = None,
) -> None:
    """Record the posted Slack message's ts so the decision webhook can
    edit that message in place once Josh responds."""
    condition = " AND slack_post_lease_until = :lease_until" if lease_until is not None else ""
    params = {"ts": slack_message_ts, "id": item_id}
    if lease_until is not None:
        params["lease_until"] = lease_until
    with get_db_context() as session:
        session.execute(
            text("UPDATE relay_approval_queue SET slack_message_ts = :ts, "
                 "slack_post_lease_until = NULL, updated_at = now() "
                 "WHERE id = :id AND slack_message_ts IS NULL" + condition),
            params,
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
    expected_revision_count: Optional[int] = None,
) -> Optional[QueueItem]:
    """Flip a 'pending' row to approved/rejected. Called by the Slack
    decision webhook.

    Returns None (no-op) if the row was not 'pending' at the moment of
    the update — guards a double button-press or a stale/duplicate Slack
    retry from re-deciding an already-decided row.

    expected_revision_count (WP-T2-2 review fix): when given, folds the
    stale-card check into this SAME atomic UPDATE (AND revision_count =
    :expected_revision_count) instead of a separate, earlier, unlocked
    SELECT the way src.api.admin_router._handle_relay_decision used to do
    it. That earlier pattern (read revision_count, THEN decide whether to
    call this function) left a real gap: a Revise could commit in between
    the read and this write, and the earlier check would have already
    passed on now-stale data. Folding the comparison into the UPDATE's own
    WHERE clause makes "is this decision still acting on the revision_count
    the caller saw" and "is this row still pending" a single indivisible
    check-and-write — there is no window between them for a concurrent
    Revise to land in. A mismatch (moved before this UPDATE commits) makes
    this a no-op, same as any other stale/duplicate decision.
    """
    new_status = STATUS_APPROVED if approved else STATUS_REJECTED
    def apply(decision_session) -> Optional[QueueItem]:
        where = "id = :id AND status = :pending"
        params: dict = {
            "new_status": new_status,
            "decided_by": decided_by,
            "id": item_id,
            "pending": STATUS_PENDING,
        }
        if expected_revision_count is not None:
            where += " AND revision_count = :expected_revision_count"
            params["expected_revision_count"] = expected_revision_count
        row = decision_session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET status = :new_status, decided_by = :decided_by, "
                "    decided_at = now(), updated_at = now() "
                f"WHERE {where} "
                f"RETURNING {_COLUMNS_SQL}"
            ),
            params,
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
    where = "status = :status AND (eligible_at IS NULL OR eligible_at <= now())"
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
                "AND (batch_id IS NULL OR (venture_key <> 'fa_max_lending' "
                "AND updated_at < now() - make_interval(mins => :stale_after)))"
            ),
            {
                "batch_id": batch_id, "id": item_id, "approved": STATUS_APPROVED,
                "stale_after": stale_after_minutes,
            },
        )
        return result.rowcount > 0


def mark_uncertain_if_stale(item_id: int, *, stale_after_minutes: int = 10) -> bool:
    """Park a previously claimed FA Max send whose provider result is unknown."""
    with get_db_context() as session:
        result = session.execute(
            text("UPDATE relay_approval_queue SET status = :uncertain, "
                 "error = 'provider_result_uncertain', updated_at = now() "
                 "WHERE id = :id AND venture_key = 'fa_max_lending' "
                 "AND status = :approved AND batch_id IS NOT NULL "
                 "AND updated_at < now() - make_interval(mins => :stale_after)"),
            {"id": item_id, "uncertain": STATUS_UNCERTAIN,
             "approved": STATUS_APPROVED, "stale_after": stale_after_minutes},
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
                "autonomy_tier_at_send, payload, material_edit"
            ),
            {"status": STATUS_SENT, "id": item_id, "approved": STATUS_APPROVED, "batch_id": batch_id},
        ).mappings().first()
        if row and row["venture_key"] == "fa_max_lending" and row["person_id"]:
            from src.services.state_engine import write_interaction

            # material_edit (WP-T2-2 review fix), not the unwritten
            # payload->>'edited_before_approval' flag -- see get_edit_rate()'s
            # docstring for the same fix on the rate side of this evidence.
            interaction_id = write_interaction(
                session=session,
                person_id=row["person_id"],
                channel=row["channel"],
                direction="outbound",
                actor=f"agent:{row['agent_name']}",
                approved_bool=not bool(row["material_edit"]),
                autonomy_tier_at_time=row["autonomy_tier_at_send"],
                body_redacted="relay outbound send",
                agent_name=row["agent_name"],
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


def snooze_item(item_id: int, *, hours: float = 4.0) -> bool:
    """Set eligible_at = now() + `hours` on a pending FA Max card (Slack
    Snooze button, WP-T2-2). Only a still-'pending' row can be snoozed --
    a row already decided/dispatched has nothing left to defer.

    Returns True if a row was updated.
    """
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE relay_approval_queue "
                "SET eligible_at = now() + make_interval(hours => :hours), updated_at = now() "
                "WHERE id = :id AND status = :pending"
            ),
            {"id": item_id, "hours": hours, "pending": STATUS_PENDING},
        )
        return result.rowcount > 0


def capture_original_draft(item_id: int, *, draft: str) -> None:
    """Record the drafted content at first human-approval enqueue (WP-T2-2).

    Called once, when a non-auto-authorize item is first enqueued for
    approval -- never overwritten afterward (revisions mutate
    final_content, not original_draft).
    """
    with get_db_context() as session:
        session.execute(
            text(
                "UPDATE relay_approval_queue SET original_draft = :draft, updated_at = now() "
                "WHERE id = :id AND original_draft IS NULL"
            ),
            {"id": item_id, "draft": draft},
        )


def record_revision(
    item_id: int, *, final_content: str, revised_by: str, material_edit: bool,
) -> Optional[QueueItem]:
    """Apply a Slack Revise submission (WP-T2-2): set final_content,
    increment revision_count, stamp last_revised_by/at, store the computed
    material_edit flag. Only applies to a still-'pending' row -- a decided
    row's content is final.

    ALSO writes the revised text into payload->>'body' (WP-T2-2 review fix):
    a Revise submission previously updated final_content for display only --
    every dispatcher (channels_email.send_email, channels_sms.send_sms)
    reads item.payload["body"], which record_revision() never touched, so
    an approved revision silently sent the ORIGINAL draft. payload is the
    single field every channel dispatcher actually reads, so making it the
    write target (rather than teaching each dispatcher about final_content)
    keeps there being exactly one place a channel needs to look, matching
    the rest of this module's "one seam" design. final_content/original_draft
    remain the audit trail of what was drafted vs. revised; payload->>'body'
    is what actually goes out.

    material_edit stored here is whatever the caller computed -- see
    src.api.admin_router._handle_relay_revise_submission, which compares
    against original_draft (not the previous revision) and OR's it with
    the row's current material_edit, so a run of individually-small edits
    that add up to a large overall change is never diluted back to
    "not material" by comparing each edit only to its immediate predecessor.

    Returns the updated row, or None if the row was not pending (stale
    revise submission on an already-decided card).
    """
    import json as _json

    with get_db_context() as session:
        row = session.execute(
            text(
                "UPDATE relay_approval_queue SET "
                "final_content = :final_content, "
                "payload = jsonb_set(COALESCE(payload, '{}'::jsonb), '{body}', :final_content_json ::jsonb, true), "
                "revision_count = revision_count + 1, "
                "last_revised_by = :revised_by, "
                "last_revised_at = now(), "
                "material_edit = :material_edit, "
                "updated_at = now() "
                "WHERE id = :id AND status = :pending "
                f"RETURNING {_COLUMNS_SQL}"
            ),
            {
                "id": item_id, "final_content": final_content,
                "final_content_json": _json.dumps(final_content),
                "revised_by": revised_by, "material_edit": material_edit,
                "pending": STATUS_PENDING,
            },
        ).mappings().first()
        return _row_to_item(dict(row)) if row else None


def mark_skipped(item_id: int, reason: str) -> bool:
    """Transitions a 'pending' or 'approved' row to 'skipped' -- guarded so a
    row that has already reached a terminal state (sent/failed/skipped) can
    never be downgraded. Without this guard, calling mark_skipped() on a row
    an earlier run already sent (e.g. a crashed sweep re-picking up the same
    id, or engine.execute_batch's own "claim_lost_to_concurrent_run" path
    firing after the row already completed) silently corrupts the
    completion receipt from 'sent' back to 'skipped' -- found by RELAY-v2.2
    R4's real-Postgres forced-retry test, which the in-memory fake-backend
    idempotency test (test_relay_engine.py) could not catch, since that
    fake models 'sent' and 'skipped' as two independent dicts rather than
    one mutually-exclusive status column.

    'pending' was added to the allowed source states in WP-T2-2 so a human
    can Skip a card still awaiting approval (the Slack Skip button), not
    only the execution engine's own skip-at-send-time path on already
    'approved' rows -- 'pending' is not a terminal state, so allowing it
    here does not reopen the RELAY-v2.2 bug this guard exists for."""
    with get_db_context() as session:
        result = session.execute(
            text(
                "UPDATE relay_approval_queue SET status = :status, "
                "error = :error, updated_at = now() "
                "WHERE id = :id AND status IN (:approved, :pending) "
                "AND (venture_key <> 'fa_max_lending' OR batch_id IS NULL)"
            ),
            {
                "status": STATUS_SKIPPED,
                "error": reason,
                "id": item_id,
                "approved": STATUS_APPROVED,
                "pending": STATUS_PENDING,
            },
        )
        return result.rowcount > 0
