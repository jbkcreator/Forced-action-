"""Frozen egress drafts awaiting a human decision (the ``pending_actions`` table).

Lifecycle of one row::

    pending ──approve──▶ approved ──claim──▶ sending ──▶ sent | failed | blocked
       │  ▲                (only before expires_at)
       │  └──revise (new payload)── revising
       ├──revise request──────────▶ revising ──reject──▶ rejected
       ├──reject──────────────────▶ rejected
       └──expires_at passes (pending or revising)──▶ expired

Every transition is a single conditional UPDATE, so a double click, a retried Slack payload or
two relay processes can move a row at most once. ``sending`` is only ever left by the process
that claimed it; a crash mid-send leaves the row in ``sending`` (outcome unknown) and it is
never retried automatically, because a duplicate send is worse than a missing one.

A revise never loses the draft it replaces: the replaced version is appended to ``revisions``.
"""
from __future__ import annotations

import enum
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

from .redaction import mask_long_digit_runs
from .store import AgentStore, utc_now

DEFAULT_TTL = timedelta(hours=72)
_ERROR_TEXT_LIMIT = 500


class ActionStatus(str, enum.Enum):
    PENDING = "pending"
    REVISING = "revising"
    APPROVED = "approved"
    REJECTED = "rejected"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    BLOCKED = "blocked"
    EXPIRED = "expired"


_OPEN_STATUSES = (ActionStatus.PENDING, ActionStatus.REVISING)


@dataclass(frozen=True)
class PendingAction:
    action_id: int
    tool_name: str
    channel: str
    payload: Mapping[str, Any]
    summary: str
    status: ActionStatus
    requested_by: str | None
    source_channel: str | None
    source_thread_ts: str | None
    card_channel: str | None
    card_ts: str | None
    decided_by: str | None
    revised_by: str | None
    revision_note: str | None
    revisions: tuple[Mapping[str, Any], ...]
    recipient_phone: str | None
    recipient_email: str | None
    contact_ref: str | None
    deal_ref: str | None
    idempotency_key: str | None
    expires_at: datetime | None
    provider_ref: str | None
    error: str | None


def _load_json(raw: Any, default: Any) -> Any:
    # jsonb comes back decoded from PostgreSQL and as text from SQLite.
    if raw is None:
        return default
    return json.loads(raw) if isinstance(raw, (str, bytes)) else raw


def _load_datetime(raw: Any) -> datetime | None:
    if raw is None or isinstance(raw, datetime):
        return raw
    return datetime.fromisoformat(str(raw))


def _dump_json(value: Any) -> str:
    """Strict: a value that is not plain JSON fails here, not at send time."""
    return json.dumps(value, sort_keys=True)


def _normalise_email(email: str | None) -> str | None:
    cleaned = (email or "").strip().lower()
    return cleaned or None


def _row_to_action(row: Mapping[str, Any]) -> PendingAction:
    return PendingAction(
        action_id=int(row["action_id"]),
        tool_name=row["tool_name"],
        channel=row["channel"],
        payload=dict(_load_json(row["payload"], {})),
        summary=row["summary"] or "",
        status=ActionStatus(row["status"]),
        requested_by=row["requested_by"],
        source_channel=row["source_channel"],
        source_thread_ts=row["source_thread_ts"],
        card_channel=row["card_channel"],
        card_ts=row["card_ts"],
        decided_by=row["decided_by"],
        revised_by=row["revised_by"],
        revision_note=row["revision_note"],
        revisions=tuple(_load_json(row["revisions"], [])),
        recipient_phone=row["recipient_phone"],
        recipient_email=row["recipient_email"],
        contact_ref=row["contact_ref"],
        deal_ref=row["deal_ref"],
        idempotency_key=row["idempotency_key"],
        expires_at=_load_datetime(row["expires_at"]),
        provider_ref=row["provider_ref"],
        error=row["error"],
    )


def is_expired(action: PendingAction, now: datetime) -> bool:
    return action.status is ActionStatus.EXPIRED or (
        action.status in _OPEN_STATUSES and action.expires_at is not None and action.expires_at <= now
    )


class PendingActionQueue:
    def __init__(self, store: AgentStore, clock: Callable[[], datetime] = utc_now,
                 default_ttl: timedelta = DEFAULT_TTL) -> None:
        self._store = store
        self._table = store.table("pending_actions")
        self._clock = clock
        self._default_ttl = default_ttl

    def now(self) -> datetime:
        return self._clock()

    def _transition(self, action_id: int, from_statuses: tuple[ActionStatus, ...], assignments: str,
                    params: Mapping[str, Any], *, require_unexpired: bool = False) -> bool:
        """One conditional UPDATE. True only for the caller that actually moved the row."""
        allowed = ", ".join(f":from_{i}" for i in range(len(from_statuses)))
        bind = {f"from_{i}": status.value for i, status in enumerate(from_statuses)}
        unexpired = " AND (expires_at IS NULL OR expires_at > :now)" if require_unexpired else ""
        with self._store.transaction() as conn:
            result = conn.execute(
                text(
                    f"UPDATE {self._table} SET {assignments}, updated_at = :now "
                    f"WHERE action_id = :action_id AND status IN ({allowed}){unexpired}"
                ),
                {**bind, **params, "action_id": action_id, "now": self._clock()},
            )
        return result.rowcount == 1

    def _select_one(self, conn, where: str, params: Mapping[str, Any]) -> PendingAction | None:
        row = conn.execute(text(f"SELECT * FROM {self._table} WHERE {where}"), params).mappings().first()
        return _row_to_action(row) if row else None

    def enqueue(self, *, tool_name: str, channel: str, payload: Mapping[str, Any], summary: str,
                requested_by: str | None = None, source_channel: str | None = None,
                source_thread_ts: str | None = None, recipient_phone: str | None = None,
                recipient_email: str | None = None, contact_ref: str | None = None, deal_ref: str | None = None,
                idempotency_key: str | None = None, ttl: timedelta | None = None) -> int:
        """Freeze the exact payload that will be sent if approved, and return its action id.

        ``recipient_phone`` must already be normalised by the host (this package does not know
        the host's phone format). A repeated ``idempotency_key`` returns the existing action
        instead of queuing a duplicate.
        """
        now = self._clock()
        params = {
            "tool_name": tool_name, "channel": channel, "payload": _dump_json(dict(payload)), "summary": summary,
            "status": ActionStatus.PENDING.value, "requested_by": requested_by, "source_channel": source_channel,
            "source_thread_ts": source_thread_ts, "recipient_phone": recipient_phone or None,
            "recipient_email": _normalise_email(recipient_email), "contact_ref": contact_ref, "deal_ref": deal_ref,
            "idempotency_key": idempotency_key, "expires_at": now + (ttl or self._default_ttl), "now": now,
        }
        with self._store.transaction() as conn:
            action_id = conn.execute(
                text(
                    f"INSERT INTO {self._table} (tool_name, channel, payload, summary, status, requested_by, "
                    "source_channel, source_thread_ts, recipient_phone, recipient_email, contact_ref, deal_ref, "
                    "idempotency_key, expires_at, created_at, updated_at) "
                    "VALUES (:tool_name, :channel, :payload, :summary, :status, :requested_by, :source_channel, "
                    ":source_thread_ts, :recipient_phone, :recipient_email, :contact_ref, :deal_ref, "
                    ":idempotency_key, :expires_at, :now, :now) "
                    "ON CONFLICT (idempotency_key) DO NOTHING RETURNING action_id"
                ),
                params,
            ).scalar_one_or_none()
            if action_id is None:
                action_id = conn.execute(
                    text(f"SELECT action_id FROM {self._table} WHERE idempotency_key = :key"),
                    {"key": idempotency_key},
                ).scalar_one()
        return int(action_id)

    def get(self, action_id: int) -> PendingAction | None:
        with self._store.transaction() as conn:
            return self._select_one(conn, "action_id = :action_id", {"action_id": action_id})

    def attach_card(self, action_id: int, channel: str, ts: str) -> None:
        with self._store.transaction() as conn:
            conn.execute(
                text(f"UPDATE {self._table} SET card_channel = :channel, card_ts = :ts, updated_at = :now "
                     "WHERE action_id = :action_id"),
                {"channel": channel, "ts": ts, "now": self._clock(), "action_id": action_id},
            )

    def approve(self, action_id: int, user_id: str) -> bool:
        return self._transition(action_id, (ActionStatus.PENDING,),
                                "status = :status, decided_by = :user_id, decided_at = :now",
                                {"status": ActionStatus.APPROVED.value, "user_id": user_id}, require_unexpired=True)

    def reject(self, action_id: int, user_id: str) -> bool:
        return self._transition(action_id, _OPEN_STATUSES,
                                "status = :status, decided_by = :user_id, decided_at = :now",
                                {"status": ActionStatus.REJECTED.value, "user_id": user_id})

    def request_revision(self, action_id: int, user_id: str) -> bool:
        """Open a revision for this user. Any other revision they had open goes back to pending."""
        now = self._clock()
        params = {"pending": ActionStatus.PENDING.value, "revising": ActionStatus.REVISING.value,
                  "user_id": user_id, "action_id": action_id, "now": now}
        with self._store.transaction() as conn:
            conn.execute(
                text(f"UPDATE {self._table} SET status = :pending, updated_at = :now "
                     "WHERE status = :revising AND revised_by = :user_id AND action_id <> :action_id"),
                params,
            )
            result = conn.execute(
                text(f"UPDATE {self._table} SET status = :revising, revised_by = :user_id, updated_at = :now "
                     "WHERE action_id = :action_id AND status = :pending"),
                params,
            )
        return result.rowcount == 1

    def revising_for(self, user_id: str) -> PendingAction | None:
        with self._store.transaction() as conn:
            return self._select_one(
                conn, "status = :revising AND revised_by = :user_id ORDER BY updated_at DESC, action_id DESC LIMIT 1",
                {"revising": ActionStatus.REVISING.value, "user_id": user_id},
            )

    def cancel_revision(self, action_id: int) -> bool:
        return self._transition(action_id, (ActionStatus.REVISING,), "status = :status",
                                {"status": ActionStatus.PENDING.value})

    def apply_revision(self, action_id: int, *, payload: Mapping[str, Any], summary: str, note: str,
                       ttl: timedelta | None = None) -> bool:
        """Replace the frozen payload with the redraft; the replaced version goes into ``revisions``.

        The redraft is a new draft: it needs a fresh approval and gets a fresh expiry.
        """
        now = self._clock()
        with self._store.transaction() as conn:
            current = self._select_one(conn, "action_id = :action_id AND status = :revising",
                                       {"action_id": action_id, "revising": ActionStatus.REVISING.value})
            if current is None:
                return False
            history = [*current.revisions, {
                "payload": dict(current.payload), "summary": current.summary, "note": note,
                "revised_by": current.revised_by, "revised_at": now.isoformat(),
            }]
            result = conn.execute(
                text(
                    f"UPDATE {self._table} SET status = :pending, payload = :payload, summary = :summary, "
                    "revision_note = :note, revisions = :revisions, card_channel = NULL, card_ts = NULL, "
                    "expires_at = :expires_at, updated_at = :now "
                    "WHERE action_id = :action_id AND status = :revising"
                ),
                {"pending": ActionStatus.PENDING.value, "revising": ActionStatus.REVISING.value,
                 "payload": _dump_json(dict(payload)), "summary": summary, "note": note,
                 "revisions": _dump_json(history), "expires_at": now + (ttl or self._default_ttl),
                 "now": now, "action_id": action_id},
            )
        return result.rowcount == 1

    def expire_stale(self) -> list[PendingAction]:
        """Move overdue pending/revising rows to ``expired`` and return them (for card updates)."""
        now = self._clock()
        with self._store.transaction() as conn:
            rows = conn.execute(
                text(
                    f"UPDATE {self._table} SET status = :expired, updated_at = :now "
                    "WHERE status IN (:pending, :revising) AND expires_at IS NOT NULL AND expires_at <= :now "
                    "RETURNING *"
                ),
                {"expired": ActionStatus.EXPIRED.value, "pending": ActionStatus.PENDING.value,
                 "revising": ActionStatus.REVISING.value, "now": now},
            ).mappings().all()
        return [_row_to_action(row) for row in rows]

    def claim_for_send(self, action_id: int) -> bool:
        return self._transition(action_id, (ActionStatus.APPROVED,), "status = :status",
                                {"status": ActionStatus.SENDING.value})

    def mark_sent(self, action_id: int, provider_ref: str | None) -> bool:
        return self._transition(action_id, (ActionStatus.SENDING,),
                                "status = :status, provider_ref = :provider_ref, executed_at = :now",
                                {"status": ActionStatus.SENT.value, "provider_ref": provider_ref})

    def _close_unsent(self, action_id: int, status: ActionStatus, reason: str) -> bool:
        return self._transition(action_id, (ActionStatus.SENDING,), "status = :status, error = :error, executed_at = :now",
                                {"status": status.value, "error": mask_long_digit_runs(reason)[:_ERROR_TEXT_LIMIT]})

    def mark_failed(self, action_id: int, error: str) -> bool:
        return self._close_unsent(action_id, ActionStatus.FAILED, error)

    def mark_blocked(self, action_id: int, reason: str) -> bool:
        """The send-time check refused it (opt-out, no consent). Nothing was sent."""
        return self._close_unsent(action_id, ActionStatus.BLOCKED, reason)

    def approved_ids(self, limit: int = 50) -> list[int]:
        with self._store.transaction() as conn:
            rows = conn.execute(
                text(f"SELECT action_id FROM {self._table} WHERE status = :approved "
                     "ORDER BY decided_at, action_id LIMIT :limit"),
                {"approved": ActionStatus.APPROVED.value, "limit": limit},
            ).scalars().all()
        return [int(action_id) for action_id in rows]
