"""FA Max pending slot (WP-T3-1) — "this approver's next message is for this card".

Port of Banks' button-driven ``pending_revisions`` pattern (never imported —
Banks is hard-walled). One slot per approver, last tap wins, short TTL. Two
kinds share the table: ``revise`` (NL draft revision) and ``voice`` (voice-note
intake). An expired slot is deleted on read so a message typed long after a
forgotten tap can never act on a stale card.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings

SLOT_KINDS: frozenset[str] = frozenset({"revise", "voice"})


@dataclass(frozen=True)
class PendingSlot:
    slack_user_id: str
    kind: str
    target_ref: str
    channel_id: str
    thread_ts: Optional[str]
    set_at: datetime


def open_slot(
    session: Session,
    *,
    slack_user_id: str,
    kind: str,
    target_ref: str,
    channel_id: str,
    thread_ts: Optional[str] = None,
) -> None:
    if kind not in SLOT_KINDS:
        raise ValueError(f"unknown pending-slot kind: {kind!r}")
    session.execute(
        text("""
            INSERT INTO fa_max_pending_slots
                (slack_user_id, kind, target_ref, channel_id, thread_ts, set_at)
            VALUES (:user, :kind, :target_ref, :channel_id, :thread_ts, now())
            ON CONFLICT (slack_user_id) DO UPDATE SET
                kind = EXCLUDED.kind,
                target_ref = EXCLUDED.target_ref,
                channel_id = EXCLUDED.channel_id,
                thread_ts = EXCLUDED.thread_ts,
                set_at = EXCLUDED.set_at
        """),
        {"user": slack_user_id, "kind": kind, "target_ref": target_ref,
         "channel_id": channel_id, "thread_ts": thread_ts},
    )


def get_slot(
    session: Session,
    slack_user_id: str,
    *,
    now: Optional[datetime] = None,
    ttl_min: Optional[int] = None,
) -> Optional[PendingSlot]:
    row = session.execute(
        text("""
            SELECT slack_user_id, kind, target_ref, channel_id, thread_ts, set_at
            FROM fa_max_pending_slots WHERE slack_user_id = :user
        """),
        {"user": slack_user_id},
    ).mappings().first()
    if not row:
        return None
    ttl = ttl_min if ttl_min is not None else get_settings().fa_max_pending_slot_ttl_min
    now = now or datetime.now(timezone.utc)
    if now - row["set_at"] > timedelta(minutes=ttl):
        clear_slot(session, slack_user_id)
        return None
    return PendingSlot(**dict(row))


def clear_slot(session: Session, slack_user_id: str) -> None:
    session.execute(
        text("DELETE FROM fa_max_pending_slots WHERE slack_user_id = :user"),
        {"user": slack_user_id},
    )
