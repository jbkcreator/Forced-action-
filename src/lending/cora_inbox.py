"""Cora's inbox: events a job wants raised with the operator in Cora's main thread.

Cora's minute sweep lists new events in one digest post in her channel (``surface_new_items``).

Three ways to add an event, by where the caller is:

  * ``add_to_inbox``: on the caller's session, in the caller's transaction. Raises on failure.
  * ``add_to_inbox_safely``: same, inside a savepoint, and never raises. For jobs whose own work must not be
    undone when the inbox write fails (an alarm that was already sent stays recorded).
  * ``add_to_inbox_in_own_session``: opens and commits its own session, and never raises. For alert paths that
    hold no session, or deliberately avoid the database so the alert still goes out when the database is down.

Adding the same ``(event_type, dedup_key)`` twice is a no-op, so a retried job never raises an event twice.
``channel_posted=True`` records that the job already posted the event to its team channel itself.

The payload is shown to the operator and to Cora's model: put references (lead_ref, ids) and masked values in
it, never raw phone numbers, emails or consent text.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.db import lending_session

logger = logging.getLogger(__name__)

_INSERT = text("""
    INSERT INTO lending.cora_inbox (event_type, dedup_key, summary, payload, target_channel, channel_posted_at)
    VALUES (:event_type, :dedup_key, :summary, CAST(:payload AS jsonb), :target_channel,
            CASE WHEN :channel_posted THEN now() END)
    ON CONFLICT (event_type, dedup_key) DO NOTHING
    RETURNING id
""")

# Read at call time so the lending tests can swap in a fake and never write to the shared database.
_own_session_factory: Callable = lending_session


def add_to_inbox(session: Session, *, event_type: str, dedup_key: str, summary: str = "",
                 payload: Optional[Mapping[str, Any]] = None, target_channel: Optional[str] = None,
                 channel_posted: bool = False) -> Optional[int]:
    """Queue one event for Cora. Returns the new row id, or None when the event was already queued.

    Does not commit: the caller's transaction decides whether the event is kept.
    """
    event_type, dedup_key = event_type.strip(), dedup_key.strip()
    if not event_type or not dedup_key:
        raise ValueError("add_to_inbox needs a non-empty event_type and dedup_key")
    # default=str: a date or Decimal in the payload is stored as text instead of failing the job that raised it.
    row_id = session.execute(_INSERT, {
        "event_type": event_type, "dedup_key": dedup_key, "summary": summary,
        "payload": json.dumps(dict(payload or {}), default=str), "target_channel": target_channel,
        "channel_posted": channel_posted,
    }).scalar_one_or_none()
    if row_id is None:
        logger.info("[cora_inbox] %s already queued (dedup_key=%s)", event_type, dedup_key)
    return row_id


def add_to_inbox_safely(session: Session, **event: Any) -> Optional[int]:
    """``add_to_inbox`` in a savepoint: a failure is logged and rolled back alone, the caller's work is kept."""
    try:
        with session.begin_nested():
            return add_to_inbox(session, **event)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[cora_inbox] %s not queued (%s)", event.get("event_type"), type(exc).__name__)
        return None


def add_to_inbox_in_own_session(*, session_factory: Optional[Callable] = None, **event: Any) -> Optional[int]:
    """``add_to_inbox`` on a session of its own, committed on success. Never raises."""
    try:
        with (session_factory or _own_session_factory)() as db:
            return add_to_inbox(db, **event)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[cora_inbox] %s not queued (%s)", event.get("event_type"), type(exc).__name__)
        return None


# ── The digest Cora posts in her channel ──────────────────────────────────────

DIGEST_LIMIT = 20  # items per digest; the rest stay 'new' and lead the next one

# Locked until the caller's transaction ends, so a second sweep never lists the same item in another digest.
_CLAIM_NEW = text("""
    SELECT id, summary, target_channel, channel_posted_at IS NOT NULL AS channel_posted
      FROM lending.cora_inbox
     WHERE status = 'new'
     ORDER BY created_at, id
     LIMIT :limit
       FOR UPDATE SKIP LOCKED
""")
_MARK_SURFACED = text("""
    UPDATE lending.cora_inbox
       SET status = 'surfaced', surfaced_channel = :channel, surfaced_ts = :ts, surfaced_at = now(), updated_at = now()
     WHERE id = ANY(:ids) AND status = 'new'
""")


def _slack_safe(value: str) -> str:
    """Slack treats <!channel>, <!here> and <url|label> as live markup unless & < > are escaped."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_digest(items: Sequence[Mapping[str, Any]], channel_ids: Mapping[str, str]) -> str:
    """One plain-text line per item, numbered by inbox id so the operator can refer to it in a reply."""
    lines = [f"*{len(items)} new item{'s' if len(items) != 1 else ''}*"]
    for item in items:
        line = f"#{item['id']}  {_slack_safe(item['summary'] or 'No summary')}"
        if item["channel_posted"] and item["target_channel"]:
            channel_id = channel_ids.get(item["target_channel"])
            line += f"  _(already in {f'<#{channel_id}>' if channel_id else item['target_channel']})_"
        lines.append(line)
    return "\n".join(lines)


def surface_new_items(db, chat: Any, *, channel: str, channel_ids: Mapping[str, str],
                      limit: int = DIGEST_LIMIT) -> int:
    """Post one digest of the oldest 'new' items to ``channel`` and mark them surfaced. Returns how many.

    Runs in the caller's transaction. When Slack refuses the post nothing is marked, so the items lead the next
    digest. If the post succeeds but the transaction then fails, the items are listed again next time: a repeated
    line is preferred to an event the operator never sees.
    """
    items = db.execute(_CLAIM_NEW, {"limit": limit}).mappings().all()
    if not items:
        return 0
    result = chat.post(text=format_digest(items, channel_ids), channel=channel)
    if not (result.ok and result.ts):
        logger.warning("[cora_inbox] digest of %d item(s) not posted (%s); retried next sweep", len(items),
                       result.error or "no response")
        return 0
    db.execute(_MARK_SURFACED, {"ids": [item["id"] for item in items], "channel": result.channel or channel,
                                "ts": result.ts})
    return len(items)
