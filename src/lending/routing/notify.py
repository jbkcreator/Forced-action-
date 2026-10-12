"""Slack notifications for R-1 routing events.

Post target: LENDING_ROUTING_CHANNEL (config/settings.py).  The Cora Lending
bot token (LENDING_SLACK_BOT_TOKEN) must be a member of that channel.

Nothing here is sent to a borrower.  Log lines carry IDs only (no PII).
Slack messages carry last name only (never phone/email).

Failure behaviour: post functions log a WARNING and return — they never raise.
A failed post leaves slack_posted_at NULL on the event row, which the routing
sweep retries.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.routing.rules import LenderRow

logger = logging.getLogger(__name__)


def _client():
    """Return a Slack WebClient using the lending bot token, or None if unconfigured."""
    settings = get_settings()
    token = settings.lending_slack_bot_token
    if not token:
        return None
    try:
        from slack_sdk import WebClient
        return WebClient(token=token.get_secret_value())
    except Exception:
        logger.exception("notify: could not build Slack client")
        return None


def _channel() -> str:
    return get_settings().lending_routing_channel


def _mark_posted(event_id: int, ts: str, db: Session) -> None:
    """Update slack_ts / slack_posted_at on the event row (only these two columns may be updated)."""
    from datetime import datetime, timezone
    db.execute(
        text(
            "UPDATE lending.file_routing_events"
            "   SET slack_ts = :ts, slack_posted_at = :now"
            " WHERE id = :eid"
        ),
        {"ts": ts, "now": datetime.now(timezone.utc), "eid": event_id},
    )
    db.commit()


def post_route_event(
    lead: dict,
    lender: LenderRow,
    position: int,
    event_id: int,
    *,
    db: Session,
) -> None:
    """Post a 'routed' notification to #next-deal-lending (best-effort)."""
    client = _client()
    channel = _channel()
    if not client or not channel:
        logger.warning("notify.post_route_event: Slack not configured — skipping")
        return

    last_name = lead.get("last_name") or "Unknown"
    loan_type = lead.get("loan_type") or "unknown"
    amount = lead.get("loan_amount")
    amount_str = f"${int(amount):,}" if amount else "unknown"
    lead_id = lead.get("id")

    text_body = (
        f":arrow_right: *Routed* | File #{lead_id} · {last_name} · {loan_type} · {amount_str}"
        f"\nLender: *{lender.name}* (position {position} in waterfall)"
    )
    try:
        resp = client.chat_postMessage(channel=channel, text=text_body)
        _mark_posted(event_id, resp["ts"], db)
    except Exception:
        logger.exception("notify.post_route_event lead_id=%s event_id=%d — will retry", lead_id, event_id)


def post_decline_event(
    lead_id: int,
    lender_name: Optional[str],
    reason_code: str,
    event_id: int,
    *,
    db: Session,
) -> None:
    """Post a 'declined' notification to #next-deal-lending (best-effort)."""
    client = _client()
    channel = _channel()
    if not client or not channel:
        logger.warning("notify.post_decline_event: Slack not configured — skipping")
        return

    text_body = (
        f":x: *Declined* | File #{lead_id} · lender: {lender_name or 'unknown'}"
        f" · reason: {reason_code}"
    )
    try:
        resp = client.chat_postMessage(channel=channel, text=text_body)
        _mark_posted(event_id, resp["ts"], db)
    except Exception:
        logger.exception("notify.post_decline_event lead_id=%d event_id=%d — will retry", lead_id, event_id)


def post_hold_event(
    lead: dict,
    event_id: int,
    *,
    reason: str,
    db: Session,
) -> None:
    """Post a 'held' notification to #next-deal-lending (best-effort)."""
    client = _client()
    channel = _channel()
    if not client or not channel:
        return

    lead_id = lead.get("id")
    last_name = lead.get("last_name") or "Unknown"
    text_body = (
        f":warning: *Held* | File #{lead_id} · {last_name} · reason: {reason}"
        f"\nNo lender matched — flagged for Josh."
    )
    try:
        resp = client.chat_postMessage(channel=channel, text=text_body)
        _mark_posted(event_id, resp["ts"], db)
    except Exception:
        logger.exception("notify.post_hold_event lead_id=%s event_id=%d — will retry", lead_id, event_id)


def retry_unposted_events(db: Session) -> int:
    """Retry Slack posts for events where slack_posted_at IS NULL.

    Called by the routing sweep so failed initial posts are eventually delivered.
    Returns the number of events successfully retried.
    """
    client = _client()
    channel = _channel()
    if not client or not channel:
        return 0

    rows = db.execute(
        text(
            "SELECT fre.id, fre.lead_id, fre.lender_name, fre.action, fre.reason_code,"
            "       ll.last_name, ll.loan_type, ll.loan_amount"
            "  FROM lending.file_routing_events fre"
            "  JOIN lending.lendingflow_leads ll ON ll.id = fre.lead_id"
            " WHERE fre.slack_posted_at IS NULL"
            " ORDER BY fre.created_at"
            " LIMIT 100"
        )
    ).mappings().all()

    posted = 0
    for row in rows:
        action = row["action"]
        lead_id = row["lead_id"]
        last_name = row["last_name"] or "Unknown"
        lender_name = row["lender_name"] or "unknown"
        amount = row["loan_amount"]
        amount_str = f"${int(amount):,}" if amount else "unknown"

        if action == "routed":
            msg = (
                f":arrow_right: *Routed* | File #{lead_id} · {last_name}"
                f" · {row['loan_type']} · {amount_str}\nLender: *{lender_name}*"
            )
        elif action == "declined":
            msg = (
                f":x: *Declined* | File #{lead_id} · lender: {lender_name}"
                f" · reason: {row['reason_code'] or 'unspecified'}"
            )
        elif action == "held":
            msg = f":warning: *Held* | File #{lead_id} · {last_name} — no lender matched"
        else:
            continue

        try:
            resp = client.chat_postMessage(channel=channel, text=msg)
            _mark_posted(row["id"], resp["ts"], db)
            posted += 1
        except Exception:
            logger.exception("retry_unposted_events event_id=%d — will retry again", row["id"])

    if posted:
        logger.info("retry_unposted_events: posted %d events", posted)
    return posted
