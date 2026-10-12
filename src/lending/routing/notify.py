"""Slack notifications for R-1 routing events.

Post target: LENDING_ROUTING_CHANNEL (config/settings.py), the #next-deal-lending channel.  The
lending bot token (LENDING_SLACK_BOT_TOKEN) must be a member of that channel.

Spec: every route and every decline posts file ID, borrower last name, product, amount, lender
and position in the waterfall.  One builder (``build_message``) serves the live post and the
retry sweep so the two can never drift.

Nothing here is sent to a borrower.  Log lines carry IDs only (no PII).
Slack messages carry last name only (never phone/email).

Failure behaviour: post functions log a WARNING and return; they never raise.
A failed post leaves slack_posted_at NULL on the event row, which the routing sweep retries.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
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


def _amount(value) -> str:
    try:
        return f"${int(value):,}" if value else "unknown"
    except (TypeError, ValueError):
        return "unknown"


def build_message(
    action: str,
    *,
    lead_id: int,
    last_name: Optional[str],
    loan_type: Optional[str],
    loan_amount,
    lender_name: Optional[str],
    position: Optional[int],
    reason_code: Optional[str] = None,
    hold_reason: Optional[str] = None,
) -> Optional[str]:
    """The Slack text for one routing event, or None for an action that is not posted."""
    head = (
        f"File #{lead_id} · {last_name or 'Unknown'} · {loan_type or 'unknown'} · {_amount(loan_amount)}"
    )
    where = f"Lender: *{lender_name or 'unknown'}* (position {position if position else '?'} in waterfall)"
    if action == "routed":
        return f":arrow_right: *Routed* | {head}\n{where}"
    if action == "declined":
        return f":x: *Declined* | {head}\n{where} · reason: {reason_code or 'unspecified'}"
    if action == "held":
        why = f" · reason: {hold_reason}" if hold_reason else ""
        return f":warning: *Held* | {head}{why}\nNo lender matched — flagged for Josh."
    return None


def _mark_posted(event_id: int, ts: str, db: Session) -> None:
    """Update slack_ts / slack_posted_at on the event row (only these two columns may be updated)."""
    db.execute(
        text(
            "UPDATE lending.file_routing_events"
            "   SET slack_ts = :ts, slack_posted_at = :now"
            " WHERE id = :eid"
        ),
        {"ts": ts, "now": datetime.now(timezone.utc), "eid": event_id},
    )
    db.commit()


def _post(message: str, event_id: int, db: Session, *, what: str, lead_id) -> None:
    client = _client()
    channel = _channel()
    if not client or not channel:
        logger.warning("notify.%s: Slack not configured — skipping", what)
        return
    try:
        resp = client.chat_postMessage(channel=channel, text=message)
        _mark_posted(event_id, resp["ts"], db)
    except Exception:
        logger.exception("notify.%s lead_id=%s event_id=%d — will retry", what, lead_id, event_id)


def post_route_event(
    lead: dict,
    lender: LenderRow,
    position: int,
    event_id: int,
    *,
    db: Session,
) -> None:
    """Post a 'routed' notification (best-effort)."""
    message = build_message(
        "routed", lead_id=lead.get("id"), last_name=lead.get("last_name"),
        loan_type=lead.get("loan_type"), loan_amount=lead.get("loan_amount"),
        lender_name=lender.name, position=position,
    )
    _post(message, event_id, db, what="post_route_event", lead_id=lead.get("id"))


def post_decline_event(
    lead: dict,
    lender_name: Optional[str],
    position: Optional[int],
    reason_code: str,
    event_id: int,
    *,
    db: Session,
) -> None:
    """Post a 'declined' notification (best-effort)."""
    message = build_message(
        "declined", lead_id=lead.get("id"), last_name=lead.get("last_name"),
        loan_type=lead.get("loan_type"), loan_amount=lead.get("loan_amount"),
        lender_name=lender_name, position=position, reason_code=reason_code,
    )
    _post(message, event_id, db, what="post_decline_event", lead_id=lead.get("id"))


def post_hold_event(
    lead: dict,
    event_id: int,
    *,
    reason: str,
    db: Session,
) -> None:
    """Post a 'held' notification (best-effort)."""
    message = build_message(
        "held", lead_id=lead.get("id"), last_name=lead.get("last_name"),
        loan_type=lead.get("loan_type"), loan_amount=lead.get("loan_amount"),
        lender_name=None, position=None, hold_reason=reason,
    )
    _post(message, event_id, db, what="post_hold_event", lead_id=lead.get("id"))


def retry_unposted_events(db: Session) -> int:
    """Retry Slack posts for events where slack_posted_at IS NULL.

    Called by the routing sweep so failed initial posts are eventually delivered.
    Returns the number of events successfully posted.  Only routed / declined / held events
    are posted; approved / funded outcomes are never posted, so they are not retried.
    """
    client = _client()
    channel = _channel()
    if not client or not channel:
        return 0

    rows = db.execute(
        text(
            "SELECT fre.id, fre.lead_id, fre.lender_name, fre.action, fre.reason_code,"
            "       ll.last_name, ll.loan_type, ll.loan_amount,"
            "       (SELECT COUNT(*) FROM lending.file_routing_events r"
            "         WHERE r.lead_id = fre.lead_id AND r.action = 'routed' AND r.id <= fre.id) AS position,"
            "       fr.hold_reason"
            "  FROM lending.file_routing_events fre"
            "  JOIN lending.lendingflow_leads ll ON ll.id = fre.lead_id"
            "  LEFT JOIN lending.file_routing fr ON fr.lead_id = fre.lead_id"
            " WHERE fre.slack_posted_at IS NULL"
            "   AND fre.action IN ('routed', 'declined', 'held')"
            " ORDER BY fre.created_at"
            " LIMIT 100"
        )
    ).mappings().all()

    posted = 0
    for row in rows:
        message = build_message(
            row["action"], lead_id=row["lead_id"], last_name=row["last_name"],
            loan_type=row["loan_type"], loan_amount=row["loan_amount"],
            lender_name=row["lender_name"], position=int(row["position"]) or None,
            reason_code=row["reason_code"], hold_reason=row["hold_reason"],
        )
        if message is None:
            continue
        try:
            resp = client.chat_postMessage(channel=channel, text=message)
            _mark_posted(row["id"], resp["ts"], db)
            posted += 1
        except Exception:
            logger.exception("retry_unposted_events event_id=%d — will retry again", row["id"])

    if posted:
        logger.info("retry_unposted_events: posted %d events", posted)
    return posted
