"""Slack notifications for R-1 routing events.

Post target: #deals (LENDING_DEALS_CHANNEL), posted by Cora's bot (LENDING_CORA_SLACK_BOT_TOKEN), the app that
already runs over Socket Mode; her bot must be a member of #deals. Unset = not posted. Her listener receives the
Decline / Approved / Funded clicks in any channel (see slack_extension.py). The /lenders and /partners audit posts
go to Cora's own channel (LENDING_CORA_CHANNEL, ``audit_channel``).

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

from config.lending_partners import PRODUCTS
from config.settings import get_settings
from src.lending.routing.file_key import key_from_row
from src.lending.routing.rules import LenderRow

logger = logging.getLogger(__name__)


def _client():
    """A Slack WebClient on Cora's bot, or None if it is not configured."""
    token = get_settings().lending_cora_slack_bot_token
    if not token:
        return None
    try:
        from slack_sdk import WebClient
        return WebClient(token=token.get_secret_value())
    except Exception:
        logger.exception("notify: could not build Slack client")
        return None


def _channel() -> str:
    """#deals: routing posts and their buttons."""
    return get_settings().lending_deals_channel


def audit_channel() -> str:
    """#cora: who changed lenders or partners from Slack."""
    return get_settings().lending_cora_channel


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
    label: Optional[str] = None,
) -> Optional[str]:
    """The Slack text for one routing event, or None for an action that is not posted."""
    head = (
        f"{label or f'File #{lead_id}'} · {last_name or 'Unknown'} · {loan_type or 'unknown'} · {_amount(loan_amount)}"
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


def _button(action_id: str, event_id: int, label: str, style: Optional[str] = None, confirm: Optional[str] = None) -> dict:
    element = {"type": "button", "action_id": action_id, "value": str(event_id), "text": {"type": "plain_text", "text": label}}
    if style:
        element["style"] = style
    if confirm:
        element["confirm"] = {
            "title": {"type": "plain_text", "text": "Are you sure?"},
            "text": {"type": "mrkdwn", "text": confirm},
            "confirm": {"type": "plain_text", "text": "Yes"}, "deny": {"type": "plain_text", "text": "Cancel"}}
    return element


def details_block(event_id: int) -> dict:
    """The Details button on its own: what stays on a post once its file has moved on."""
    from config.lending_routing import ACTION_DETAILS

    return {"type": "actions", "block_id": f"routing_details_{event_id}", "elements": [_button(ACTION_DETAILS, event_id, "Details")]}


def _action_blocks(message: str, action: str, event_id: int) -> Optional[list[dict]]:
    """Buttons under a post: Details on every route, decline and hold; Decline / Approved / Funded only on a route."""
    if action not in ("routed", "declined", "held"):
        return None
    from config.lending_routing import ACTION_APPROVED, ACTION_DECLINE, ACTION_DETAILS, ACTION_FUNDED, ACTION_REROUTE

    elements = [_button(ACTION_DETAILS, event_id, "Details")]
    if action == "held":
        elements = [_button(ACTION_REROUTE, event_id, "Re-route", "primary")] + elements
    if action == "routed":
        elements = [
            _button(ACTION_DECLINE, event_id, "Decline", "danger"),
            _button(ACTION_APPROVED, event_id, "Approved"),
            _button(ACTION_FUNDED, event_id, "Funded", "primary", "This uses one of the lender's monthly seats. Mark it funded?"),
        ] + elements
    return [{"type": "section", "text": {"type": "mrkdwn", "text": message}},
            {"type": "actions", "block_id": f"routing_{event_id}", "elements": elements}]


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


def _post(message: str, event_id: int, db: Session, *, what: str, lead_id, action: str = "") -> None:
    client = _client()
    channel = _channel()
    if not client or not channel:
        logger.warning("notify.%s: Slack not configured — skipping", what)
        return
    try:
        blocks = _action_blocks(message, action, event_id)
        resp = (client.chat_postMessage(channel=channel, text=message, blocks=blocks) if blocks
                else client.chat_postMessage(channel=channel, text=message))
        _mark_posted(event_id, resp["ts"], db)
    except Exception:
        db.rollback()  # a failed bookkeeping write must not leave the caller's session aborted
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
        lender_name=lender.name, position=position, label=lead.get("label"),
    )
    _post(message, event_id, db, what="post_route_event", lead_id=lead.get("id"), action="routed")


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
        lender_name=lender_name, position=position, reason_code=reason_code, label=lead.get("label"),
    )
    _post(message, event_id, db, what="post_decline_event", lead_id=lead.get("id"), action="declined")


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
        lender_name=None, position=None, hold_reason=reason, label=lead.get("label"),
    )
    _post(message, event_id, db, what="post_hold_event", lead_id=lead.get("id"), action="held")


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

    # The Partner Door table may not be installed yet; the three fragments below are fixed literals.
    partner = db.execute(text("SELECT to_regclass('lending.loan_files') IS NOT NULL")).scalar() is True
    last_name_sql = "COALESCE(ll.last_name, lf.borrower_last_name)" if partner else "ll.last_name"
    product_sql = "lf.product" if partner else "CAST(NULL AS text)"
    amount_sql = "COALESCE(ll.loan_amount, lf.loan_amount)" if partner else "ll.loan_amount"
    partner_join = "  LEFT JOIN lending.loan_files lf ON lf.id = fre.loan_file_id" if partner else ""
    rows = db.execute(
        text(
            "SELECT fre.id, fre.lead_id, fre.loan_file_id, fre.lender_name, fre.action, fre.reason_code,"
            f"       {last_name_sql} AS last_name,"
            f"       ll.loan_type, {product_sql} AS product,"
            f"       {amount_sql} AS loan_amount,"
            "       (SELECT COUNT(*) FROM lending.file_routing_events r"
            "         WHERE r.action = 'routed' AND r.id <= fre.id"
            "           AND ((fre.lead_id IS NOT NULL AND r.lead_id = fre.lead_id)"
            "             OR (fre.loan_file_id IS NOT NULL AND r.loan_file_id = fre.loan_file_id))) AS position,"
            "       fr.hold_reason"
            "  FROM lending.file_routing_events fre"
            "  LEFT JOIN lending.lendingflow_leads ll ON ll.id = fre.lead_id"
            f"{partner_join}"
            "  LEFT JOIN lending.file_routing fr"
            "    ON (fre.lead_id IS NOT NULL AND fr.lead_id = fre.lead_id)"
            "    OR (fre.loan_file_id IS NOT NULL AND fr.loan_file_id = fre.loan_file_id)"
            " WHERE fre.slack_posted_at IS NULL"
            "   AND fre.action IN ('routed', 'declined', 'held')"
            " ORDER BY fre.created_at"
            " LIMIT 100"
        )
    ).mappings().all()

    posted = 0
    for row in rows:
        key = key_from_row(row)
        loan_type = row["loan_type"]
        if row["product"] is not None:  # a partner file stores its product, not a lender-box loan type
            product_type = PRODUCTS.get(row["product"])
            loan_type = product_type.value if product_type is not None else None
        message = build_message(
            row["action"], lead_id=key.id, last_name=row["last_name"],
            loan_type=loan_type, loan_amount=row["loan_amount"],
            lender_name=row["lender_name"], position=int(row["position"]) or None,
            reason_code=row["reason_code"], hold_reason=row["hold_reason"], label=key.label,
        )
        if message is None:
            continue
        try:
            blocks = _action_blocks(message, row["action"], row["id"])
            resp = (client.chat_postMessage(channel=channel, text=message, blocks=blocks) if blocks
                    else client.chat_postMessage(channel=channel, text=message))
            _mark_posted(row["id"], resp["ts"], db)
            posted += 1
        except Exception:
            db.rollback()  # keep the session usable for the rest of the batch
            logger.exception("retry_unposted_events event_id=%d — will retry again", row["id"])

    if posted:
        logger.info("retry_unposted_events: posted %d events", posted)
    return posted
