"""R-1 buttons on a "Routed" Slack post: Decline, Approved, Funded (Josh: a decline always needs a reason).

The routed post (``notify._action_blocks``) carries three buttons whose value is the routing event id. It is posted by
Cora's bot, so the clicks arrive on Cora's Socket Mode listener, which hands them to ``slack_extension.RoutingSlack``.
  * **Decline** opens a form: a reason from Josh's list is required, and the catch-all 'other' also needs details.
  * **Approved** records an ``approved`` event; the post keeps its buttons (the file can still fund or decline).
  * **Funded** (Slack asks for confirmation first) records ``funded``, which uses one of the lender's seats.

Only Cora's approvers (``LENDING_CORA_APPROVER_USER_IDS``) may act; empty means nobody. A button works only while its post is still the file's current route, so an old post can never hit a later
lender. Events record the actor as ``slack:<user id>``.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import (
    ACTION_APPROVED, ACTION_DECLINE, ACTION_DETAILS, ACTION_FUNDED, ACTION_REROUTE, CATCH_ALL_DECLINE_REASON, ROUTING_TZ,
)
from src.lending.db import lending_session
from src.lending.routing import lender_service
from src.lending.routing.file_key import key_from_row
from src.lending.routing.notify import details_block
from src.lending.routing.slack_admin import _input, _user_id, _value, allowed_user_ids

logger = logging.getLogger(__name__)

DECLINE_CALLBACK_ID = "routing_decline_form"
_OUTCOME_BUTTONS = {ACTION_APPROVED: "approved", ACTION_FUNDED: "funded"}
NOT_CURRENT = "That post is no longer the file's current route (it was declined, re-routed or already actioned)."
ALREADY_FUNDED = "This file is already funded, so it cannot go back to approved. Nothing was changed."

Scheduler = Callable[..., None]  # run_later(fn, *args): do the slow part after Slack has been answered


def is_routing_action(payload: dict) -> bool:
    actions = payload.get("actions") or []
    return (payload.get("type") == "block_actions" and bool(actions)
            and actions[0].get("action_id") in {ACTION_DECLINE, ACTION_DETAILS, ACTION_REROUTE, *_OUTCOME_BUTTONS})


def is_decline_submission(payload: dict) -> bool:
    return payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == DECLINE_CALLBACK_ID


def _current_route(db: Session, event_id: int, *, strict: bool) -> Optional[dict]:
    """The 'routed' event if it is still its file's current route. ``strict`` (a decline): a funding also ends it.
    An approval never does: a lender that approved can still decline, and an approved file can still be funded."""
    event = db.execute(
        text("SELECT id, lead_id, loan_file_id, lender_name, action FROM lending.file_routing_events WHERE id = :e"),
        {"e": event_id}).mappings().one_or_none()
    if event is None or event["action"] != "routed":
        return None
    key = key_from_row(event)
    later = ("routed", "declined", "held", "funded") if strict else ("routed", "declined", "held")
    newer = db.execute(
        text(f"SELECT 1 FROM lending.file_routing_events WHERE {key.column} = :id AND id > :e AND action = ANY(:acts) LIMIT 1"),
        {"id": key.id, "e": event_id, "acts": list(later)}).scalar()
    status = db.execute(text(f"SELECT status FROM lending.file_routing WHERE {key.column} = :id"), {"id": key.id}).scalar()
    return None if newer or status != "routed" else {"key": key, "lender": event["lender_name"]}


def _tell(client: Any, channel: str, user: str, message: str) -> None:
    try:
        client.chat_postEphemeral(channel=channel, user=user, text=message)
    except Exception as exc:  # class only
        logger.error("[routing-slack] ephemeral notice failed: %s", type(exc).__name__)


def _update_post(client: Any, channel: str, ts: str, original: str, note: str, *, keep_buttons: Optional[list] = None) -> None:
    """Add what happened under the post; drop the buttons unless asked to keep them (best-effort)."""
    if not (channel and ts):
        return
    body = f"{original}\n{note}"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": body}}] + list(keep_buttons or [])
    try:
        client.chat_update(channel=channel, ts=ts, text=body, blocks=blocks)
    except Exception as exc:  # class only
        logger.error("[routing-slack] updating the post failed: %s", type(exc).__name__)


def _decline_view(event_id: int, lender: str, channel: str, ts: str, original: str, db: Session) -> dict:
    reasons = db.execute(
        text("SELECT code, label FROM lending.decline_reason_codes WHERE active ORDER BY label LIMIT 100")).all()
    options = [{"text": {"type": "plain_text", "text": label[:75]}, "value": code} for code, label in reasons]
    return {
        "type": "modal", "callback_id": DECLINE_CALLBACK_ID, "title": {"type": "plain_text", "text": "Decline"},
        "submit": {"type": "plain_text", "text": "Decline"}, "close": {"type": "plain_text", "text": "Cancel"},
        "private_metadata": json.dumps({"event_id": event_id, "channel": channel, "ts": ts, "original": original[:2500]}),
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*{lender}* declined this file. Why?"}},
            _input("reason_b", "Reason", {"type": "static_select", "action_id": "v", "options": options}, optional=False),
            _input("text_b", "Details", {"type": "plain_text_input", "action_id": "v", "multiline": True},
                   hint=f"Required when the reason is '{CATCH_ALL_DECLINE_REASON}'. Anything that helps place the file elsewhere."),
        ]}


def format_record(record: dict) -> str:
    """The file record as Slack text: where it is, who is next, and the route history (times in Eastern)."""
    file = record["file"]
    head = (f"*File #{file['file_id']} · {file['last_name'] or 'Unknown'} · "
            f"{str(file['loan_type'] or 'unknown').replace('_', ' ').title()} · "
            f"${int(file['loan_amount']):,}* · status: {file['status']}" if file.get("loan_amount") else
            f"*File #{file['file_id']} · {file['last_name'] or 'Unknown'}* · status: {file['status']}")
    current, nxt = record.get("current"), record.get("next")
    lines = [head,
             f"Current lender: *{current['name']}* (position {current['position']})" if current else "Current lender: none",
             f"Next in line: *{nxt['name']}* (position {nxt['position']})" if nxt else "Next in line: none"]
    if file.get("hold_reason"):
        lines.append(f"Held because: {file['hold_reason']}")
    lines.append("*Route history*")
    for item in record["history"][-15:]:
        when = item["created_at"].astimezone(ROUTING_TZ).strftime("%b %d %I:%M %p ET")
        line = f"• {when} · {item['action']} · {item['lender_name'] or '-'}"
        if item.get("reason_code"):
            line += f" · {item['reason_code']}"
        if item.get("free_text"):
            line += f" ({item['free_text'][:120]})"
        lines.append(f"{line} · {item['actor'] or 'system'}")
    return "\n".join(lines)


def _show_details(client: Any, channel: str, user: str, event_id: int) -> None:
    """The file record, shown privately to whoever clicked Details."""
    try:
        with lending_session() as db:
            event = db.execute(text("SELECT lead_id, loan_file_id FROM lending.file_routing_events WHERE id = :e"),
                               {"e": event_id}).mappings().one_or_none()
            if event is None:
                _tell(client, channel, user, "That file was not found.")
                return
            key = key_from_row(event)
            record = lender_service.file_record(key, db)
        _tell(client, channel, user, format_record(record))
    except Exception as exc:  # class only
        logger.error("[routing-slack] details failed event=%s: %s", event_id, type(exc).__name__)
        _tell(client, channel, user, "Something went wrong; nothing was changed.")


def _reroute(client: Any, channel: str, user: str, ts: str, original: str, event_id: int) -> None:
    """Send a held or exhausted file back to the router now (after a lender was signed or a box was changed)."""
    from src.lending.routing.file_key import LEAD
    from src.lending.routing.router import reopen_file, route_file, route_loan_file

    try:
        with lending_session() as db:
            event = db.execute(text("SELECT lead_id, loan_file_id, action FROM lending.file_routing_events WHERE id = :e"),
                               {"e": event_id}).mappings().one_or_none()
            if event is None or event["action"] != "held":
                _tell(client, channel, user, "That post is not a held file.")
                return
            key = key_from_row(event)
            if not reopen_file(key, db):
                _tell(client, channel, user, "That file is no longer in the hold queue.")
                return
            (route_file if key.kind == LEAD else route_loan_file)(key.id, db)
            result = db.execute(text(f"SELECT status, current_lender_name FROM lending.file_routing WHERE {key.column} = :id"),
                                {"id": key.id}).one()
    except Exception as exc:  # class only
        logger.error("[routing-slack] re-route failed event=%s: %s", event_id, type(exc).__name__)
        _tell(client, channel, user, "Something went wrong; nothing was changed.")
        return
    outcome = (f"now with *{result.current_lender_name}*" if result.status == "routed"
               else "but no lender matches yet, so it is held again (a new Held post follows)")
    _update_post(client, channel, ts, original, f":recycle: *Sent back to the router* by <@{user}>: {outcome}",
                 keep_buttons=[details_block(event_id)])


def _buttons_left_after_approval(message: dict) -> list[dict]:
    """The post's buttons without Approved (done now); Decline and Funded stay, the file is not finished."""
    return [{**block, "elements": [e for e in block.get("elements") or [] if e.get("action_id") != ACTION_APPROVED]}
            for block in message.get("blocks") or [] if block.get("type") == "actions"]


def handle_action(payload: dict, client: Any) -> None:
    """A click on Decline / Approved / Funded under a routed post."""
    user = _user_id(payload)
    channel = str((payload.get("channel") or {}).get("id") or "")
    if user not in allowed_user_ids():
        logger.warning("[routing-slack] user %s is not allowed to act on files", user or "unknown")
        _tell(client, channel, user, "Only approved users can act on routed files.")
        return
    action = payload["actions"][0]
    message = payload.get("message") or {}
    ts, original = str(message.get("ts") or ""), str(message.get("text") or "")
    if action["action_id"] == ACTION_DETAILS:
        _show_details(client, channel, user, int(action.get("value") or 0))
        return
    if action["action_id"] == ACTION_REROUTE:
        _reroute(client, channel, user, ts, original, int(action.get("value") or 0))
        return
    try:
        event_id = int(action.get("value") or 0)
        with lending_session() as db:
            current = _current_route(db, event_id, strict=action["action_id"] == ACTION_DECLINE)
            if current is None:
                _tell(client, channel, user, NOT_CURRENT)
                return
            if action["action_id"] == ACTION_DECLINE:
                view = _decline_view(event_id, current["lender"], channel, ts, original, db)
                client.views_open(trigger_id=payload.get("trigger_id", ""), view=view)
                return
            from src.lending.routing.outcome import AlreadyFunded, StaleRoute, record_outcome

            outcome = _OUTCOME_BUTTONS[action["action_id"]]
            key = current["key"]
            try:
                written = record_outcome(key.id, action=outcome, actor=f"slack:{user}", db=db, kind=key.kind,
                                         routed_event_id=event_id)
            except StaleRoute:  # re-routed between the check above and the locked write
                _tell(client, channel, user, NOT_CURRENT)
                return
            except AlreadyFunded:  # a queued Approved click arriving after Funded
                _tell(client, channel, user, ALREADY_FUNDED)
                return
    except Exception as exc:  # class only: SQL errors can embed bound values
        logger.error("[routing-slack] a routed-post button failed: %s", type(exc).__name__)
        _tell(client, channel, user, "Something went wrong; nothing was changed.")
        return
    note = f":white_check_mark: *{outcome.title()}* by <@{user}>" if written else f"_{outcome.title()} was already recorded._"
    buttons = _buttons_left_after_approval(message) if outcome == "approved" else [details_block(event_id)]
    _update_post(client, channel, ts, original, note, keep_buttons=buttons)


def _run_decline(client: Any, event_id: int, reason: str, details: str, user: str, channel: str, ts: str, original: str) -> None:
    from src.lending.routing.decline import record_decline

    try:
        with lending_session() as db:
            record_decline(event_id, reason_code=reason, actor=f"slack:{user}", db=db, free_text=details or None)
    except Exception as exc:  # class only
        logger.error("[routing-slack] decline failed event=%s: %s", event_id, type(exc).__name__)
        _tell(client, channel, user, "The decline could not be recorded. Nothing was changed.")
        return
    _update_post(client, channel, ts, original, f":x: *Declined* (reason: {reason}) by <@{user}>",
                 keep_buttons=[details_block(event_id)])


def handle_decline_submission(payload: dict, client: Any, run_later: Scheduler) -> dict[str, Any]:
    """The Decline form. A reason is required and 'other' needs details. Checked here, applied via ``run_later``
    (Slack wants an answer within 3 seconds; a decline re-routes the file and posts to Slack)."""
    user = _user_id(payload)
    view = payload.get("view") or {}
    if user not in allowed_user_ids():
        return {"response_action": "errors", "errors": {"reason_b": "You are not allowed to decline files."}}
    values = (view.get("state") or {}).get("values") or {}
    reason, details = _value(values, "reason_b"), _value(values, "text_b")
    if not reason:
        return {"response_action": "errors", "errors": {"reason_b": "A decline needs a reason."}}
    if reason == CATCH_ALL_DECLINE_REASON and not details:
        return {"response_action": "errors", "errors": {"text_b": f"Say why when the reason is '{CATCH_ALL_DECLINE_REASON}'."}}
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
        event_id = int(meta["event_id"])
        with lending_session() as db:
            valid = db.execute(text("SELECT 1 FROM lending.decline_reason_codes WHERE code = :c AND active"), {"c": reason}).scalar()
            current = _current_route(db, event_id, strict=True)
    except Exception as exc:  # class only
        logger.error("[routing-slack] decline form check failed: %s", type(exc).__name__)
        return {"response_action": "errors", "errors": {"reason_b": "Something went wrong; nothing was changed."}}
    if not valid:
        return {"response_action": "errors", "errors": {"reason_b": "That reason is not available any more."}}
    if current is None:
        return {"response_action": "errors", "errors": {"reason_b": NOT_CURRENT}}
    run_later(_run_decline, client, event_id, reason, details, user,
              str(meta.get("channel") or ""), str(meta.get("ts") or ""), str(meta.get("original") or ""))
    return {}
