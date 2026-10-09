"""Slack side of the soft approval: the per-call card, the after-call form and the submission parser.

The card goes to ``LENDING_DIAL_TASKS_CHANNEL`` (#dial-tasks, next to the call's result message) once per finished call, with a button that opens the form.
The caller enters only the property facts; credit, loan type and amount come from the LendingFlow lead
(see lead_source.py). Anyone who can see the card can submit (decided with the team, recorded in the PR);
every submission stores the submitter's Slack user id.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Any, Mapping, Optional

from pydantic import ValidationError
from sqlalchemy import text

from config.lending_dispositions import DNC_CODE
from config.lending_soft_approval import FORM_CALLBACK_ID, OPEN_FORM_ACTION_ID
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dispositions import last4
from src.lending.soft_approval.facts import SoftApprovalFacts

logger = logging.getLogger(__name__)

SIGNATURE_TOLERANCE_SECONDS = 300

# modal block id -> field name on SoftApprovalFacts
_BLOCKS = {
    "address": "property_address",
    "purchase": "purchase_price",
    "rehab": "rehab_budget",
    "arv": "arv",
    "ptype": "property_type",
    "close": "target_close_date",
}
_FIELD_TO_BLOCK = {field: block for block, field in _BLOCKS.items()}

OUTCOME_MESSAGES = {
    "generated": ":white_check_mark: Soft approval PDF is ready and stored for this lead.",
    "no_lead": ":warning: No LendingFlow lead was found for this call, so no soft approval was made.",
    "no_fit": ":no_entry_sign: No lender fits, so nothing was made.",
    "out_of_scope": ":no_entry_sign: This loan type is not covered by the soft approval yet, so nothing was made.",
    "terms_unconfirmed": ":warning: The matching lender's calculation inputs are not confirmed yet, so nothing was made.",
    "render_failed": ":x: The PDF could not be generated. Please tell the engineering team.",
}


def verify_slack_signature(
    secret: str, timestamp: Optional[str], signature: Optional[str], body: bytes, *, now: Optional[float] = None,
) -> bool:
    """Slack's v0 request signature. False for a missing header, a stale timestamp or a bad digest."""
    if not secret or not timestamp or not signature:
        return False
    try:
        age = abs((now if now is not None else time.time()) - int(timestamp))
    except ValueError:
        return False
    if age > SIGNATURE_TOLERANCE_SECONDS:
        return False
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def build_card_blocks(*, call_id: str, phone: Optional[str], caller: Optional[str]) -> tuple[str, list[dict]]:
    summary = f"Call {call_id} · {last4(phone)} · caller {caller or '—'}"
    fallback = f"Enter deal facts for the soft approval — {summary}"
    return fallback, [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":house: *Enter deal facts after the call*\n{summary}"}},
        {"type": "actions", "elements": [{
            "type": "button", "style": "primary", "action_id": OPEN_FORM_ACTION_ID, "value": call_id,
            "text": {"type": "plain_text", "text": "Enter deal facts"},
        }]},
    ]


def _input(block_id: str, label: str, element: dict, *, optional: bool = False) -> dict:
    element = {**element, "action_id": "value"}
    return {"type": "input", "block_id": block_id, "optional": optional,
            "label": {"type": "plain_text", "text": label}, "element": element}


def _money_input(block_id: str, label: str) -> dict:
    return _input(block_id, label, {"type": "number_input", "is_decimal_allowed": True, "min_value": "0"})


def build_form_view(*, call_id: str, channel: str, message_ts: str) -> dict:
    return {
        "type": "modal",
        "callback_id": FORM_CALLBACK_ID,
        "private_metadata": json.dumps({"call_id": call_id, "channel": channel, "message_ts": message_ts}),
        "title": {"type": "plain_text", "text": "Deal facts"},
        "submit": {"type": "plain_text", "text": "Create PDF"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            _input("address", "Property address", {"type": "plain_text_input", "max_length": 200}),
            _money_input("purchase", "Purchase price ($)"),
            _money_input("rehab", "Rehab budget ($)"),
            _money_input("arv", "Estimated ARV ($)"),
            _input("ptype", "Property type", {"type": "plain_text_input", "max_length": 60}, optional=True),
            _input("close", "Target close date", {"type": "datepicker"}, optional=True),
        ],
    }


def _read_value(state_values: Mapping[str, Any], block_id: str) -> Optional[str]:
    element = (state_values.get(block_id) or {}).get("value") or {}
    raw = element.get("value") if element.get("value") is not None else element.get("selected_date")
    return raw.strip() if isinstance(raw, str) and raw.strip() else None


def parse_submission(state_values: Mapping[str, Any]) -> tuple[Optional[SoftApprovalFacts], dict[str, str]]:
    """The validated facts, or (None, {block_id: message}) for Slack to show inline."""
    raw = {field: _read_value(state_values, block) for block, field in _BLOCKS.items()}
    try:
        return SoftApprovalFacts(**raw), {}
    except ValidationError as exc:
        errors: dict[str, str] = {}
        for err in exc.errors():
            field = str(err["loc"][0]) if err["loc"] else ""
            block = _FIELD_TO_BLOCK.get(field)
            if block and block not in errors:
                errors[block] = "Enter a valid value." if raw.get(field) else "This field is required."
        return None, errors or {"address": "Enter valid deal facts."}


def slack_client():
    from slack_sdk import WebClient

    token = get_settings().lending_slack_bot_token
    return WebClient(token=token.get_secret_value() if token else None)


def post_soft_approval_card(row_id: int, client: Any = None) -> None:
    """Post the card for a finished call, once. Best-effort: a failure is logged and the call log is untouched."""
    settings = get_settings()
    if not settings.lending_soft_approval_enabled:
        return
    channel = settings.lending_dial_tasks_channel
    if not channel or not settings.lending_slack_bot_token:
        logger.warning("[soft-approval] card not posted: LENDING_DIAL_TASKS_CHANNEL or LENDING_SLACK_BOT_TOKEN is not set")
        return
    try:
        with lending_session() as db:
            row = db.execute(
                text("SELECT dialer_call_id, phone, caller_name, caller_seat, talk_duration_sec, disposition "
                     "FROM lending.call_dispositions WHERE id = :id"),
                {"id": row_id},
            ).first()
            if row is None or not row.phone or not (row.talk_duration_sec or 0) > 0 or row.disposition == DNC_CODE:
                return
            claimed = db.execute(
                text("INSERT INTO lending.soft_approval_cards (dialer_call_id, phone, channel) "
                     "VALUES (:call_id, :phone, :channel) ON CONFLICT (dialer_call_id) DO NOTHING RETURNING id"),
                {"call_id": row.dialer_call_id, "phone": row.phone, "channel": channel},
            ).scalar()
            if claimed is None:
                return
            fallback, blocks = build_card_blocks(
                call_id=row.dialer_call_id, phone=row.phone, caller=row.caller_name or row.caller_seat)
            posted = (client or slack_client()).chat_postMessage(channel=channel, text=fallback, blocks=blocks)
            db.execute(text("UPDATE lending.soft_approval_cards SET message_ts = :ts WHERE id = :id"),
                       {"ts": posted["ts"], "id": claimed})
    except Exception as exc:  # class only: Slack and SQL errors can carry the phone number
        logger.error("[soft-approval] card for call row %s failed: %s", row_id, type(exc).__name__)
