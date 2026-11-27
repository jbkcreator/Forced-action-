"""R1 1.6: the Slack notes form for a connected call that has no recording.

The 2:45 PM scorecard posts an "Add notes" button under each such call. The form saves the call's extraction fields
into ``lending.call_extractions`` as ``source='caller_notes'``. It never replaces a transcript extraction, and only
allowed Slack users can open or submit it. Rides on the lending app's ``/webhooks/lending/slack-interactivity``.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Optional

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_dialer_queue import NOTES_CALLBACK_ID, NOTES_OPEN_ACTION_ID
from config.settings import get_settings
from src.lending.call_extraction import CREDIT_BANDS, DEAL_STATUSES, MAX_COMPLETED_PROJECTS, CallExtraction
from src.lending.db import lending_session
from src.lending.soft_approval.slack_card import slack_client
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

SaveResult = Literal["saved", "has_transcript", "unknown_call"]
_REQUIRED = "next_action"
_LIQUIDITY = {"yes": True, "no": False, "unknown": None}
_SELECTS = {
    "deal_status": ("Deal status", DEAL_STATUSES),
    "credit_band": ("Credit (a band only, never a score)", CREDIT_BANDS),
    "has_liquidity": ("Has liquidity", tuple(_LIQUIDITY)),
}
_LIST_FIELDS = ("referral_names", "missing_file_items")
_MESSAGES = {
    "has_transcript": "This call already has a transcript summary, so notes cannot replace it.",
    "unknown_call": "That call was not found, so nothing was saved.",
}

_UPSERT = text("""
    INSERT INTO lending.call_extractions (dialer_call_id, phone, fields, extracted_at, source, entered_by)
    VALUES (:call_id, :phone, :fields, :now, 'caller_notes', :user_id)
    ON CONFLICT (dialer_call_id) DO UPDATE SET
        phone = EXCLUDED.phone, fields = EXCLUDED.fields, extracted_at = EXCLUDED.extracted_at,
        source = 'caller_notes', entered_by = EXCLUDED.entered_by
     WHERE lending.call_extractions.source = 'caller_notes'
""")


def notes_button(dialer_call_id: str) -> dict:
    return {"type": "button", "action_id": NOTES_OPEN_ACTION_ID, "value": dialer_call_id,
            "text": {"type": "plain_text", "text": "Add notes"}}


def _input(block_id: str, label: str, element: dict, *, optional: bool = True) -> dict:
    return {"type": "input", "block_id": block_id, "optional": optional,
            "label": {"type": "plain_text", "text": label}, "element": {**element, "action_id": "value"}}


def _select(block_id: str) -> dict:
    label, options = _SELECTS[block_id]
    return _input(block_id, label, {"type": "static_select", "options": [
        {"text": {"type": "plain_text", "text": o.replace("_", " ")}, "value": o} for o in options]})


def build_notes_view(*, dialer_call_id: str, channel: str, message_ts: str) -> dict:
    lines = {"type": "plain_text_input", "multiline": True}
    return {
        "type": "modal", "callback_id": NOTES_CALLBACK_ID,
        "private_metadata": json.dumps({"call_id": dialer_call_id, "channel": channel, "message_ts": message_ts}),
        "title": {"type": "plain_text", "text": "Call notes"},
        "submit": {"type": "plain_text", "text": "Save notes"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            _input("next_action", "Next action", {"type": "plain_text_input", "max_length": 200}, optional=False),
            _input("deadline", "Deadline", {"type": "datepicker"}),
            _input("decision_maker", "Decision maker", {"type": "plain_text_input", "max_length": 120}),
            _input("objection", "Objection", {**lines, "max_length": 280}),
            _select("deal_status"),
            _select("credit_band"),
            _input("completed_projects_3y", "Projects completed in the last 3 years",
                   {"type": "number_input", "is_decimal_allowed": False, "min_value": "0",
                    "max_value": str(MAX_COMPLETED_PROJECTS)}),
            _select("has_liquidity"),
            _input("property_address", "Property address", {"type": "plain_text_input", "max_length": 200}),
            _input("target_market", "Target market", {"type": "plain_text_input", "max_length": 120}),
            _input("referral_names", "Referrals (one per line)", lines),
            _input("missing_file_items", "Missing file items (one per line)", lines),
        ],
    }


def _read(values: Mapping[str, Any], block_id: str) -> Optional[str]:
    element = (values.get(block_id) or {}).get("value") or {}
    raw = element.get("value")
    if raw is None:
        raw = element.get("selected_date")
    if raw is None and isinstance(element.get("selected_option"), dict):
        raw = element["selected_option"].get("value")
    return raw.strip() if isinstance(raw, str) and raw.strip() else None


def parse_notes(state_values: Mapping[str, Any]) -> tuple[Optional[CallExtraction], dict[str, str]]:
    """The validated extraction, or (None, {block_id: message}) for Slack to show inline."""
    fields: dict[str, Any] = {name: _read(state_values, name) for name in (
        "next_action", "deadline", "decision_maker", "objection", "deal_status", "credit_band",
        "property_address", "target_market")}
    for name in _LIST_FIELDS:
        fields[name] = [line for line in (_read(state_values, name) or "").splitlines()]
    liquidity = _read(state_values, "has_liquidity")
    fields["has_liquidity"] = _LIQUIDITY.get(liquidity) if liquidity else None
    errors: dict[str, str] = {}
    projects = _read(state_values, "completed_projects_3y")
    try:
        fields["completed_projects_3y"] = int(projects) if projects else None
        if projects and not 0 <= fields["completed_projects_3y"] <= MAX_COMPLETED_PROJECTS:
            raise ValueError
    except ValueError:
        errors["completed_projects_3y"] = f"Enter a whole number from 0 to {MAX_COMPLETED_PROJECTS}."
    if not fields["next_action"]:
        errors[_REQUIRED] = "This field is required."
    try:
        return (None, errors) if errors else (CallExtraction(**fields), {})
    except ValidationError:
        return None, {_REQUIRED: "Enter valid notes."}


def allowed_note_users() -> frozenset[str]:
    s = get_settings()
    raw = f"{s.lending_david_slack_user_id},{s.lending_cora_approver_user_ids}"
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def save_notes(db: Session, *, dialer_call_id: str, extraction: CallExtraction, user_id: str,
               now: datetime) -> SaveResult:
    call = db.execute(text("SELECT phone FROM lending.call_dispositions WHERE dialer_call_id = :id"),
                      {"id": dialer_call_id}).first()
    if call is None:
        return "unknown_call"
    saved = db.execute(_UPSERT, {
        "call_id": dialer_call_id, "phone": normalize_phone(call[0]) if call[0] else None,
        "fields": extraction.model_dump_json(), "now": now, "user_id": user_id})
    return "saved" if saved.rowcount else "has_transcript"  # the WHERE leaves a transcript row untouched


def _open(payload: dict, action: dict) -> None:
    user = str((payload.get("user") or {}).get("id") or "")
    channel = str((payload.get("channel") or {}).get("id") or "")
    try:
        if user not in allowed_note_users():
            logger.warning("[call-notes] user %s is not allowed to add notes", user or "unknown")
            slack_client().chat_postEphemeral(channel=channel, user=user, text="Only approved users can add call notes.")
            return
        view = build_notes_view(dialer_call_id=str(action.get("value") or ""), channel=channel,
                                message_ts=str((payload.get("message") or {}).get("ts") or ""))
        slack_client().views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[call-notes] opening the form failed: %s", type(exc).__name__)


def _submit(payload: dict) -> dict:
    view = payload["view"]
    user = str((payload.get("user") or {}).get("id") or "")
    if user not in allowed_note_users():
        logger.warning("[call-notes] user %s is not allowed to submit notes", user or "unknown")
        return {"response_action": "errors", "errors": {_REQUIRED: "You are not allowed to add call notes."}}
    extraction, errors = parse_notes((view.get("state") or {}).get("values") or {})
    if extraction is None:
        return {"response_action": "errors", "errors": errors}
    try:
        call_id = str(json.loads(view.get("private_metadata") or "{}").get("call_id") or "")
    except ValueError:
        call_id = ""
    if not call_id:
        return {"response_action": "errors", "errors": {_REQUIRED: "This form has expired. Open it again."}}
    try:
        with lending_session() as db:
            result = save_notes(db, dialer_call_id=call_id, extraction=extraction, user_id=user,
                                now=datetime.now(timezone.utc))
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[call-notes] saving failed: %s", type(exc).__name__)
        return {"response_action": "errors", "errors": {_REQUIRED: "Notes could not be saved. Try again."}}
    if result == "saved":
        logger.info("[call-notes] notes saved for call %s by %s", call_id, user)
        return {}
    return {"response_action": "errors", "errors": {_REQUIRED: _MESSAGES[result]}}


def handle_notes_interaction(payload: dict) -> Optional[dict]:
    """The Slack response for a notes payload; None when the payload is not the notes form's."""
    action = next((a for a in payload.get("actions") or [] if a.get("action_id") == NOTES_OPEN_ACTION_ID), None)
    if payload.get("type") == "block_actions" and action is not None:
        if get_settings().lending_caller_scorecard_enabled:
            _open(payload, action)
        return {}
    if payload.get("type") == "view_submission" and (payload.get("view") or {}).get("callback_id") == NOTES_CALLBACK_ID:
        return _submit(payload) if get_settings().lending_caller_scorecard_enabled else {}
    return None
