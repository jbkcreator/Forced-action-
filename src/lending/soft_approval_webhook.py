"""POST /webhooks/lending/slack-interactivity: the Slack Interactivity Request URL for the soft approval form.

Served by lending-api (nginx /webhooks/lending/). Slack must be in HTTP mode (Socket Mode off) with this
URL set under Interactivity. Every request is signature-checked with LENDING_SLACK_SIGNING_SECRET; the
route is closed (503) while the secret is unset. The form submission is acknowledged at once and the
evaluation, rendering and storage run in a background task. Cora's approval cards belong to her own
Slack app; a click on a Cora card this app posted before that move retires the card (see
:func:`_retire_moved_cora_card`) and changes nothing. Only the Slack users on the allow-list can open and
submit the form (B11: Josh only until he names callers; :func:`allowed_user_ids`).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text

from config.lending_soft_approval import FORM_CALLBACK_ID, OPEN_FORM_ACTION_ID
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.slack_names import names_by_phone
from src.lending.soft_approval.enrichment_link import share_facts_with_enrichment
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.service import generate_soft_approval
from src.lending.soft_approval.slack_card import (
    OUTCOME_MESSAGES,
    slack_client,
    build_card_blocks,
    build_form_view,
    card_status,
    parse_submission,
    verify_slack_signature,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])

# Approve / Revise / Reject buttons on Cora cards this app posted before Cora had her own app.
_MOVED_CORA_ACTION_PREFIX = "agent_core_"
MOVED_CORA_CARD_TEXT = (":arrows_counterclockwise: *This approval card has moved* to Cora's own Slack app. "
                        "Use the newer card for this action in the Cora channel. Nothing was approved or sent "
                        "from this card.")


def is_moved_cora_click(payload: dict) -> bool:
    actions = payload.get("actions") or []
    return (payload.get("type") == "block_actions" and bool(actions)
            and str(actions[0].get("action_id") or "").startswith(_MOVED_CORA_ACTION_PREFIX))


def _retire_moved_cora_card(payload: dict) -> None:
    """Replace the old card's buttons with a pointer to the new card. The draft itself is untouched:
    Cora reposts open drafts under her own app at startup."""
    channel = str((payload.get("channel") or {}).get("id") or "")
    ts = str((payload.get("message") or {}).get("ts") or "")
    if not (channel and ts):
        return
    try:
        slack_client().chat_update(channel=channel, ts=ts, text=MOVED_CORA_CARD_TEXT, blocks=[
            {"type": "section", "text": {"type": "mrkdwn", "text": MOVED_CORA_CARD_TEXT}}])
    except Exception as exc:  # class only
        logger.error("[soft-approval] retiring a moved Cora card failed: %s", type(exc).__name__)


def allowed_user_ids() -> frozenset[str]:
    """Slack users who may open and submit the form. Fails closed: empty means nobody (B11)."""
    settings = get_settings()
    raw = (getattr(settings, "lending_soft_approval_user_ids", "")
           or getattr(settings, "lending_cora_approver_user_ids", "") or "")
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def _user_id(payload: dict) -> str:
    return str((payload.get("user") or {}).get("id") or "")


def _is_allowed(payload: dict) -> bool:
    user = _user_id(payload)
    allowed = bool(user) and user in allowed_user_ids()
    if not allowed:
        logger.warning("[soft-approval] user %s is not allowed to use the form", user or "unknown")
    return allowed


def _open_form(payload: dict) -> None:
    action = next((a for a in payload.get("actions") or [] if a.get("action_id") == OPEN_FORM_ACTION_ID), None)
    if action is None:
        return
    channel = str((payload.get("channel") or {}).get("id") or "")
    if not _is_allowed(payload):
        try:
            slack_client().chat_postEphemeral(
                channel=channel, user=_user_id(payload), text="Only approved users can enter deal facts.")
        except Exception as exc:  # class only
            logger.error("[soft-approval] denial notice failed: %s", type(exc).__name__)
        return
    message = payload.get("message") or {}
    view = build_form_view(
        call_id=str(action.get("value") or ""),
        channel=channel,
        message_ts=str(message.get("ts") or ""),
    )
    try:
        slack_client().views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[soft-approval] opening the form failed: %s", type(exc).__name__)


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value)


def _upload_pdf(client, db, *, approval_id: int, phone: str, channel: str, message_ts: str) -> None:
    """B7: Josh opens the stored PDF from the card's thread on the call. The file name carries only the last four
    digits; the PDF is already stored, so a failed upload never fails the soft approval."""
    pdf = db.execute(text("SELECT pdf FROM lending.soft_approvals WHERE id = :id"), {"id": approval_id}).scalar()
    if not pdf:
        return
    try:
        client.files_upload_v2(
            channel=channel, thread_ts=message_ts, content=bytes(pdf), filename=f"soft-approval-{_digits(phone)[-4:]}.pdf",
            title="Soft approval summary (internal, not sent to the borrower)")
    except Exception as exc:  # class only
        logger.error("[soft-approval] PDF upload failed approval=%s: %s", approval_id, type(exc).__name__)


def _outcome_key(outcome) -> str:
    if outcome.status == "generated" and outcome.subject_to_confirmation:
        return "generated_unconfirmed"
    return outcome.reason if outcome.reason in OUTCOME_MESSAGES else outcome.status


def _status_text(outcome) -> str:
    return OUTCOME_MESSAGES.get(_outcome_key(outcome)) or ""


def _refresh_card(client, *, channel: str, message_ts: str, call_id: str, phone: str,
                  caller: Optional[str], key: str) -> None:
    """The card itself shows the last result and offers the form again; best-effort (the thread reply and the
    stored PDF are the record)."""
    borrower = None
    try:
        with lending_session() as db:
            borrower = names_by_phone(db, [phone]).get(phone)
    except Exception as exc:  # class only: a failed lookup only drops the name
        logger.warning("[soft-approval] call %s borrower name lookup failed: %s", call_id, type(exc).__name__)
    fallback, blocks = build_card_blocks(
        call_id=call_id, phone=phone, caller=caller, borrower=borrower,
        status=card_status(key, when=datetime.now(timezone.utc)),
        button_label="Re-enter deal facts" if key.startswith("generated") else "Enter deal facts")
    try:
        client.chat_update(channel=channel, ts=message_ts, text=fallback, blocks=blocks)
    except Exception as exc:  # class only
        logger.error("[soft-approval] call %s card update failed: %s", call_id, type(exc).__name__)


def _process_submission(call_id: str, facts: SoftApprovalFacts, user_id: str, channel: str, message_ts: str) -> None:
    status_text = "The soft approval could not be processed. Please tell the engineering team."
    phone = None
    card: Optional[tuple[str, Optional[str], str]] = None  # (phone, caller, outcome key)
    client = slack_client()
    try:
        with lending_session() as db:
            call = db.execute(
                text("SELECT phone, caller_name FROM lending.call_dispositions WHERE dialer_call_id = :id"),
                {"id": call_id},
            ).first()
            phone = call[0] if call else None
            if not phone:
                status_text = ":warning: That call was not found, so no soft approval was made."
            else:
                outcome = generate_soft_approval(
                    db, phone=phone, dialer_call_id=call_id, facts=facts, submitted_by=user_id or None)
                status_text = _status_text(outcome) or status_text
                card = (phone, call[1], _outcome_key(outcome))
                if outcome.status == "generated" and outcome.approval_id and channel and message_ts:
                    _upload_pdf(client, db, approval_id=outcome.approval_id, phone=phone,
                                channel=channel, message_ts=message_ts)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s submission failed: %s", call_id, type(exc).__name__)
    if channel and message_ts:
        try:
            client.chat_postMessage(channel=channel, thread_ts=message_ts, text=status_text)
        except Exception as exc:
            logger.error("[soft-approval] call %s status reply failed: %s", call_id, type(exc).__name__)
        if card is not None:
            _refresh_card(client, channel=channel, message_ts=message_ts, call_id=call_id,
                          phone=card[0], caller=card[1], key=card[2])
    if phone:
        share_facts_with_enrichment(call_id, phone, facts)


def _handle_submission(payload: dict, background: BackgroundTasks) -> dict[str, Any]:
    view = payload.get("view") or {}
    if view.get("callback_id") != FORM_CALLBACK_ID:
        return {}
    if not _is_allowed(payload):
        return {"response_action": "errors", "errors": {"address": "You are not allowed to submit deal facts."}}
    facts, errors = parse_submission((view.get("state") or {}).get("values") or {})
    if facts is None:
        return {"response_action": "errors", "errors": errors}
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
    except ValueError:
        meta = {}
    call_id = str(meta.get("call_id") or "")
    if not call_id:
        return {"response_action": "errors", "errors": {"address": "This form has expired. Open it again from the card."}}
    background.add_task(
        _process_submission, call_id, facts, _user_id(payload),
        str(meta.get("channel") or ""), str(meta.get("message_ts") or ""),
    )
    return {}


@router.post("/slack-interactivity")
async def slack_interactivity(request: Request, background: BackgroundTasks) -> dict[str, Any]:
    secret = get_settings().lending_slack_signing_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Slack interactivity is not configured")
    body = await request.body()
    if not verify_slack_signature(
        secret.get_secret_value(), request.headers.get("X-Slack-Request-Timestamp"),
        request.headers.get("X-Slack-Signature"), body,
    ):
        raise HTTPException(status_code=401, detail="Invalid signature")
    try:
        payload: Optional[dict] = json.loads(parse_qs(body.decode()).get("payload", [""])[0])
    except ValueError:
        payload = None
    # Checked before the soft-approval switch: an old Cora card must never look clickable but dead.
    if isinstance(payload, dict) and is_moved_cora_click(payload):
        background.add_task(_retire_moved_cora_card, payload)
        return {}
    if not get_settings().lending_soft_approval_enabled:
        return {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Malformed payload")
    kind: Optional[str] = payload.get("type")
    if kind == "block_actions":
        await run_in_threadpool(_open_form, payload)
        return {}
    if kind == "view_submission":
        return _handle_submission(payload, background)
    return {}
