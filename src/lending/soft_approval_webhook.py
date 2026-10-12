"""POST /webhooks/lending/slack-interactivity: the Slack Interactivity Request URL for the soft approval form.

Served by lending-api (nginx /webhooks/lending/). Slack must be in HTTP mode (Socket Mode off) with this
URL set under Interactivity. Every request is signature-checked with LENDING_SLACK_SIGNING_SECRET; the
route is closed (503) while the secret is unset. The form submission is acknowledged at once and the
evaluation, rendering and storage run in a background task.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text

from config.lending_soft_approval import FORM_CALLBACK_ID, OPEN_FORM_ACTION_ID
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.soft_approval.facts import SoftApprovalFacts
from src.lending.soft_approval.service import generate_soft_approval
from src.lending.soft_approval.slack_card import (
    OUTCOME_MESSAGES,
    slack_client,
    build_form_view,
    parse_submission,
    verify_slack_signature,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _open_form(payload: dict) -> None:
    action = next((a for a in payload.get("actions") or [] if a.get("action_id") == OPEN_FORM_ACTION_ID), None)
    if action is None:
        return
    message = payload.get("message") or {}
    view = build_form_view(
        call_id=str(action.get("value") or ""),
        channel=str((payload.get("channel") or {}).get("id") or ""),
        message_ts=str(message.get("ts") or ""),
    )
    try:
        slack_client().views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[soft-approval] opening the form failed: %s", type(exc).__name__)


def _process_submission(call_id: str, facts: SoftApprovalFacts, user_id: str, channel: str, message_ts: str) -> None:
    status_text = "The soft approval could not be processed. Please tell the engineering team."
    try:
        with lending_session() as db:
            phone = db.execute(
                text("SELECT phone FROM lending.call_dispositions WHERE dialer_call_id = :id"), {"id": call_id},
            ).scalar()
            if not phone:
                status_text = ":warning: That call was not found, so no soft approval was made."
            else:
                outcome = generate_soft_approval(
                    db, phone=phone, dialer_call_id=call_id, facts=facts, submitted_by=user_id or None)
                status_text = OUTCOME_MESSAGES.get(outcome.status, status_text)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s submission failed: %s", call_id, type(exc).__name__)
    if channel and message_ts:
        try:
            slack_client().chat_postMessage(channel=channel, thread_ts=message_ts, text=status_text)
        except Exception as exc:
            logger.error("[soft-approval] call %s status reply failed: %s", call_id, type(exc).__name__)


def _handle_submission(payload: dict, background: BackgroundTasks) -> dict[str, Any]:
    view = payload.get("view") or {}
    if view.get("callback_id") != FORM_CALLBACK_ID:
        return {}
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
        _process_submission, call_id, facts, str((payload.get("user") or {}).get("id") or ""),
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
    if not get_settings().lending_soft_approval_enabled:
        return {}
    try:
        payload = json.loads(parse_qs(body.decode()).get("payload", [""])[0])
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed payload") from None
    kind: Optional[str] = payload.get("type")
    if kind == "block_actions":
        await run_in_threadpool(_open_form, payload)
        return {}
    if kind == "view_submission":
        return _handle_submission(payload, background)
    return {}
