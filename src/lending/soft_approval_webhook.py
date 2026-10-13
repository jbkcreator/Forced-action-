"""POST /webhooks/lending/slack-interactivity: the Slack Interactivity Request URL for the soft approval form.

Served by lending-api (nginx /webhooks/lending/). Slack must be in HTTP mode (Socket Mode off) with this
URL set under Interactivity. The same URL also receives Cora's approval-card clicks (``agent_core_*``
action ids), which are handed to Cora before any soft-approval handling. Every request is
signature-checked with LENDING_SLACK_SIGNING_SECRET; the route is closed (503) while the secret is
unset. The form submission is acknowledged at once and the evaluation, rendering and storage run in
a background task.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text

from config.lending_soft_approval import FORM_CALLBACK_ID, OPEN_FORM_ACTION_ID
from config.settings import get_settings
from packages.agent_core.approval import ApprovalAction
from src.lending.cora_runtime import handle_slack_request
from src.lending.db import lending_session
from src.lending.enrichment.service import enabled as enrichment_enabled, enrich_lead, record_deal_facts
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

_CORA_ACTION_IDS = frozenset(action.value for action in ApprovalAction)
SLACK_FORM_SOURCE = "slack_form"  # a value enrichment.webhook.ALLOWED_SOURCES already accepts


def is_cora_click(payload: dict) -> bool:
    """A click on one of Cora's Approve / Revise / Reject card buttons (Cora acts on the first action only)."""
    actions = payload.get("actions") or []
    return payload.get("type") == "block_actions" and bool(actions) and actions[0].get("action_id") in _CORA_ACTION_IDS


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
        channel=str((payload.get("channel") or {}).get("id") or ""),
        message_ts=str(message.get("ts") or ""),
    )
    try:
        slack_client().views_open(trigger_id=payload.get("trigger_id", ""), view=view)
    except Exception as exc:  # class only
        logger.error("[soft-approval] opening the form failed: %s", type(exc).__name__)


def _record_deal_facts(db, phone: str, facts: SoftApprovalFacts) -> Optional[int]:
    """F5: the address and close date the caller entered reach the lead card and routing (source slack_form).
    Never blocks the soft approval; only runs while T-12 enrichment is switched on."""
    if not enrichment_enabled():
        return None
    try:
        with db.begin_nested():
            return record_deal_facts(
                db, phone=phone, address=facts.property_address,
                target_close_date=facts.target_close_date, source=SLACK_FORM_SOURCE)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] recording the deal facts failed: %s", type(exc).__name__)
        return None


def _enrich_after_form(lead_id: int) -> None:
    try:
        with lending_session() as db:
            enrich_lead(db, lead_id)
    except Exception as exc:
        logger.error("[soft-approval] enrichment re-run failed lead=%s: %s", lead_id, type(exc).__name__)


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


def _status_text(outcome) -> str:
    if outcome.status == "generated" and outcome.subject_to_confirmation:
        return OUTCOME_MESSAGES["generated_unconfirmed"]
    return OUTCOME_MESSAGES.get(outcome.reason) or OUTCOME_MESSAGES.get(outcome.status) or ""


def _process_submission(call_id: str, facts: SoftApprovalFacts, user_id: str, channel: str, message_ts: str) -> None:
    status_text = "The soft approval could not be processed. Please tell the engineering team."
    lead_id: Optional[int] = None
    client = slack_client()
    try:
        with lending_session() as db:
            phone = db.execute(
                text("SELECT phone FROM lending.call_dispositions WHERE dialer_call_id = :id"), {"id": call_id},
            ).scalar()
            if not phone:
                status_text = ":warning: That call was not found, so no soft approval was made."
            else:
                lead_id = _record_deal_facts(db, phone, facts)
                outcome = generate_soft_approval(
                    db, phone=phone, dialer_call_id=call_id, facts=facts, submitted_by=user_id or None)
                status_text = _status_text(outcome) or status_text
                if outcome.status == "generated" and outcome.approval_id and channel and message_ts:
                    _upload_pdf(client, db, approval_id=outcome.approval_id, phone=phone,
                                channel=channel, message_ts=message_ts)
    except Exception as exc:  # class only: SQL errors embed bound params (phones)
        logger.error("[soft-approval] call %s submission failed: %s", call_id, type(exc).__name__)
        lead_id = None
    if channel and message_ts:
        try:
            client.chat_postMessage(channel=channel, thread_ts=message_ts, text=status_text)
        except Exception as exc:
            logger.error("[soft-approval] call %s status reply failed: %s", call_id, type(exc).__name__)
    if lead_id is not None:
        _enrich_after_form(lead_id)


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
    try:
        payload: Optional[dict] = json.loads(parse_qs(body.decode()).get("payload", [""])[0])
    except ValueError:
        payload = None
    # The lending app has one Interactivity URL: Cora's approval-card clicks share it. They are routed
    # before the soft-approval switch so turning soft approval off never strands a Cora approval.
    if isinstance(payload, dict) and is_cora_click(payload):
        action = (payload.get("actions") or [{}])[0]
        background.add_task(handle_slack_request, "interactive", payload, str(action.get("action_ts") or ""))
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
