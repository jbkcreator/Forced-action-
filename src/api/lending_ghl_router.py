"""GoHighLevel -> lending opt-out and consent webhooks.

A GHL workflow ("Contact DND changed" -> Webhook) POSTs the contact to ``/ghl-opt-out``; the number is
suppressed in every lending store and removed from the dialer, the same as a dialer "do not call". Inbound
texts reach ``/ghl-text-consent``, where free-text opt-outs are caught. Auth: ``X-Webhook-Secret`` must equal
LENDING_GHL_WEBHOOK_SECRET; the endpoints are closed while the secret is unset.

The event handling lives in ``src.lending.ghl_consent_events``. A call that fails after it arrived is saved and
alerted by ``src.lending.webhook_failures`` and replayed until it succeeds, so an opt-out is never lost.

Endpoints: POST /webhooks/lending/ghl-opt-out, POST /webhooks/lending/ghl-text-consent, POST /webhooks/lending/ghl-stage (scoreboard "showed"),
POST /webhooks/lending/after-hours-intake (T-06 after-hours voicemail / answering service; see src.lending.after_hours_intake)
"""
from __future__ import annotations

import hmac
import json
import logging
from datetime import timedelta
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from sqlalchemy import text

from config.settings import get_settings
from src.api.deps import get_db
from src.lending.after_hours_intake import IntakeEvent, IntakeResult, model_drafter, queue_reply_draft, record_intake
from src.lending.ghl_consent_events import InvalidEvent, field as _field, record_ghl_opt_out, record_inbound_consent
from src.lending.webhook_failures import GHL_OPT_OUT, GHL_TEXT_CONSENT, report_webhook_failure
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _verify_secret(received: Optional[str]) -> None:
    secret = get_settings().lending_ghl_webhook_secret
    if secret is None:
        raise HTTPException(status_code=503, detail="GHL webhook is not configured")
    if not received or not hmac.compare_digest(received, secret.get_secret_value()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


@router.post("/ghl-opt-out")
def ghl_opt_out(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    try:
        event_id = record_ghl_opt_out(db, body)
        db.commit()
    except InvalidEvent as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] opt-out webhook failed: %s", type(exc).__name__)
        report_webhook_failure(GHL_OPT_OUT, body, exc)
        raise HTTPException(status_code=500, detail="Opt-out could not be recorded") from exc
    return {"recorded": event_id is not None}


@router.post("/ghl-text-consent")
def ghl_text_consent(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow reports text consent. ``web_form``: the lead form's consent box (only a checked
    box counts). ``inbound_text``: the contact texted the Next Deal Lending number; a hard opt-out
    revokes and suppresses, a soft decline only revokes; an empty or non-text reply records nothing. Body: source, phone, consent (web_form), message (inbound_text), contact_id."""
    _verify_secret(x_webhook_secret)
    try:
        result = record_inbound_consent(db, body)
        db.commit()
    except InvalidEvent as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] consent webhook failed: %s", type(exc).__name__)
        report_webhook_failure(GHL_TEXT_CONSENT, body, exc)
        raise HTTPException(status_code=500, detail="Consent could not be recorded") from exc
    return result


@router.post("/ghl-stage")
def ghl_stage(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow ("Pipeline Stage Changed" -> Webhook) reports an opportunity entering a stage.
    Body: opportunity_id, stage_name, optional pipeline_id, phone, booked_by."""
    _verify_secret(x_webhook_secret)
    opportunity_id, stage = _field(body, "opportunity_id") or _field(body, "id"), _field(body, "stage_name")
    if not opportunity_id or not stage:
        raise HTTPException(status_code=422, detail="opportunity_id and stage_name are required")
    try:
        inserted = db.execute(
            text("INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, pipeline_id, stage_name, stage_key, phone, "
                 "booked_by, raw_event) VALUES (:opp, :pipe, :stage, :key, :phone, :by, CAST(:raw AS jsonb)) "
                 "ON CONFLICT (ghl_opportunity_id, stage_key) DO NOTHING"),
            {"opp": opportunity_id, "pipe": _field(body, "pipeline_id"), "stage": stage, "key": stage.strip().lower(),
             "phone": normalize(_field(body, "phone")), "by": _field(body, "booked_by"), "raw": json.dumps(body)},
        ).rowcount
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] stage webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Stage event could not be recorded") from exc
    return {"recorded": bool(inserted)}


def _queue_after_hours_draft(event: IntakeEvent, result: IntakeResult) -> None:
    """Background: draft the reply and freeze it for Approve. A failure leaves the logged call and the pin."""
    import anthropic
    from sqlalchemy import create_engine

    from packages.agent_core.pending_actions import PendingActionQueue
    from packages.agent_core.store import AgentStore
    from src.lending.models import LENDING_SCHEMA

    settings = get_settings()
    engine = create_engine(str(settings.database_url), pool_pre_ping=True)
    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key.get_secret_value(), timeout=30.0,
                                     max_retries=1).messages
        queue = PendingActionQueue(AgentStore(engine, LENDING_SCHEMA),
                                   default_ttl=timedelta(hours=settings.lending_cora_pending_action_ttl_hours))
        action_id = queue_reply_draft(queue, event, result, model_drafter(client, settings.lending_cora_model))
        logger.info("[lending-ghl] after-hours reply draft queued: action %s", action_id)
    except Exception as exc:
        logger.error("[lending-ghl] after-hours reply draft failed (%s)", type(exc).__name__)
    finally:
        engine.dispose()


@router.post("/after-hours-intake")
def after_hours_intake(
    body: dict[str, Any],
    background_tasks: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A caller's after-hours voicemail (GHL workflow) or answering-service message. Logs the call, pins the
    contact first for the next morning and queues a reply draft for Approve. Body: phone, first_name/name,
    message/transcription, received_at, source, call_id."""
    if not get_settings().lending_after_hours_intake_enabled:
        raise HTTPException(status_code=503, detail="After-hours intake is not enabled")
    _verify_secret(x_webhook_secret)
    try:
        event, result = record_intake(db, body)
        db.commit()
    except InvalidEvent as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] after-hours intake failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Intake could not be recorded") from exc
    if result.recorded and result.draft_allowed:
        background_tasks.add_task(_queue_after_hours_draft, event, result)
    return {"recorded": result.recorded, "pinned": result.recorded, "draft_queued": result.draft_allowed}
