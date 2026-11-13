"""GoHighLevel -> lending opt-out and consent webhooks.

A GHL workflow ("Contact DND changed" -> Webhook) POSTs the contact to ``/ghl-opt-out``; the number is
suppressed in every lending store and removed from the dialer, the same as a dialer "do not call". Inbound
texts reach ``/ghl-text-consent``, where free-text opt-outs are caught. Auth: ``X-Webhook-Secret`` must equal
LENDING_GHL_WEBHOOK_SECRET; the endpoints are closed while the secret is unset.

The event handling lives in ``src.lending.ghl_consent_events``. A call that fails after it arrived is saved and
alerted by ``src.lending.webhook_failures`` and replayed until it succeeds, so an opt-out is never lost.

Endpoints: POST /webhooks/lending/ghl-opt-out, POST /webhooks/lending/ghl-text-consent, POST /webhooks/lending/ghl-stage (scoreboard "showed"),
POST /webhooks/lending/ghl-disposition (GHL call outcome; see src.lending.ghl_disposition_match),
POST /webhooks/lending/after-hours-intake (after-hours voicemail / answering service; see src.lending.after_hours_intake)
"""
from __future__ import annotations

import hmac
import json
import logging
from typing import Any, Optional

from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from sqlalchemy import text

from config.settings import get_settings
from src.api.deps import get_db
from src.lending.after_hours_conversation import conversations_enabled
from src.lending.after_hours_intake import record_intake
from src.lending.after_hours_responder import run_send_cycle_now
from src.lending.ghl_disposition_match import (
    InvalidDispositionEvent,
    attach_pending,
    deliver_matched,
    parse_event as parse_disposition_event,
)
from src.lending.ghl_disposition_match import record_event as record_disposition_event
from src.lending.ghl_missed_call import InvalidMissedCall, parse_missed_call, record_missed_call, run_text_back_now
from src.lending.payload_shape import log_shape
from src.lending.ghl_consent_events import InvalidEvent, field as _field, record_ghl_opt_out, record_inbound_consent
from src.lending.webhook_failures import AFTER_HOURS_INTAKE, GHL_OPT_OUT, GHL_TEXT_CONSENT, report_webhook_failure
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


# What a GHL merge field becomes when it does not resolve (the workflow's variable is wrong or empty). Saving such
# a value as a stage would hide the mistake: the event would never match "funded" and nobody would be told.
_UNRESOLVED_VALUES = frozenset({"null", "none", "undefined", "nan"})


def _resolved(body: dict[str, Any], name: str) -> Optional[str]:
    """An optional field, or None when GHL sent an unresolved merge field ("null") in its place."""
    value = _field(body, name)
    return None if value is None or value.strip().lower() in _UNRESOLVED_VALUES else value


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
    if stage.strip().lower() in _UNRESOLVED_VALUES or opportunity_id.strip().lower() in _UNRESOLVED_VALUES:
        logger.warning("[lending-ghl] stage webhook refused: a merge field did not resolve (opportunity_id/stage_name)")
        raise HTTPException(status_code=422, detail="stage_name or opportunity_id did not resolve: check the GHL merge fields")
    try:
        inserted = db.execute(
            text("INSERT INTO lending.ghl_stage_events (ghl_opportunity_id, pipeline_id, stage_name, stage_key, phone, "
                 "booked_by, raw_event) VALUES (:opp, :pipe, :stage, :key, :phone, :by, CAST(:raw AS jsonb)) "
                 "ON CONFLICT (ghl_opportunity_id, stage_key) DO NOTHING"),
            {"opp": opportunity_id, "pipe": _field(body, "pipeline_id"), "stage": stage, "key": stage.strip().lower(),
             "phone": normalize(_resolved(body, "phone")), "by": _resolved(body, "booked_by"), "raw": json.dumps(body)},
        ).rowcount
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] stage webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Stage event could not be recorded") from exc
    return {"recorded": bool(inserted)}


@router.post("/ghl-disposition")
def ghl_disposition(
    body: dict[str, Any],
    background: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow reports the outcome a caller picked for a GHL call. The outcome is stored, matched onto the
    imported call when it is already there (else by the next GHL call import), and a matched call is delivered to
    the Sheet and Slack after the response. UNVERIFIED body (Dev A builds the workflow): contact_id, phone,
    disposition (GHL label, our code or a disp:<CODE> tag), optional call_id, user, called_at, gate_failed."""
    _verify_secret(x_webhook_secret)
    try:
        event = parse_disposition_event(body)
    except InvalidDispositionEvent as exc:
        logger.warning("[lending-ghl] disposition webhook refused: %s", exc.reason)
        raise HTTPException(status_code=422, detail=exc.reason) from None
    try:
        _, duplicate = record_disposition_event(db, event, body)
        matched = attach_pending(db, datetime.now(timezone.utc))
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] disposition webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Call outcome could not be recorded") from exc
    if matched:
        background.add_task(deliver_matched, matched)
    return {"recorded": True, "duplicate": duplicate, "matched": len(matched)}


@router.post("/ghl-missed-call")
def ghl_missed_call(
    body: dict[str, Any],
    background: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """G4: a GHL workflow posts a call nobody picked up the moment it ends; its missed-call text is queued and decided
    right after the response, with every BatchDialer gate (src/lending/ghl_missed_call.py). Answered or inbound calls
    are ignored. UNVERIFIED body (Dev A builds the workflow): call_id, phone, call_status, contact_id, direction,
    user, called_at, ended_at."""
    _verify_secret(x_webhook_secret)
    log_shape("ghl-missed-call", body)
    try:
        call = parse_missed_call(body, datetime.now(timezone.utc))
    except InvalidMissedCall as exc:
        logger.warning("[lending-ghl] missed-call webhook refused: %s", exc.reason)
        raise HTTPException(status_code=422, detail=exc.reason) from None
    if call is None:
        return {"queued": False, "ignored": True}
    try:
        status = record_missed_call(db, call)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] missed-call webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Missed call could not be recorded") from exc
    if status == "pending":
        background.add_task(run_text_back_now)
    return {"queued": status == "pending", "status": status}


@router.post("/after-hours-intake")
def after_hours_intake(
    body: dict[str, Any],
    background: BackgroundTasks,
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A caller's after-hours voicemail (GHL workflow) or answering-service message: logs the call and pins the
    contact for the next morning (src.lending.after_hours_intake). Body: phone, first_name/name,
    message/transcription, received_at, source, call_id."""
    _verify_secret(x_webhook_secret)
    if not get_settings().lending_after_hours_intake_enabled:
        raise HTTPException(status_code=503, detail="After-hours intake is not enabled")
    try:
        recorded = record_intake(db, body)
        db.commit()
    except InvalidEvent as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] after-hours intake failed: %s", type(exc).__name__)
        report_webhook_failure(AFTER_HOURS_INTAKE, body, exc)
        raise HTTPException(status_code=500, detail="Intake could not be recorded") from exc
    if recorded and conversations_enabled():
        background.add_task(run_send_cycle_now)  # the night responder's first message, right after the response
    return {"recorded": recorded}
