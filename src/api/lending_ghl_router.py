"""GoHighLevel -> lending opt-out webhook.

A GHL workflow ("Contact DND changed" -> Webhook) POSTs the contact here; the number is
suppressed in every lending store and removed from the dialer, the same as a dialer
"do not call". Auth: ``X-Webhook-Secret`` must equal LENDING_GHL_WEBHOOK_SECRET; the
endpoint is closed while the secret is unset.

Endpoints: POST /webhooks/lending/ghl-opt-out, POST /webhooks/lending/ghl-stage (scoreboard "showed")
"""
from __future__ import annotations

import hmac
import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.settings import get_settings
from src.api.deps import get_db
from src.lending.compliance import propagate_opt_out
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _verify_secret(received: Optional[str]) -> None:
    secret = get_settings().lending_ghl_webhook_secret
    if secret is None:
        raise HTTPException(status_code=503, detail="GHL webhook is not configured")
    if not received or not hmac.compare_digest(received, secret.get_secret_value()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _field(body: dict[str, Any], name: str) -> Optional[str]:
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    value = body.get(name) or contact.get(name)
    return str(value) if value else None


@router.post("/ghl-opt-out")
def ghl_opt_out(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    phone, email = _field(body, "phone"), _field(body, "email")
    contact_id = _field(body, "contact_id") or _field(body, "id")
    if not phone and not email:
        raise HTTPException(status_code=422, detail="phone or email is required")
    try:
        event_id = propagate_opt_out(
            db, phone=phone, email=email, source_ref=f"ghl:{contact_id}" if contact_id else None,
            channel=OptOutChannel.GHL,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] opt-out webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Opt-out could not be recorded") from exc
    return {"recorded": event_id is not None}


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
