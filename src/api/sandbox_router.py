"""
Sandbox admin endpoints — inspect sandbox_outbox + simulate webhooks.

Mounted at /api/admin/sandbox. Reuses the same JWT bearer auth as the
regular admin endpoints. All endpoints return 503 if TWILIO_SANDBOX (or the
relevant sandbox flag) is disabled, so production deployments with sandbox
off effectively disable this router.

Endpoints:
  GET  /api/admin/sandbox/outbox                — list captured messages
  POST /api/admin/sandbox/simulate-inbound      — Twilio-shape inbound SMS
  POST /api/admin/sandbox/simulate-nws-alert    — CAP payload → NWS handler
  POST /api/admin/sandbox/simulate-stripe-event — scripted webhook event
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import desc
from sqlalchemy.orm import Session

from config.settings import settings
from src.api.admin_router import get_current_admin, get_db
from src.core.models import SandboxOutbox

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/sandbox", tags=["admin-sandbox"])


def _require_sandbox_mode() -> None:
	if not settings.twilio_sandbox and not settings.redis_sandbox:
		raise HTTPException(
			status_code=503,
			detail="Sandbox endpoints are disabled. Set TWILIO_SANDBOX=true or REDIS_SANDBOX=true.",
		)


# ──────────────────────────────────────────────────────────────────────────────
# GET /outbox
# ──────────────────────────────────────────────────────────────────────────────

class OutboxRow(BaseModel):
	id: int
	channel: str
	to_number: Optional[str]
	body: str
	campaign: Optional[str]
	variant_id: Optional[str]
	subscriber_id: Optional[int]
	decision_id: Optional[str]
	compliance_allowed: bool
	compliance_reason: Optional[str]
	would_have_delivered: bool
	sandbox_flag: str
	created_at: datetime


@router.get("/outbox", response_model=List[OutboxRow])
def list_outbox(
	subscriber_id: Optional[int] = Query(None, description="Filter by subscriber"),
	campaign: Optional[str] = Query(None, description="Filter by campaign"),
	decision_id: Optional[str] = Query(None, description="Filter by Cora decision_id"),
	channel: Optional[str] = Query(None, pattern="^(sms|voice|email)$"),
	limit: int = Query(50, ge=1, le=500),
	admin: dict = Depends(get_current_admin),
	db: Session = Depends(get_db),
) -> List[OutboxRow]:
	"""
	Return recent sandbox_outbox rows, newest first. Filterable by subscriber,
	campaign, decision, channel.
	"""
	_require_sandbox_mode()

	q = db.query(SandboxOutbox)
	if subscriber_id is not None:
		q = q.filter(SandboxOutbox.subscriber_id == subscriber_id)
	if campaign is not None:
		q = q.filter(SandboxOutbox.campaign == campaign)
	if decision_id is not None:
		q = q.filter(SandboxOutbox.decision_id == decision_id)
	if channel is not None:
		q = q.filter(SandboxOutbox.channel == channel)

	rows = q.order_by(desc(SandboxOutbox.created_at)).limit(limit).all()
	return [OutboxRow.model_validate(r, from_attributes=True) for r in rows]


# ──────────────────────────────────────────────────────────────────────────────
# POST /simulate-inbound — Twilio-shape inbound SMS
# ──────────────────────────────────────────────────────────────────────────────

class SimulatedInbound(BaseModel):
	from_number: str    # E.164
	body: str
	to_number: Optional[str] = None


@router.post("/simulate-inbound")
def simulate_inbound(
	payload: SimulatedInbound,
	admin: dict = Depends(get_current_admin),
	db: Session = Depends(get_db),
) -> Dict[str, Any]:
	"""
	Simulate an inbound Twilio SMS. Calls the exact same handler the real
	Twilio webhook uses — STOP routes to opt-out, product commands route to
	the dispatcher, YES routes to opt-in recording.

	Returns the TwiML reply (or None), plus a preview of what the real
	webhook would have done.
	"""
	_require_sandbox_mode()
	from src.services import sms_compliance, sms_commands

	# 1. STOP keywords first (same as production path)
	stop_reply = sms_compliance.handle_inbound(payload.from_number, payload.body, db)
	if stop_reply is not None:
		return {"handled": "stop", "reply": stop_reply}

	# 2. Opt-in keywords (YES / START / JOIN / SUBSCRIBE / UNSTOP)
	opt_in_reply = sms_compliance.handle_opt_in_reply(payload.from_number, payload.body, db)
	if opt_in_reply is not None:
		return {"handled": "opt_in", "reply": opt_in_reply}

	# 3. Product commands (BALANCE / LOCK / BOOST / ...)
	if hasattr(sms_commands, "dispatch"):
		cmd_reply = sms_commands.dispatch(payload.from_number, payload.body, db)
		if cmd_reply:
			return {"handled": "command", "reply": cmd_reply}

	return {"handled": "unmatched", "reply": None}


# ──────────────────────────────────────────────────────────────────────────────
# POST /simulate-nws-alert
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/simulate-nws-alert")
def simulate_nws_alert(
	payload: Dict[str, Any] = Body(...),
	admin: dict = Depends(get_current_admin),
	db: Session = Depends(get_db),
) -> Dict[str, Any]:
	"""
	POST a synthetic CAP payload directly to the NWS handler so scenarios can
	trigger Storm Pack end-to-end without a real weather event.

	Expected payload shape matches src.services.nws_webhook.process_alert input.
	"""
	_require_sandbox_mode()
	from src.services import nws_webhook

	if not hasattr(nws_webhook, "process_alert"):
		raise HTTPException(
			status_code=501,
			detail="NWS webhook handler does not expose process_alert(); update handler or use POST /webhooks/nws/alert directly",
		)

	result = nws_webhook.process_alert(payload, db)
	return {"handled": "nws_alert", "result": result}


# ──────────────────────────────────────────────────────────────────────────────
# POST /simulate-stripe-event
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/simulate-stripe-event")
def simulate_stripe_event(
	payload: Dict[str, Any] = Body(...),
	admin: dict = Depends(get_current_admin),
	db: Session = Depends(get_db),
) -> Dict[str, Any]:
	"""
	Simulate a Stripe webhook event. Calls the same handler as the real
	webhook so idempotency, processed_events dedup, and downstream
	side-effects all exercise production code paths.

	Payload must be a Stripe-style event dict with at least `id` and `type`.
	"""
	_require_sandbox_mode()
	from src.services import stripe_webhooks

	if not hasattr(stripe_webhooks, "handle_event"):
		raise HTTPException(
			status_code=501,
			detail="Stripe webhook handler does not expose handle_event(); route via POST /webhooks/stripe directly",
		)

	result = stripe_webhooks.handle_event(payload, db)
	return {"handled": "stripe_event", "result": result}


# ──────────────────────────────────────────────────────────────────────────────
# POST /dispatch-event — fire any Cora event directly
# ──────────────────────────────────────────────────────────────────────────────

class DispatchEventRequest(BaseModel):
	event_type: str
	subscriber_id: Optional[int] = None
	payload: Optional[Dict[str, Any]] = None
	decision_id: Optional[str] = None
	idempotency_key: Optional[str] = None


@router.post("/dispatch-event")
def dispatch_event_endpoint(
	req: DispatchEventRequest,
	admin: dict = Depends(get_current_admin),
) -> Dict[str, Any]:
	"""
	Fire a Cora event directly into the supervisor.

	Intended for Stage 1 server narratives where waiting for the real
	trigger (Redis TTL expiry, NWS CAP feed, cron schedule) is impractical.
	Routes through the same `dispatch_event` used by real event sources, so
	the supervisor enforces kill switches, idempotency, and audit logging
	exactly as in production.

	event_type must be a key in src.agents.router.EVENT_TO_GRAPH.

	Example body:
		{
			"event_type": "wall_session_abandoned",
			"subscriber_id": 42,
			"payload": {
				"zip_code": "33647",
				"vertical": "roofing",
				"minutes_elapsed": 12,
				"wall_countdown_minutes": 3
			}
		}
	"""
	_require_sandbox_mode()
	from src.agents.events.ingestion import ingest_admin_event

	return ingest_admin_event({
		"event_type": req.event_type,
		"subscriber_id": req.subscriber_id,
		"payload": req.payload or {},
		"decision_id": req.decision_id,
		"idempotency_key": req.idempotency_key,
	})
