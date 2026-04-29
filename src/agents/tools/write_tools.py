"""
Write tools for Cora graphs — priority-list scope.

Only two write tools are needed to close the four priority-list LangGraph
items (supervisor, FOMO, abandonment, retention):

	send_sms         — emit an SMS through the compliance-gated outbound path
	log_decision     — write one agent_decisions row per graph run

Both tools are idempotent. send_sms short-circuits when a duplicate
(subscriber, campaign, variant) send has occurred inside the 24-hour window.
log_decision uses the decision_id UUID as the primary key so a duplicate
call with the same decision_id merges into the existing row rather than
inserting a second.

Write tools deliberately stay thin — business logic stays in
src/services/*. These are the single entry points agents use to mutate
state so policy enforcement (compliance, audit) can live in one place.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, Optional

from sqlalchemy.orm import Session

from src.agents.tools.registry import tool
from src.core.database import db
from src.core.models import AgentDecision, MessageOutcome, Subscriber, SmsOptIn

logger = logging.getLogger(__name__)


@contextmanager
def _session(provided: Optional[Session]) -> Generator[Session, None, None]:
	if provided is not None:
		yield provided
		return
	with db.session_scope() as s:
		yield s


# ──────────────────────────────────────────────────────────────────────────────
# send_sms
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="write", idempotent=True, requires_compliance=True)
def send_sms(
	subscriber_id: int,
	body: str,
	campaign: str,
	variant_id: Optional[str] = None,
	decision_id: Optional[str] = None,
	message_type: str = "marketing",
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Send an SMS through the compliance gate. Records a MessageOutcome row on
	success. Idempotent within a 24-hour window by (subscriber_id, campaign,
	variant_id) — a duplicate call returns the prior send's result without
	re-dispatching to Twilio.

	Behaviour:
	  1. Load subscriber's phone from the latest SmsOptIn row
	  2. Resolve idempotency: if a recent MessageOutcome row matches, return it
	  3. Call src.services.sms_compliance.send_sms — runs the opt-out gate
		 internally, writes to DLQ on failure, dry-runs if TWILIO_ENABLED=false
	  4. Record a MessageOutcome row keyed by campaign + variant_id

	Returns:
	  {
		'sent': bool,              # True on success (including dry-run)
		'reason': str,             # 'ok' | 'no_phone' | 'opted_out' | 'twilio_error' | 'duplicate'
		'subscriber_id': int,
		'campaign': str,
		'variant_id': str | None,
		'message_outcome_id': int | None,
	  }
	"""
	from src.services import sms_compliance

	with _session(session) as s:
		# 1. Resolve phone via the most recent opt-in (subscriber has no direct phone column).
		opt_in = (
			s.query(SmsOptIn)
			.filter(SmsOptIn.subscriber_id == subscriber_id)
			.order_by(SmsOptIn.opted_in_at.desc())
			.first()
		)
		if opt_in is None:
			return {
				"sent": False,
				"reason": "no_phone",
				"subscriber_id": subscriber_id,
				"campaign": campaign,
				"variant_id": variant_id,
				"message_outcome_id": None,
			}
		phone = opt_in.phone

		# 2. Idempotency — look for a matching MessageOutcome in the last 24h.
		cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
		dup_q = (
			s.query(MessageOutcome)
			.filter(MessageOutcome.subscriber_id == subscriber_id)
			.filter(MessageOutcome.template_id == campaign)
			.filter(MessageOutcome.sent_at >= cutoff)
		)
		if variant_id:
			dup_q = dup_q.filter(MessageOutcome.variant_id == variant_id)
		duplicate = dup_q.first()
		if duplicate is not None:
			logger.info(
				"send_sms: duplicate skipped (subscriber=%s campaign=%s variant=%s outcome=%s)",
				subscriber_id, campaign, variant_id, duplicate.id,
			)
			return {
				"sent": False,
				"reason": "duplicate",
				"subscriber_id": subscriber_id,
				"campaign": campaign,
				"variant_id": variant_id,
				"message_outcome_id": duplicate.id,
			}

		# 3. Dispatch through the compliance-gated outbound service.
		ok = sms_compliance.send_sms(
			to=phone,
			body=body,
			db=s,
			subscriber_id=subscriber_id,
			task_type=campaign,
			campaign=campaign,
			variant_id=variant_id,
			decision_id=decision_id,
		)
		if not ok:
			return {
				"sent": False,
				"reason": "opted_out_or_twilio_error",
				"subscriber_id": subscriber_id,
				"campaign": campaign,
				"variant_id": variant_id,
				"message_outcome_id": None,
			}

		# 4. Record the MessageOutcome row so learning cards can attribute later.
		outcome = MessageOutcome(
			subscriber_id=subscriber_id,
			message_type="sms",
			template_id=campaign,
			variant_id=variant_id,
			channel="twilio",
			sent_at=datetime.now(timezone.utc),
		)
		s.add(outcome)
		s.flush()

		return {
			"sent": True,
			"reason": "ok",
			"subscriber_id": subscriber_id,
			"campaign": campaign,
			"variant_id": variant_id,
			"message_outcome_id": outcome.id,
		}


# ──────────────────────────────────────────────────────────────────────────────
# log_decision
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="write", idempotent=True)
def log_decision(
	decision_id: str,
	graph_name: str,
	subscriber_id: Optional[int] = None,
	event_type: Optional[str] = None,
	terminal_status: Optional[str] = None,
	tokens_used: int = 0,
	cost_usd: float = 0.0,
	summary: Optional[Dict[str, Any]] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Upsert one row into agent_decisions for a graph run.

	Called twice per decision:
	  - once at graph start (terminal_status=None) to establish the row
	  - once at graph end (terminal_status set) to finalize with totals

	Idempotent by decision_id (the primary key). A second call with the
	same decision_id updates the existing row rather than inserting a new
	one, so graphs can safely re-log on resume after a crash.

	Returns the final persisted state of the row.
	"""
	valid_statuses = {"completed", "aborted", "escalated", "failed", None}
	if terminal_status not in valid_statuses:
		raise ValueError(
			f"terminal_status must be one of {valid_statuses}, got {terminal_status!r}"
		)

	with _session(session) as s:
		row = (
			s.query(AgentDecision)
			.filter(AgentDecision.decision_id == decision_id)
			.first()
		)

		if row is None:
			row = AgentDecision(
				decision_id=decision_id,
				graph_name=graph_name,
				subscriber_id=subscriber_id,
				event_type=event_type,
				started_at=datetime.now(timezone.utc),
				terminal_status=terminal_status,
				tokens_used=tokens_used,
				cost_usd=cost_usd,
				summary=summary,
			)
			s.add(row)
		else:
			# Update — but never regress non-null identifying fields.
			if subscriber_id is not None:
				row.subscriber_id = subscriber_id
			if event_type is not None:
				row.event_type = event_type
			if terminal_status is not None:
				row.terminal_status = terminal_status
				row.completed_at = datetime.now(timezone.utc)
			# Always take the larger of the counters (we accept additive updates).
			row.tokens_used = max(row.tokens_used or 0, tokens_used)
			row.cost_usd = max(float(row.cost_usd or 0), float(cost_usd))
			if summary is not None:
				row.summary = summary

		s.flush()

		return {
			"decision_id": row.decision_id,
			"graph_name": row.graph_name,
			"subscriber_id": row.subscriber_id,
			"event_type": row.event_type,
			"terminal_status": row.terminal_status,
			"tokens_used": row.tokens_used,
			"cost_usd": float(row.cost_usd or 0),
			"started_at": row.started_at.isoformat() if row.started_at else None,
			"completed_at": row.completed_at.isoformat() if row.completed_at else None,
		}
