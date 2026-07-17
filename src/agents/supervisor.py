"""
Cora Supervisor — entry point for every autonomous decision.

Takes a normalized Event, routes it to the right graph via src/agents/router.py,
enforces global + per-graph kill switches, enforces idempotency by
decision_id, and ensures every routed run produces an agent_decisions row
even if the downstream graph short-circuits.

The supervisor deliberately does NOT call Claude on its own for known event
types — routing is a dict lookup. An unknown event short-circuits to an
escalated audit row, not a Haiku classification call, to keep cost
deterministic in the hot path.

Public entry points:
	dispatch_event(event)            — synchronous single-event handler
	dispatch_events(events)          — iterate + dispatch a batch

Events are dicts matching src.agents.events.types.Event (see that module).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from config.agents import get_agents_settings
from src.agents.router import EVENT_TO_GRAPH, get_graph_spec
from src.agents.tools.write_tools import log_decision
from src.core.database import db
from src.core.models import AgentDecision

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def dispatch_event(event: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Route a single event to its graph.

	event shape (required keys):
		event_type: str         — must be a key in EVENT_TO_GRAPH
		payload:    dict        — graph-specific fields
		subscriber_id: int      — target for the decision (may be None for cron-style events)

	Optional:
		decision_id:  str       — if caller already has one (e.g. Wave 2 reuses Wave 1's)
		idempotency_key: str    — supervisor-level dedup key (defaults to decision_id)

	Returns:
		{
			'handled':       bool,
			'outcome':       'routed' | 'dropped_kill_switch' | 'dropped_unknown_event'
							| 'dropped_duplicate',
			'graph_name':    str | None,
			'decision_id':   str,
			'final_state':   dict | None,    # the routed graph's final state
			'reason':        str,
		}
	"""
	settings = get_agents_settings()
	event_type = event.get("event_type")
	subscriber_id = event.get("subscriber_id")
	payload = event.get("payload") or {}
	decision_id = event.get("decision_id") or str(uuid.uuid4())
	idempotency_key = event.get("idempotency_key") or decision_id

	# Pipeline event routing — enrichment cascade (ADR 0016).
	# gold_lead_scored has no subscriber_id and no compose_and_send path;
	# hand it to the EnrichmentBatcher and return before EVENT_TO_GRAPH lookup.
	if event_type == "gold_lead_scored":
		try:
			from src.agents.enrichment_consumer import get_batcher
			batcher = get_batcher()
			if batcher is not None:
				batcher.add(payload)
			else:
				logger.warning(
					"supervisor: EnrichmentBatcher not initialized — gold_lead_scored dropped "
					"(property_id=%s); nightly batch is backstop",
					payload.get("property_id"),
				)
		except Exception as _batcher_exc:
			logger.warning("supervisor: enrichment batcher add failed: %s", _batcher_exc)
		return _outcome("routed", "enrichment_cascade", decision_id, "ok")

	# Pipeline event routing — Lead Pack Hot-Enrichment (ADR 0018).
	# lead_pack_reserved is a fulfillment trigger, not a Cora messaging graph:
	# hand it to the fulfillment worker (which fans out onto a daemon thread so
	# the listener never blocks on the Tracerfy poll) and return. The cron sweep
	# remains the durability backstop if this event is dropped.
	if event_type == "lead_pack_reserved":
		try:
			from src.tasks.lead_pack_fulfillment_sweep import handle_reserved_event
			handle_reserved_event(payload)
		except Exception as _lp_exc:
			logger.warning(
				"supervisor: lead_pack_reserved handoff failed (purchase=%s); "
				"cron sweep is backstop: %s",
				payload.get("purchase_id"), _lp_exc,
			)
		return _outcome("routed", "lead_pack_fulfillment", decision_id, "ok")

	# Closer Cockpit tagging (ADR closer-telemetry-separate-from-agent-decisions).
	# call_transcribed is post-call enrichment, NOT a Cora Touch — hand it to the
	# tagging service and return BEFORE the EVENT_TO_GRAPH lookup / agent_decisions
	# logging. The nightly retag sweep is the durability backstop.
	if event_type == "call_transcribed":
		try:
			from src.services.closer_call_tagging import tag_closer_call
			tag_closer_call((payload or {}).get("aircall_call_id"))
		except Exception as _tag_exc:
			logger.warning(
				"supervisor: closer_call tagging failed (call=%s); retag sweep is backstop: %s",
				(payload or {}).get("aircall_call_id"), _tag_exc,
			)
		return _outcome("routed", "closer_call_tagging", decision_id, "ok")

	# Feedback Ritual capture hook (Sprint 4.3).
	# feedback_ritual_candidate is queueing/enrichment, not a Cora graph run:
	# load the finished agent_decisions row and enqueue a shared feedback-ritual
	# queue row if it qualifies.
	if event_type == "feedback_ritual_candidate":
		try:
			from src.services.feedback_ritual import process_feedback_ritual_candidate
			# db is the Database singleton — it has no .session; open a transactional
			# scope so the enqueued queue row actually commits (the processor doesn't).
			with db.session_scope() as _s:
				process_feedback_ritual_candidate(
					_s,
					(payload or {}).get("decision_id"),
					actor="system",
				)
		except Exception as _feedback_exc:
			logger.warning(
				"supervisor: feedback ritual enqueue failed (decision=%s): %s",
				(payload or {}).get("decision_id"), _feedback_exc,
			)
		return _outcome("routed", "feedback_ritual", decision_id, "ok")

	# Unlock Placement + Scarcity (spec 3.3, D7): a paid hot-lead/lead unlock
	# stamps last-touch nudge attribution, not a Cora messaging graph run.
	if event_type == "unlock_purchased":
		try:
			from src.services.nudge_conversion import record_nudge_conversion
			with db.session_scope() as _s:
				record_nudge_conversion(
					subscriber_id,
					conversion_type="unlock",
					revenue=(payload or {}).get("revenue"),
					db=_s,
				)
		except Exception as _nudge_exc:
			logger.warning(
				"supervisor: unlock nudge conversion failed (sub=%s): %s",
				subscriber_id, _nudge_exc,
			)
		return _outcome("routed", "unlock_outcome_recorder", decision_id, "ok")

	# Global kill switch
	if settings.agents_global_kill_switch:
		reason = "global_kill_switch_enabled"
		logger.info("supervisor drop: %s (event=%s)", reason, event_type)
		_record_dropped(decision_id, "supervisor", subscriber_id, event_type, reason)
		_notify_drop(event, decision_id, reason)
		return _outcome("dropped_kill_switch", None, decision_id, reason)

	# Unknown event type
	spec = get_graph_spec(event_type)
	if spec is None:
		reason = f"unknown_event_type:{event_type}"
		logger.warning("supervisor drop: %s", reason)
		_record_dropped(decision_id, "supervisor", subscriber_id, event_type, reason)
		_notify_drop(event, decision_id, reason)
		return _outcome("dropped_unknown_event", None, decision_id, reason)

	# Per-graph kill switch
	if not settings.graph_is_enabled(spec.graph_name):
		reason = f"graph_disabled:{spec.graph_name}"
		logger.info("supervisor drop: %s (event=%s)", reason, event_type)
		_record_dropped(decision_id, spec.graph_name, subscriber_id, event_type, reason)
		_notify_drop(event, decision_id, reason)
		return _outcome("dropped_kill_switch", spec.graph_name, decision_id, reason)

	# Idempotency — if a completed decision already exists for this key, skip.
	if _already_handled(idempotency_key):
		reason = "duplicate_idempotency_key"
		logger.info("supervisor drop: %s key=%s", reason, idempotency_key)
		_notify_drop(event, decision_id, reason)
		return _outcome("dropped_duplicate", spec.graph_name, decision_id, reason)

	# Wave 2 needs a decision_id from Wave 1; reject if missing.
	if spec.requires_decision_id and "decision_id" not in event:
		reason = "wave2_missing_decision_id"
		logger.warning("supervisor drop: %s (event=%s)", reason, event_type)
		_record_dropped(decision_id, spec.graph_name, subscriber_id, event_type, reason)
		return _outcome("dropped_unknown_event", spec.graph_name, decision_id, reason)

	# Route.
	logger.info(
		"supervisor route: graph=%s event=%s subscriber=%s decision=%s",
		spec.graph_name, event_type, subscriber_id, decision_id,
	)

	try:
		final_state = spec.runner(
			event_payload=payload,
			subscriber_id=subscriber_id,
			decision_id=decision_id,
		)
	except Exception as exc:
		logger.exception("supervisor: graph %s raised %s", spec.graph_name, exc)
		log_decision(
			decision_id=decision_id,
			graph_name=spec.graph_name,
			subscriber_id=subscriber_id,
			event_type=event_type,
			terminal_status="failed",
			summary={"exception": f"{type(exc).__name__}: {exc}"},
		)
		return _outcome("routed", spec.graph_name, decision_id, f"exception:{type(exc).__name__}",
						final_state=None)

	return _outcome("routed", spec.graph_name, decision_id, "ok", final_state=final_state)


def dispatch_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
	"""Sequentially dispatch a batch of events and collect outcomes."""
	return [dispatch_event(e) for e in events]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _outcome(
	outcome: str,
	graph_name: Optional[str],
	decision_id: str,
	reason: str,
	final_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
	return {
		"handled": outcome == "routed",
		"outcome": outcome,
		"graph_name": graph_name,
		"decision_id": decision_id,
		"reason": reason,
		"final_state": final_state,
	}


def _record_dropped(
	decision_id: str,
	graph_name: str,
	subscriber_id: Optional[int],
	event_type: Optional[str],
	reason: str,
) -> None:
	"""Write an aborted-row for events the supervisor drops before routing."""
	try:
		log_decision(
			decision_id=decision_id,
			graph_name=graph_name,
			subscriber_id=subscriber_id,
			event_type=event_type,
			terminal_status="aborted",
			summary={"drop_reason": reason},
		)
	except Exception as exc:   # never let logging break dispatch
		logger.warning("supervisor: failed to log drop: %s", exc)


def _notify_drop(event: Dict[str, Any], decision_id: str, reason: str) -> None:
	"""
	If the event declared a result_channel, publish a drop notification so
	the caller (e.g. quora_miner) can unblock immediately instead of waiting
	out its full subscribe timeout.
	"""
	import json as _json
	result_channel = event.get("result_channel")
	if not result_channel:
		return
	try:
		from src.core.redis_client import get_redis, redis_available
		if redis_available():
			get_redis().publish(result_channel, _json.dumps({
				"decision_id":     decision_id,
				"terminal_status": reason,
				"write_confirmed": False,
			}))
	except Exception as exc:
		logger.warning("supervisor: drop notification failed (channel=%s): %s", result_channel, exc)


def _already_handled(idempotency_key: str) -> bool:
	"""Return True if an agent_decisions row with this decision_id already completed."""
	try:
		with db.session_scope() as s:
			row = (
				s.query(AgentDecision)
				.filter(AgentDecision.decision_id == idempotency_key)
				.filter(AgentDecision.terminal_status.in_(("completed", "failed", "aborted")))
				.first()
			)
			return row is not None
	except Exception as exc:
		logger.warning("supervisor: idempotency check errored, allowing: %s", exc)
		return False
