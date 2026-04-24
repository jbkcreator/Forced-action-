"""
Scenario driver helpers.

Thin wrappers around dispatch_event / DB queries / outbox reads so scenario
tests read like narratives.

Typical usage:

	def test_whatever(seed_subscriber):
		sub = seed_subscriber(name="Mike", vertical="roofing")
		freeze_at("2026-05-01T10:00:00Z")

		dispatch({"event_type": "...", "subscriber_id": sub.id, "payload": {...}})

		outbox = read_outbox(sub.id)
		assert len(outbox) == 1
		assert_agent_decision(sub.id, graph="fomo", terminal_status="completed")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import desc

from src.core import clock
from src.core.database import db
from src.core.models import AgentDecision, SandboxOutbox, SmsDeadLetter


# Re-export clock controls so scenario tests only import from one place.
freeze_at = clock.freeze_at
advance_by = clock.advance_by
reset_clock = clock.reset


# ──────────────────────────────────────────────────────────────────────────────
# Dispatch shorthand
# ──────────────────────────────────────────────────────────────────────────────

def dispatch(event: Dict[str, Any]) -> Dict[str, Any]:
	"""Route an event through the Cora supervisor. Returns the supervisor outcome dict."""
	from src.agents.supervisor import dispatch_event
	return dispatch_event(event)


# ──────────────────────────────────────────────────────────────────────────────
# Outbox readers
# ──────────────────────────────────────────────────────────────────────────────

def read_outbox(
	subscriber_id: Optional[int] = None,
	campaign: Optional[str] = None,
	channel: Optional[str] = None,
	limit: int = 50,
) -> List[SandboxOutbox]:
	"""Return sandbox_outbox rows, newest first, filtered as requested."""
	with db.session_scope() as s:
		q = s.query(SandboxOutbox)
		if subscriber_id is not None:
			q = q.filter(SandboxOutbox.subscriber_id == subscriber_id)
		if campaign is not None:
			q = q.filter(SandboxOutbox.campaign == campaign)
		if channel is not None:
			q = q.filter(SandboxOutbox.channel == channel)
		rows = q.order_by(desc(SandboxOutbox.created_at)).limit(limit).all()
		s.expunge_all()
		return rows


def last_outbox_body(subscriber_id: int) -> Optional[str]:
	"""Most recent captured SMS body for a subscriber, or None."""
	rows = read_outbox(subscriber_id=subscriber_id, limit=1)
	return rows[0].body if rows else None


def clear_outbox(subscriber_id: Optional[int] = None) -> None:
	"""Remove captured rows for a subscriber (or all if None). Test hygiene helper."""
	with db.session_scope() as s:
		q = s.query(SandboxOutbox)
		if subscriber_id is not None:
			q = q.filter(SandboxOutbox.subscriber_id == subscriber_id)
		q.delete(synchronize_session=False)


# ──────────────────────────────────────────────────────────────────────────────
# Agent decision assertions
# ──────────────────────────────────────────────────────────────────────────────

def read_agent_decisions(
	subscriber_id: Optional[int] = None,
	graph: Optional[str] = None,
	limit: int = 50,
) -> List[AgentDecision]:
	"""Return agent_decisions rows, newest first."""
	with db.session_scope() as s:
		q = s.query(AgentDecision)
		if subscriber_id is not None:
			q = q.filter(AgentDecision.subscriber_id == subscriber_id)
		if graph is not None:
			q = q.filter(AgentDecision.graph_name == graph)
		rows = q.order_by(desc(AgentDecision.started_at)).limit(limit).all()
		s.expunge_all()
		return rows


def assert_agent_decision(
	subscriber_id: int,
	*,
	graph: Optional[str] = None,
	terminal_status: Optional[str] = None,
) -> AgentDecision:
	"""
	Assert at least one agent_decisions row matches the filters. Returns
	the most recent matching row for further inspection.
	"""
	rows = read_agent_decisions(subscriber_id=subscriber_id, graph=graph)
	if terminal_status is not None:
		rows = [r for r in rows if r.terminal_status == terminal_status]
	assert rows, (
		f"No agent_decisions row for subscriber_id={subscriber_id} "
		f"graph={graph!r} status={terminal_status!r}"
	)
	return rows[0]


def assert_no_agent_decision(subscriber_id: int, graph: str) -> None:
	"""Assert that no decision of this graph type exists for this subscriber."""
	rows = read_agent_decisions(subscriber_id=subscriber_id, graph=graph)
	assert not rows, (
		f"Expected no {graph} decisions for subscriber {subscriber_id}, "
		f"found {len(rows)}"
	)


# ──────────────────────────────────────────────────────────────────────────────
# Dead-letter inspection
# ──────────────────────────────────────────────────────────────────────────────

def read_dlq(phone: Optional[str] = None, limit: int = 20) -> List[SmsDeadLetter]:
	with db.session_scope() as s:
		q = s.query(SmsDeadLetter)
		if phone is not None:
			q = q.filter(SmsDeadLetter.phone == phone)
		rows = q.order_by(desc(SmsDeadLetter.created_at)).limit(limit).all()
		s.expunge_all()
		return rows
