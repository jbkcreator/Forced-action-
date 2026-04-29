"""
Event envelope definitions.

One shape for all event sources (Redis, Postgres LISTEN, cron, admin API).
The source-specific handlers normalize their payloads into `Event` before
invoking the supervisor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Event:
	"""
	Normalized event envelope fed to the supervisor.

	Fields:
		event_type       — key in src.agents.router.EVENT_TO_GRAPH
		subscriber_id    — target user (None allowed for broadcast-style events)
		payload          — graph-specific fields
		source           — where the event originated ('redis' | 'postgres' | 'cron' | 'admin')
		decision_id      — UUID. Wave 2 uses Wave 1's id; others generate one.
		idempotency_key  — supervisor-level dedup key (defaults to decision_id)
	"""
	event_type: str
	subscriber_id: Optional[int] = None
	payload: Dict[str, Any] = field(default_factory=dict)
	source: str = "unknown"
	decision_id: Optional[str] = None
	idempotency_key: Optional[str] = None

	def to_dispatch_dict(self) -> Dict[str, Any]:
		"""Return the dict shape that src.agents.supervisor.dispatch_event expects."""
		d: Dict[str, Any] = {
			"event_type": self.event_type,
			"subscriber_id": self.subscriber_id,
			"payload": self.payload,
		}
		if self.decision_id:
			d["decision_id"] = self.decision_id
		if self.idempotency_key:
			d["idempotency_key"] = self.idempotency_key
		return d
