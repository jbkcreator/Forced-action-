"""
Source-specific handlers. Each takes a raw payload from one of the four
event sources and returns a normalized Event for the supervisor.

Raw shapes the platform currently emits:

	Redis Pub/Sub (channel 'cora:events'):
		JSON string:
		  {"event_type": "...", "subscriber_id": 107, "payload": {...},
		   "decision_id": "optional"}

	Postgres LISTEN/NOTIFY (channel 'cora_events'):
		Same JSON string as Redis — senders must match envelope shape.

	Cron:
		Dict passed directly from the scheduler (no serialization).

	Admin API:
		Dict from the request body (validated upstream).
"""

from __future__ import annotations

import json
from typing import Any, Dict, Union

from src.agents.events.types import Event


def _coerce(raw: Union[str, bytes, Dict[str, Any]], source: str) -> Event:
	"""Parse a raw payload (dict or JSON string) into an Event."""
	if isinstance(raw, (bytes, str)):
		if isinstance(raw, bytes):
			raw = raw.decode("utf-8")
		data = json.loads(raw)
	else:
		data = raw

	event_type = data.get("event_type")
	if not event_type:
		raise ValueError("Event missing 'event_type'")

	return Event(
		event_type=event_type,
		subscriber_id=data.get("subscriber_id"),
		payload=data.get("payload") or {},
		source=source,
		decision_id=data.get("decision_id"),
		idempotency_key=data.get("idempotency_key"),
	)


def from_redis(raw: Union[str, bytes]) -> Event:
	"""Normalize a Redis Pub/Sub message body."""
	return _coerce(raw, source="redis")


def from_postgres(raw: Union[str, bytes]) -> Event:
	"""Normalize a Postgres NOTIFY payload."""
	return _coerce(raw, source="postgres")


def from_cron(payload: Dict[str, Any]) -> Event:
	"""Normalize a cron-produced payload (already a dict)."""
	return _coerce(payload, source="cron")


def from_admin(payload: Dict[str, Any]) -> Event:
	"""Normalize an admin-API payload (already a dict)."""
	return _coerce(payload, source="admin")
