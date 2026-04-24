"""
Long-running listeners for the four event sources.

Each listener pulls raw messages, normalizes via src.agents.events.handlers,
and dispatches via src.agents.supervisor.dispatch_event.

Priority-list scope:
  - Postgres LISTEN/NOTIFY — works today with the existing DB
  - Cron trigger            — works today via the scheduler entry point
  - Admin API trigger       — works today via the ingest_admin_event helper
  - Redis Pub/Sub           — scaffolded; enters dry-run mode if REDIS_URL
							  is absent. Comes live when Redis is provisioned.

Production run:
	python -m scripts.run_agents --serve
starts all listeners configured as enabled in AgentsSettings.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Callable, Dict, Optional

from config.agents import get_agents_settings
from src.agents.events.handlers import (
	from_admin,
	from_cron,
	from_postgres,
	from_redis,
)
from src.agents.supervisor import dispatch_event

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# One-shot dispatch helpers — safe to call from any entry point (API, cron, CLI)
# ──────────────────────────────────────────────────────────────────────────────

def ingest_admin_event(payload: Dict[str, Any]) -> Dict[str, Any]:
	event = from_admin(payload)
	return dispatch_event(event.to_dispatch_dict())


def ingest_cron_event(payload: Dict[str, Any]) -> Dict[str, Any]:
	event = from_cron(payload)
	return dispatch_event(event.to_dispatch_dict())


# ──────────────────────────────────────────────────────────────────────────────
# Postgres LISTEN listener — notifies via NOTIFY cora_events, '<json-body>'
# ──────────────────────────────────────────────────────────────────────────────

def listen_postgres(
	channel: str = "cora_events",
	stop_event: Optional[threading.Event] = None,
) -> None:
	"""
	Blocking listener on a Postgres NOTIFY channel.

	Senders write:
		NOTIFY cora_events, '{"event_type":"retention_summary_due", "subscriber_id": 42, "payload": {"tier":"wallet"}}';

	This handler normalizes the payload via handlers.from_postgres and
	dispatches to the supervisor.

	Run in a daemon thread — pass stop_event to request a clean shutdown.
	"""
	try:
		from psycopg import Connection
	except ImportError:
		logger.error("listen_postgres requires psycopg (installed via requirements.txt)")
		return

	settings = get_agents_settings()
	stop_event = stop_event or threading.Event()

	conn = Connection.connect(settings.database_url.replace("+psycopg2", ""), autocommit=True)
	with conn.cursor() as cur:
		cur.execute(f'LISTEN "{channel}"')
	logger.info("listen_postgres: subscribed to channel=%s", channel)

	try:
		while not stop_event.is_set():
			# psycopg3 generators block up to the timeout; pass timeout=1s
			# so we can check stop_event regularly.
			gen = conn.notifies(timeout=1.0)
			for notify in gen:
				try:
					event = from_postgres(notify.payload)
					dispatch_event(event.to_dispatch_dict())
				except Exception as exc:
					logger.exception("listen_postgres: dispatch failed: %s", exc)
				if stop_event.is_set():
					break
	finally:
		conn.close()
		logger.info("listen_postgres: closed")


# ──────────────────────────────────────────────────────────────────────────────
# Redis Pub/Sub listener — scaffolded; dry-runs without REDIS_URL
# ──────────────────────────────────────────────────────────────────────────────

def listen_redis(
	channel: str = "cora:events",
	stop_event: Optional[threading.Event] = None,
) -> None:
	settings = get_agents_settings()
	if not settings.redis_url:
		logger.info("listen_redis: REDIS_URL not set — listener disabled (dry-run)")
		return

	try:
		import redis
	except ImportError:
		logger.error("listen_redis requires redis (installed via requirements.txt)")
		return

	stop_event = stop_event or threading.Event()
	client = redis.from_url(settings.redis_url)
	pubsub = client.pubsub(ignore_subscribe_messages=True)
	pubsub.subscribe(channel)
	logger.info("listen_redis: subscribed to channel=%s", channel)

	try:
		while not stop_event.is_set():
			message = pubsub.get_message(timeout=1.0)
			if message is None:
				continue
			try:
				event = from_redis(message["data"])
				dispatch_event(event.to_dispatch_dict())
			except Exception as exc:
				logger.exception("listen_redis: dispatch failed: %s", exc)
	finally:
		pubsub.close()
		client.close()
		logger.info("listen_redis: closed")


# ──────────────────────────────────────────────────────────────────────────────
# Supervisor "server" — runs all enabled listeners
# ──────────────────────────────────────────────────────────────────────────────

def run_forever() -> None:
	"""
	Start every enabled listener in its own daemon thread and block on SIGTERM.

	Threads:
	  - Postgres LISTEN   (if AGENTS_EVENT_SOURCE_POSTGRES=true)
	  - Redis Pub/Sub     (if AGENTS_EVENT_SOURCE_REDIS=true and REDIS_URL set)

	Cron and admin-API events are pushed via ingest_cron_event /
	ingest_admin_event — no dedicated listener thread is needed.
	"""
	import signal

	settings = get_agents_settings()
	stop_event = threading.Event()
	threads = []

	if settings.agents_event_source_postgres:
		t = threading.Thread(
			target=listen_postgres,
			args=("cora_events", stop_event),
			daemon=True,
			name="cora-listen-postgres",
		)
		t.start()
		threads.append(t)

	if settings.agents_event_source_redis:
		t = threading.Thread(
			target=listen_redis,
			args=("cora:events", stop_event),
			daemon=True,
			name="cora-listen-redis",
		)
		t.start()
		threads.append(t)

	def _stop(*_: Any) -> None:
		logger.info("supervisor: received stop signal; draining")
		stop_event.set()

	signal.signal(signal.SIGINT, _stop)
	try:
		signal.signal(signal.SIGTERM, _stop)
	except (AttributeError, ValueError):
		# SIGTERM is not always available on Windows.
		pass

	logger.info("supervisor: running (%d listeners)", len(threads))
	for t in threads:
		t.join()
	logger.info("supervisor: shutdown complete")
