"""
Long-running listeners for the four event sources.

Each listener pulls raw messages, normalizes via src.agents.events.handlers,
and dispatches via src.agents.supervisor.dispatch_event.

Priority-list scope:
  - Postgres LISTEN/NOTIFY — works today with the existing DB
  - Cron trigger            — works today via the scheduler entry point
  - Admin API trigger       — works today via the ingest_admin_event helper
  - Redis Queue (LPUSH/BRPOP key "cora:queue") — active when REDIS_URL is set
                              and AGENTS_EVENT_SOURCE_REDIS=true.

Public event publish API (for use by services/tasks — never dispatch_event directly):
  publish_cora_event(event)  — Redis queue primary, Postgres durable fallback.

Production run:
	python -m scripts.run_agents --serve
starts all listeners configured as enabled in AgentsSettings.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from sqlalchemy import text

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
# Public publish API — call this from services/tasks instead of dispatch_event
# ──────────────────────────────────────────────────────────────────────────────

def publish_cora_event(event: Dict[str, Any]) -> None:
	"""
	Publish an event for async pickup by the agents process.

	Primary path  : Redis Pub/Sub channel "cora:events" (low-latency, <100ms).
	Fallback path : INSERT into cora_event_queue + NOTIFY cora_events (durable).

	Never calls dispatch_event() inline — the API/cron process must never own
	a graph run. The agents process is the sole consumer.
	"""
	from src.core.redis_client import get_redis, redis_available

	if redis_available():
		try:
			get_redis().lpush("cora:queue", json.dumps(event, default=str))
			return
		except Exception as exc:
			logger.warning(
				"publish_cora_event: Redis lpush failed (%s) — falling back to Postgres", exc
			)

	_publish_via_postgres(event)


def publish_after_commit(session: Any, event: Dict[str, Any]) -> None:
	"""
	Publish `event` only after `session` next commits — never inline.

	Signup flushes (not commits) the subscriber inside a request transaction
	the caller commits later. Publishing inline lets the agents process consume
	the event and, in its own transaction, fail to see the still-uncommitted
	subscriber (get_subscriber_profile returns None → aborted run). Deferring
	to after_commit guarantees the row is durable before any consumer can act.
	If the transaction rolls back, the event is never published.
	"""
	from sqlalchemy import event as sa_event

	def _fire(_session: Any) -> None:
		try:
			publish_cora_event(event)
		except Exception:
			logger.warning(
				"publish_after_commit: publish failed for event_type=%s",
				event.get("event_type"), exc_info=True,
			)

	sa_event.listen(session, "after_commit", _fire, once=True)


def _publish_via_postgres(event: Dict[str, Any]) -> None:
	"""Insert event into the durable queue table and emit a NOTIFY."""
	from src.core.database import get_db_context
	from src.core.models import CoraEventQueue

	try:
		with get_db_context() as session:
			row = CoraEventQueue(
				event_type=event.get("event_type"),
				subscriber_id=event.get("subscriber_id"),
				payload=event.get("payload") or {},
				idempotency_key=event.get("idempotency_key"),
				decision_id=event.get("decision_id"),
				status="pending",
			)
			session.add(row)
			session.flush()
			session.execute(
				text("SELECT pg_notify('cora_events', :payload)"),
				{"payload": json.dumps(event, default=str)},
			)
	except Exception as exc:
		logger.error("publish_cora_event: Postgres fallback also failed: %s", exc)


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

def _sweep_postgres_queue() -> int:
	"""
	Sweep cora_event_queue for pending events and dispatch them.
	Called on listener startup and every 60s to catch events published
	while the listener was offline. Returns the count of events processed.
	"""
	from src.core.database import get_db_context
	from src.core.models import CoraEventQueue

	processed = 0
	try:
		with get_db_context() as session:
			rows = (
				session.query(CoraEventQueue)
				.filter(CoraEventQueue.status == "pending")
				.order_by(CoraEventQueue.created_at)
				.with_for_update(skip_locked=True)
				.limit(100)
				.all()
			)
			for row in rows:
				try:
					row.status = "processing"
					session.flush()
					event = {
						"event_type": row.event_type,
						"subscriber_id": row.subscriber_id,
						"payload": row.payload or {},
						"idempotency_key": row.idempotency_key,
						"decision_id": row.decision_id,
					}
					dispatch_event(event)
					row.status = "done"
					row.processed_at = datetime.now(timezone.utc)
					processed += 1
				except Exception as exc:
					row.status = "failed"
					row.error = str(exc)[:500]
					logger.exception("_sweep_postgres_queue: dispatch failed for id=%s: %s", row.id, exc)
	except Exception as exc:
		logger.error("_sweep_postgres_queue: sweep failed: %s", exc)
	if processed:
		logger.info("_sweep_postgres_queue: dispatched %d pending event(s)", processed)
	return processed


def listen_postgres(
	channel: str = "cora_events",
	stop_event: Optional[threading.Event] = None,
) -> None:
	"""
	Blocking listener on a Postgres NOTIFY channel.

	On startup: sweeps cora_event_queue for any events published while the
	listener was offline. Every 60s: re-sweeps for stuck pending rows.

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

	# Sweep on startup — dispatch any events published while we were offline
	_sweep_postgres_queue()
	last_sweep = time.monotonic()

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

			# Periodic re-sweep every 60s to catch stuck pending rows
			if time.monotonic() - last_sweep >= 60:
				_sweep_postgres_queue()
				last_sweep = time.monotonic()
	finally:
		conn.close()
		logger.info("listen_postgres: closed")


# ──────────────────────────────────────────────────────────────────────────────
# Redis Queue listener — BRPOP on "cora:queue"; dry-runs without REDIS_URL
# ──────────────────────────────────────────────────────────────────────────────

def listen_redis(
	key: str = "cora:queue",
	stop_event: Optional[threading.Event] = None,
) -> None:
	"""
	Blocking BRPOP consumer on the Redis list key "cora:queue".

	Events pushed via LPUSH sit in the list until consumed. BRPOP is atomic —
	each call dequeues exactly one item; no two workers can receive the same
	message. Events survive Cora restarts (they wait in the list). They do NOT
	survive a Redis restart unless AOF/RDB persistence is enabled on the Redis
	instance — the Postgres cora_event_queue is the durability backstop for that.
	"""
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
	logger.info("listen_redis: polling queue key=%s", key)

	try:
		while not stop_event.is_set():
			# BRPOP blocks up to `timeout` seconds then returns None.
			# timeout=1 keeps the stop_event check responsive.
			result = client.brpop(key, timeout=1)
			if result is None:
				continue
			try:
				# result is (key_bytes, value_bytes); value is the JSON payload.
				event = from_redis(result[1])
				dispatch_event(event.to_dispatch_dict())
			except Exception as exc:
				logger.exception("listen_redis: dispatch failed: %s", exc)
	finally:
		client.close()
		logger.info("listen_redis: closed")


# ──────────────────────────────────────────────────────────────────────────────
# Supervisor "server" — runs all enabled listeners
# ──────────────────────────────────────────────────────────────────────────────

def run_forever() -> None:
	"""
	Start every enabled listener in its own daemon thread and block until a
	stop signal arrives. Exits cleanly on SIGINT (Ctrl+C) / SIGTERM, and on
	Windows also on SIGBREAK (Ctrl+Break).

	Threads:
	  - Postgres LISTEN   (if AGENTS_EVENT_SOURCE_POSTGRES=true)
	  - Redis Pub/Sub     (if AGENTS_EVENT_SOURCE_REDIS=true and REDIS_URL set)

	Cron and admin-API events are pushed via ingest_cron_event /
	ingest_admin_event — no dedicated listener thread is needed.
	"""
	import signal

	# Bridge LangSmith settings (.env → os.environ) so the tracer emits to the
	# configured project. Must run before any graph executes a Claude call.
	from src.agents.observability.langsmith import configure_tracing
	if configure_tracing():
		logger.info("ingestion: LangSmith tracing enabled (project=%s)",
		            get_agents_settings().langsmith_project)

	settings = get_agents_settings()
	stop_event = threading.Event()
	threads = []

	# Initialise the EnrichmentBatcher (ADR 0016).
	# The batcher starts its own timer thread; stop_event signals clean shutdown.
	try:
		from src.agents.enrichment_consumer import init_batcher
		init_batcher(stop_event=stop_event)
		logger.info("ingestion: EnrichmentBatcher initialised")
	except Exception as _batcher_init_exc:
		logger.error(
			"ingestion: failed to initialise EnrichmentBatcher: %s", _batcher_init_exc
		)

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
			args=("cora:queue", stop_event),
			daemon=True,
			name="cora-listen-redis",
		)
		t.start()
		threads.append(t)

	def _stop(*_: Any) -> None:
		logger.info("supervisor: received stop signal; draining")
		stop_event.set()

	# SIGINT works on all platforms. SIGTERM / SIGBREAK are best-effort.
	signal.signal(signal.SIGINT, _stop)
	for sig_name in ("SIGTERM", "SIGBREAK"):
		sig = getattr(signal, sig_name, None)
		if sig is not None:
			try:
				signal.signal(sig, _stop)
			except (ValueError, OSError):
				pass

	logger.info("supervisor: running (%d listeners)", len(threads))

	# Block the main thread on stop_event, polling so SIGINT on Windows
	# (where signal delivery to a blocked thread can be delayed) has a
	# chance to run. 0.5s is tight enough that Ctrl+C feels responsive.
	try:
		while not stop_event.is_set():
			stop_event.wait(timeout=0.5)
	except KeyboardInterrupt:
		stop_event.set()

	# Give listeners a short drain window, then let daemon threads die with
	# the process. Never block the main thread indefinitely on a listener.
	for t in threads:
		t.join(timeout=5.0)
	logger.info("supervisor: shutdown complete")
