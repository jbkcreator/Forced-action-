"""
Command Center worker — separate from Cora's existing worker.py.

Own stream key  : cc:events
Own group       : cc_workers
Concurrent lock : Redis cc_lock:{session_id} SET NX TTL=120s

A single session_id may only have one in-flight request at a time.  If a
second message arrives for the same session while one is processing, the
worker leaves it unacked rather than dropping it — it will be picked up
once the lock expires or is released.

Shutdown: SIGINT/SIGTERM → stop flag → drain in-flight → exit.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

CC_STREAM_KEY = "cc:events"
CC_GROUP_NAME = "cc_workers"
CC_DLQ_KEY = "cc:dlq"
CC_LOCK_PREFIX = "cc_lock:"
CC_LOCK_TTL = 120
MAX_DELIVERIES = 3
CLAIM_MIN_IDLE_MS = 150_000   # must exceed CC_LOCK_TTL (120s) to avoid reclaiming still-locked messages
CLAIM_SWEEP_EVERY_N = 12


def ensure_group() -> None:
    if not redis_available():
        return
    try:
        get_redis().xgroup_create(CC_STREAM_KEY, CC_GROUP_NAME, id="0", mkstream=True)
    except Exception as exc:  # noqa: BLE001
        if "BUSYGROUP" not in str(exc):
            raise


def publish_query(
    session_id: str,
    question: str,
    slack_user_id: Optional[str] = None,
    slack_channel: Optional[str] = None,
    slack_thread_ts: Optional[str] = None,
) -> Optional[str]:
    """Publish a query.received event to the command center stream."""
    if not redis_available():
        logger.warning("cc.worker: Redis unavailable — query dropped")
        return None
    ensure_group()
    payload = {
        "session_id": session_id,
        "question": question,
        "slack_user_id": slack_user_id,
        "slack_channel": slack_channel,
        "slack_thread_ts": slack_thread_ts,
    }
    fields = {
        "event_type": "query.received",
        "idempotency_key": f"query:{session_id}:{uuid.uuid4().hex}",
        "payload": json.dumps(payload, default=str),
    }
    return get_redis().xadd(CC_STREAM_KEY, fields, maxlen=5_000, approximate=True)


def _acquire_session_lock(session_id: str, owner: str) -> bool:
    if not redis_available():
        return True
    key = f"{CC_LOCK_PREFIX}{session_id}"
    return bool(get_redis().set(key, owner, nx=True, ex=CC_LOCK_TTL))


def _release_session_lock(session_id: str, owner: str) -> None:
    if not redis_available():
        return
    key = f"{CC_LOCK_PREFIX}{session_id}"
    r = get_redis()
    if r.get(key) == owner:
        r.delete(key)


def _consumer_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _read_batch(consumer_name: str, count: int = 1, block_ms: int = 1000) -> List[Dict[str, Any]]:
    if not redis_available():
        return []
    result = get_redis().xreadgroup(
        CC_GROUP_NAME, consumer_name, {CC_STREAM_KEY: ">"}, count=count, block=block_ms
    )
    if not result:
        return []
    messages = []
    for _stream, entries in result:
        for message_id, fields in entries:
            try:
                payload = json.loads(fields.get("payload", "{}"))
            except Exception:
                payload = {}
            messages.append({
                "message_id": message_id,
                "event_type": fields.get("event_type", ""),
                "idempotency_key": fields.get("idempotency_key", ""),
                "payload": payload,
            })
    return messages


def _ack(message_id: str) -> None:
    if not redis_available():
        return
    get_redis().xack(CC_STREAM_KEY, CC_GROUP_NAME, message_id)


def _dead_letter(message_id: str, fields: Dict[str, Any], reason: str) -> None:
    if not redis_available():
        return
    r = get_redis()
    r.xadd(CC_DLQ_KEY, {**fields, "original_message_id": message_id, "dlq_reason": reason})
    r.xack(CC_STREAM_KEY, CC_GROUP_NAME, message_id)
    logger.warning("cc.worker: dead-lettered message_id=%s reason=%s", message_id, reason)


def _claim_stale(consumer_name: str) -> List[Dict[str, Any]]:
    if not redis_available():
        return []
    r = get_redis()
    pending = r.xpending_range(CC_STREAM_KEY, CC_GROUP_NAME, min="-", max="+", count=50)
    to_claim = []
    for entry in pending:
        if entry.get("time_since_delivered", 0) < CLAIM_MIN_IDLE_MS:
            continue
        if entry.get("times_delivered", 1) >= MAX_DELIVERIES:
            # Fetch the message payload before dead-lettering so the DLQ entry is useful.
            try:
                fetched = get_redis().xrange(CC_STREAM_KEY, entry["message_id"], entry["message_id"], count=1)
                dl_fields = dict(fetched[0][1]) if fetched else {}
            except Exception:
                dl_fields = {}
            _dead_letter(entry["message_id"], dl_fields, "max_deliveries_exceeded")
            continue
        to_claim.append(entry["message_id"])

    if not to_claim:
        return []
    claimed = r.xclaim(
        CC_STREAM_KEY, CC_GROUP_NAME, consumer_name,
        min_idle_time=CLAIM_MIN_IDLE_MS, message_ids=to_claim,
    )
    messages = []
    for message_id, fields in claimed:
        if fields is None:
            continue
        try:
            payload = json.loads(fields.get("payload", "{}"))
        except Exception:
            payload = {}
        messages.append({
            "message_id": message_id,
            "event_type": fields.get("event_type", ""),
            "idempotency_key": fields.get("idempotency_key", ""),
            "payload": payload,
        })
    return messages


def _instant_greeting_reply(question: str) -> Optional[str]:
    """
    If the question matches a Phase 1 greeting or identity pattern, return
    the reply immediately — so the worker skips the placeholder + graph entirely.
    """
    import hashlib
    import random
    import re
    from src.agents.cora.command_center.guard import (
        _GREETING_PATTERNS,
        _GREETING_REPLIES,
        _IDENTITY_PATTERNS,
        _IDENTITY_REPLY,
    )
    clean = re.sub(r'^<@[A-Z0-9]+>\s*', '', question.strip())
    if _GREETING_PATTERNS.match(clean):
        rng = random.Random(hashlib.md5(clean.encode()).hexdigest())
        return rng.choice(_GREETING_REPLIES)
    if _IDENTITY_PATTERNS.match(clean):
        return _IDENTITY_REPLY
    return None


class CommandCenterWorker:
    def __init__(self, consumer_name: Optional[str] = None) -> None:
        self.consumer_name = consumer_name or _consumer_name()
        self._stop = False
        self._loop_count = 0

    def request_stop(self, *_args: Any) -> None:
        logger.info("cc.worker: shutdown requested (consumer=%s)", self.consumer_name)
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def _process_one(self, message: Dict[str, Any]) -> None:
        payload = message["payload"]
        session_id = payload.get("session_id", "")
        message_id = message["message_id"]
        started = time.monotonic()

        # Queue delay: time from Redis xadd to now (message_id = <ms_epoch>-<seq>).
        try:
            stream_ms = int(str(message_id).split("-")[0])
            queue_delay_ms = int(time.time() * 1000) - stream_ms
            logger.info("PROFILE node=%-8s session=%s duration_ms=%d", "queued", session_id, queue_delay_ms)
        except Exception:
            pass

        if not session_id:
            logger.warning("cc.worker: message_id=%s has no session_id — dead-lettering", message_id)
            _dead_letter(message_id, message.get("payload", {}), "missing_session_id")
            return

        if not _acquire_session_lock(session_id, owner=self.consumer_name):
            logger.info(
                "cc.worker: session=%s already locked — leaving message=%s unacked for retry",
                session_id, message_id,
            )
            return

        slack_channel = payload.get("slack_channel")

        # Fast-path: if Phase 1 already knows this is a greeting, post the reply
        # directly (one Slack call) and skip the entire graph + placeholder round-trip.
        greeting_reply = _instant_greeting_reply(payload.get("question", ""))
        if greeting_reply and slack_channel:
            try:
                from config.settings import get_settings
                from slack_sdk import WebClient
                _s = get_settings()
                if _s.slack_bot_token:
                    WebClient(token=_s.slack_bot_token.get_secret_value()).chat_postMessage(
                        channel=slack_channel, text=greeting_reply,
                    )
            except Exception as exc:
                logger.warning("cc.worker: greeting fast-path post failed: %s", exc)
            _release_session_lock(session_id, self.consumer_name)
            _ack(message_id)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "PROFILE summary session=%s outcome=completed total_ms=%d cost_usd=0.000000 | greeting=fast-path",
                session_id, elapsed_ms,
            )
            return

        # Post "thinking..." placeholder immediately so Josh sees a response
        # within ~200ms. The emit node will edit this message with the real answer.
        placeholder_ts: Optional[str] = None
        if slack_channel:
            try:
                from config.settings import get_settings
                from slack_sdk import WebClient
                _s = get_settings()
                if _s.slack_bot_token:
                    _client = WebClient(token=_s.slack_bot_token.get_secret_value())
                    _resp = _client.chat_postMessage(
                        channel=slack_channel,
                        text="⏳ _Looking that up..._",
                    )
                    placeholder_ts = _resp["ts"]
                    logger.info("cc.worker: posted placeholder ts=%s channel=%s", placeholder_ts, slack_channel)
            except Exception as exc:
                logger.warning("cc.worker: placeholder post failed: %s — will post fresh on emit", exc)
        payload["placeholder_ts"] = placeholder_ts

        # Heartbeat thread: refresh the session lock TTL every CC_LOCK_TTL//2 seconds
        # so a long-running graph cannot lose the lock mid-execution.
        _hb_stop = threading.Event()

        def _heartbeat() -> None:
            key = f"{CC_LOCK_PREFIX}{session_id}"
            while not _hb_stop.wait(CC_LOCK_TTL // 2):
                try:
                    if redis_available():
                        get_redis().expire(key, CC_LOCK_TTL)
                except Exception:
                    pass

        _hb_thread = threading.Thread(target=_heartbeat, daemon=True, name=f"cc-lock-hb-{session_id[:8]}")
        _hb_thread.start()

        try:
            from src.agents.cora.command_center.graph import run_command_center
            from src.core.database import get_db_context

            with get_db_context() as db:
                result = run_command_center(payload, db=db)
                db.commit()

            outcome = result.get("terminal_status", "failed")
            total_ms = int((time.monotonic() - started) * 1000)
            timings = result.get("_timings") or {}
            total_cost = float(result.get("_cost_usd") or 0)
            logger.info(
                "PROFILE summary session=%s outcome=%s total_ms=%d cost_usd=%.6f ($%.4f) | %s",
                session_id, outcome, total_ms, total_cost, total_cost,
                " ".join(f"{k}={v}ms" for k, v in timings.items()),
            )
            logger.info(
                "cc.worker: processed session=%s outcome=%s reject=%s message_id=%s elapsed=%.2fs",
                session_id, outcome, result.get("reject_reason"), message_id,
                total_ms / 1000,
            )
        except Exception:
            logger.exception(
                "cc.worker: run_command_center raised for session=%s message_id=%s",
                session_id, message_id,
            )
            _hb_stop.set()
            _release_session_lock(session_id, self.consumer_name)
            return
        finally:
            _hb_stop.set()

        _release_session_lock(session_id, self.consumer_name)
        _ack(message_id)

    def run_forever(self, block_ms: int = 1000) -> None:
        ensure_group()
        logger.info("cc.worker: starting (consumer=%s)", self.consumer_name)
        while not self._stop:
            self._loop_count += 1
            try:
                if self._loop_count % CLAIM_SWEEP_EVERY_N == 0:
                    for msg in _claim_stale(self.consumer_name):
                        if self._stop:
                            break
                        self._process_one(msg)

                for msg in _read_batch(self.consumer_name, count=1, block_ms=block_ms):
                    if self._stop:
                        break
                    self._process_one(msg)
            except Exception:
                logger.exception("cc.worker: main loop iteration failed — continuing")

        logger.info("cc.worker: stopped (consumer=%s)", self.consumer_name)


def main() -> None:
    import threading
    logging.basicConfig(level=logging.INFO)

    stop_event = threading.Event()

    # Start Slack inbound thread — Socket Mode (real-time WebSocket) if
    # SLACK_APP_TOKEN is set, otherwise fall back to the polling listener.
    import os
    _has_app_token = bool(os.environ.get("SLACK_APP_TOKEN", "").strip())
    if not _has_app_token:
        try:
            from config.settings import get_settings as _gs
            _has_app_token = bool(getattr(_gs(), "slack_app_token", None))
        except Exception:
            pass

    if _has_app_token:
        from src.agents.cora.command_center.slack_socket import run_socket_mode as _listen
        _mode = "socket-mode (real-time)"
    else:
        from src.agents.cora.command_center.slack_listener import run_periodic as _listen
        _mode = "polling (fallback — set SLACK_APP_TOKEN for real-time)"

    listener_thread = threading.Thread(
        target=_listen,
        args=(stop_event,),
        daemon=True,
        name="cc-slack-listener",
    )
    listener_thread.start()
    logger.info("cc.worker: slack listener started mode=%s", _mode)

    worker = CommandCenterWorker()

    def _shutdown(*_args: Any) -> None:
        worker.request_stop()
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        worker.run_forever()
    finally:
        stop_event.set()
        listener_thread.join(timeout=5)


if __name__ == "__main__":
    main()
