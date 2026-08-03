"""
call.completed producer — THROUGH-v2.2 T3's post-call auto-drafter input.

Same treatment as reply_stub_producer.py: this is the one place a real
vendor integration needs to change. There is no confirmed answer yet on
whether a Cora-booked call is even handled by Synthflow (AI voice) or by a
human closer via Aircall (CloserCall) off the static Calendly booking link
— both would need their own dedicated webhook, since the existing
src.api.main.synthflow_call_completed handler serves the OLD Lifecycle IVR
population (phone-matched only, no path back to opportunity_thread_id,
confirmed by reading it directly — same population _publish_call_booked's
own docstring already warned about).

Everything downstream of this function is real: the event lands on the
real Redis Stream, the real worker picks it up, the real post_call_recap
subgraph composes and persists a normal OutboundDraft, THROUGH's batch
pipeline picks it up like any other draft.

Documented stub payload shape (matches src.agents.cora.contracts.CallCompletedStubPayload):
    {
        "opportunity_thread_id": str,
        "transcript_text": str | None,
        "call_outcome": str | None,
        "duration_seconds": int | None,
        "completed_at": str (ISO-8601),
    }
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from src.agents.cora import queue

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("opportunity_thread_id", "completed_at")


def produce_post_call_event(payload: Dict[str, Any], idempotency_key: Optional[str] = None) -> Optional[str]:
    """
    idempotency_key: pass an explicit, stable key when the caller has one
    (e.g. a vendor's own call_id) — that key must never change across
    reprocessing of the SAME call, or the worker's own dedup can't recognize
    a duplicate. The default derived from opportunity_thread_id:completed_at
    is only stable for synthetic/manual payloads where completed_at is fixed
    at call time, not re-stamped on every processing attempt.
    """
    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        raise ValueError(f"post_call_producer: payload missing required field(s): {missing}")

    if idempotency_key is None:
        idempotency_key = queue.make_idempotency_key(
            "call.completed", payload["opportunity_thread_id"], payload["completed_at"],
        )
    message_id = queue.publish("call.completed", payload, idempotency_key=idempotency_key)
    logger.info(
        "post_call_producer: published call.completed thread=%s message_id=%s",
        payload["opportunity_thread_id"], message_id,
    )
    return message_id
