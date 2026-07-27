"""
call.booked producer — C4's input stub, same caveat as reply_stub_producer.py:
no confirmed real calendar/booking-webhook integration yet. Everything
downstream (pre_call.py, the brief, the store) is real.

Documented stub payload shape (matches src.agents.cora.contracts.CallBookedStubPayload):
    {
        "opportunity_thread_id": str,
        "call_booked_at": str (ISO-8601),
        "rep": str | None,
        "scheduled_for": str | None (ISO-8601),
        "buyer_entity": dict,   # shape of tools.read_tools.get_buyer_entity_by_opportunity_thread_id()
    }
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from src.agents.cora import queue

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("opportunity_thread_id", "call_booked_at", "buyer_entity")


def produce_stub_call_booked(payload: Dict[str, Any]) -> Optional[str]:
    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        raise ValueError(f"call_booked_stub_producer: payload missing required field(s): {missing}")

    idempotency_key = queue.make_idempotency_key(
        "call.booked", payload["opportunity_thread_id"], payload["call_booked_at"],
    )
    message_id = queue.publish("call.booked", payload, idempotency_key=idempotency_key)
    logger.info(
        "call_booked_stub_producer: published call.booked thread=%s message_id=%s",
        payload["opportunity_thread_id"], message_id,
    )
    return message_id
