"""
reply.received producer — C3's ONE genuinely-stubbed input path.

There is no confirmed monitored-mailbox address/format yet (Josh hasn't
signed off on one — per the original build instructions, this stays a
documented stub rather than a guess at a real inbound-email integration).
Everything downstream of this function is real: the event lands on the
real Redis Stream, the real worker picks it up, the real reply.py subgraph
classifies and drafts a response, the real store persists it.

Documented stub payload shape (matches src.agents.cora.contracts.ReplyStubPayload):
    {
        "opportunity_thread_id": str | None,   # None => unmatched, routed to manual_review
        "from_address": str,
        "subject": str,
        "body_text": str,
        "received_at": str (ISO-8601),
        "raw_headers": dict,                    # opaque, not parsed by Cora
        "avenue": str | None,                   # optional hint for objection-library lookup
    }

Once a real mailbox integration exists, only this module's producer
function needs to change — reply.py and everything else stays as-is.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from src.agents.cora import queue

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("from_address", "subject", "body_text", "received_at")


def produce_stub_reply(payload: Dict[str, Any]) -> Optional[str]:
    missing = [f for f in REQUIRED_FIELDS if not payload.get(f)]
    if missing:
        raise ValueError(f"reply_stub_producer: payload missing required field(s): {missing}")

    idempotency_key = queue.make_idempotency_key(
        "reply.received",
        payload.get("opportunity_thread_id"),
        f"{payload['from_address']}:{payload['received_at']}",
    )
    message_id = queue.publish("reply.received", payload, idempotency_key=idempotency_key)
    logger.info(
        "reply_stub_producer: published reply.received from=%s thread=%s message_id=%s",
        payload["from_address"], payload.get("opportunity_thread_id"), message_id,
    )
    return message_id
