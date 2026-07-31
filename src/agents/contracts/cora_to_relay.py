"""
Cora->Relay handoff contract (QUALITY-v2.2 Q3) -- the core deliverable of
this task per the round-3 Q3-scope decision (analysis §5e).

R4's audit (analysis §5c) found Relay's "batch-intake contract" was a
36-line docstring (src/services/relay/queue.py:19-53) with NO schema
validation: any caller could pass payload={} or a bogus channel, and the
row would land 'pending', get Slack-approved, and only fail at DISPATCH
time (engine.py / channels_email.py silently defaulting subject to "").
This module is the fix.

Verified directly against every real caller (not assumed from the spec's
field list alone):
  - src.services.relay.__main__.py's `--seed` CLI (R1's own scaffolding
    tool for founders/devs to manually test the queue -- NOT real Cora
    traffic; always allowed --thread-id to be omitted).
  - src.services.cora_throughput.decisions.py's `_enqueue_to_relay()`
    (THROUGH-v2.2, branch feat/through-v2.2-founder-throughput, fully
    built, unmerged -- read directly off that branch to ground this
    contract against real code).

Scope boundary (round-3 decision): Q3 builds ONLY this enqueue()-level
validator. THROUGH-v2.2 needs ZERO changes when this lands -- see the
field-by-field disposition table in this task's plan document for the full
reasoning on which D1 fields are hard-required here vs. already enforced
elsewhere vs. a logged-but-not-yet-closeable gap.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from pydantic import BaseModel, field_validator, model_validator
from sqlalchemy.orm import Session

from src.agents.contracts.base import HandoffRejected, reject_and_notify
# Import-for-side-effect: ensures 'email' is registered in DISPATCHERS
# before validation runs, matching __main__.py:37's own convention.
import src.services.relay.channels_email  # noqa: F401
from src.services.relay.channels import DISPATCHERS
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

_THREAD_ID_RE = re.compile(r"^OPP-\d{4}-\d{5}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SOFT_CELL_TAG_FIELDS = ("cell_id", "offer", "avenue", "angle")


class CoraRelayHandoff(BaseModel):
    idempotency_key: str
    channel: str
    recipient: str
    thread_id: str
    subject: Optional[str] = None
    body: str

    @field_validator("idempotency_key")
    @classmethod
    def _idempotency_key_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("idempotency_key must be non-empty")
        return v

    @field_validator("channel")
    @classmethod
    def _channel_registered(cls, v: str) -> str:
        if v not in DISPATCHERS:
            raise ValueError(f"channel {v!r} has no registered dispatcher (known: {sorted(DISPATCHERS)})")
        return v

    @field_validator("thread_id")
    @classmethod
    def _thread_id_format(cls, v: str) -> str:
        if not v or not _THREAD_ID_RE.match(v):
            raise ValueError(f"thread_id {v!r} does not match OPP-YYYY-##### format")
        return v

    @field_validator("body")
    @classmethod
    def _body_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("payload['body'] must be non-empty")
        return v

    @model_validator(mode="after")
    def _recipient_matches_channel(self) -> "CoraRelayHandoff":
        if self.channel == "email":
            if not self.recipient or not _EMAIL_RE.match(self.recipient):
                raise ValueError(f"recipient {self.recipient!r} is not a valid email for channel='email'")
            if not self.subject or not self.subject.strip():
                raise ValueError("payload['subject'] must be non-empty for channel='email'")
        elif self.channel in ("sms", "voice"):
            if not self.recipient or normalize_phone(self.recipient) is None:
                raise ValueError(f"recipient {self.recipient!r} is not a valid phone number for channel={self.channel!r}")
        else:
            if not self.recipient or not self.recipient.strip():
                raise ValueError("recipient must be non-empty")
        return self


def _log_soft_gaps(payload: dict[str, Any], thread_id: str) -> None:
    """SOFT fields (cell tag, payment/booking link) are logged, never
    rejected -- see this task's field-by-field disposition table for why."""
    missing_soft = [f for f in _SOFT_CELL_TAG_FIELDS if not payload.get(f)]
    if missing_soft:
        logger.warning(
            "cora_to_relay: thread_id=%s missing soft/coverage fields %s "
            "(cell_tag_missing) -- not rejected, logged for visibility only",
            thread_id, missing_soft,
        )


def validate_handoff(
    *,
    idempotency_key: str,
    channel: str,
    recipient: str,
    payload: dict[str, Any],
    thread_id: Optional[str],
) -> CoraRelayHandoff:
    """Raises pydantic.ValidationError on any hard-required field failure.
    Called from relay.queue.enqueue(); callers there catch it and route
    through reject_handoff() below."""
    handoff = CoraRelayHandoff(
        idempotency_key=idempotency_key,
        channel=channel,
        recipient=recipient,
        thread_id=thread_id or "",
        subject=(payload or {}).get("subject"),
        body=(payload or {}).get("body", ""),
    )
    _log_soft_gaps(payload or {}, handoff.thread_id)
    return handoff


def reject_handoff(
    session: Session, *, idempotency_key: str, errors: list[str], payload_snapshot: dict[str, Any],
) -> HandoffRejected:
    return reject_and_notify(
        session, boundary="cora_to_relay", missing_fields=errors,
        reference_id=idempotency_key, payload_snapshot=payload_snapshot,
    )
