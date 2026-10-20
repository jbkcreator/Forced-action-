"""Relay: the only code path that performs external egress, and only for approved actions.

Not an agent. No model call, no inference. Given an approved row it claims it, asks the host's
send-time check whether the send is still allowed (the recipient may have opted out since the
draft), hands the frozen payload to the executor registered for its channel, and records the
outcome. The executor sends exactly the bytes that were approved; it never regenerates them.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .halt import HaltSwitch
from .pending_actions import ActionStatus, PendingAction, PendingActionQueue

logger = logging.getLogger(__name__)

# Receives the frozen payload, performs the send, returns the provider's message id (or None).
EgressExecutor = Callable[[Mapping[str, Any]], "str | None"]


class SendDeferred(str):
    """A send-check reason meaning "not yet" (a time window, a pending approval): the action stays approved and
    a later sweep tries again, until the action's expiry, when it is blocked."""


# Returns None when the send may go ahead, or a short reason (no PII) when it must be blocked; a
# :class:`SendDeferred` reason holds the send for a later sweep instead.
SendCheck = Callable[[PendingAction], "str | None"]


class DispatchOutcome(str, Enum):
    SENT = "sent"
    FAILED = "failed"
    # No executor is registered for the channel: sending there is not switched on yet. Stored as
    # failed (never retried), but reported apart so an expected gap does not read as an error.
    NOT_ENABLED = "not_enabled"
    BLOCKED = "blocked"
    DEFERRED = "deferred"
    NOT_CLAIMED = "not_claimed"


@dataclass(frozen=True)
class DispatchResult:
    outcome: DispatchOutcome
    reason: str | None = None


@dataclass(frozen=True)
class RelayResult:
    sent: tuple[int, ...]
    failed: tuple[int, ...]
    blocked: tuple[int, ...]
    not_claimed: tuple[int, ...]
    deferred: tuple[int, ...] = ()


class Relay:
    def __init__(self, queue: PendingActionQueue, halt: HaltSwitch, executors: Mapping[str, EgressExecutor],
                 send_check: SendCheck | None = None) -> None:
        self._queue = queue
        self._halt = halt
        self._executors = dict(executors)
        self._send_check = send_check

    def _blocking_reason(self, action: PendingAction) -> str | None:
        if self._send_check is None:
            return None
        try:
            return self._send_check(action)
        except Exception as exc:
            # Fail closed: if the opt-out state cannot be read, the send cannot be proven allowed.
            logger.error("relay: send check errored on action %s (%s)", action.action_id, type(exc).__name__)
            return f"send check could not run ({type(exc).__name__})"

    def dispatch(self, action_id: int) -> DispatchResult:
        """Send one approved action. Raises :class:`AgentHalted` before claiming if halted."""
        self._halt.check()
        action = self._queue.get(action_id)
        if action is None or action.status is not ActionStatus.APPROVED:
            return DispatchResult(DispatchOutcome.NOT_CLAIMED)
        if not self._queue.claim_for_send(action_id):
            return DispatchResult(DispatchOutcome.NOT_CLAIMED)

        executor = self._executors.get(action.channel)
        if executor is None:
            reason = f"no executor registered for channel {action.channel}"
            logger.warning("relay: sending via %s is not switched on; action %s was not sent", action.channel,
                           action_id)
            self._queue.mark_failed(action_id, reason)
            return DispatchResult(DispatchOutcome.NOT_ENABLED, reason)

        # An expired draft never sends, whatever the send check says (a deferred send may clear only after it).
        if action.expires_at is not None and action.expires_at <= self._queue.now():
            reason: str | None = "the draft expired before it could be sent"
        else:
            reason = self._blocking_reason(action)
        if isinstance(reason, SendDeferred):
            self._queue.release_claim(action_id)
            logger.debug("relay: action %s held for a later sweep: %s", action_id, reason)
            return DispatchResult(DispatchOutcome.DEFERRED, reason)
        if reason is not None:
            logger.warning("relay: action %s blocked at send time: %s", action_id, reason)
            self._queue.mark_blocked(action_id, reason)
            return DispatchResult(DispatchOutcome.BLOCKED, reason)

        try:
            provider_ref = executor(action.payload)
        except Exception as exc:
            # Payload values can carry contact details; log the id and the error type only.
            logger.error("relay: executor for %s failed on action %s (%s)", action.channel, action_id,
                         type(exc).__name__)
            self._queue.mark_failed(action_id, f"{type(exc).__name__}: {exc}")
            return DispatchResult(DispatchOutcome.FAILED, type(exc).__name__)
        self._queue.mark_sent(action_id, provider_ref)
        logger.info("relay: action %s sent via %s", action_id, action.channel)
        return DispatchResult(DispatchOutcome.SENT)

    def run(self, limit: int = 50) -> RelayResult:
        """Sweep approved actions that were not dispatched at click time (restart, halt lifted)."""
        self._halt.check()
        buckets: dict[DispatchOutcome, list[int]] = {outcome: [] for outcome in DispatchOutcome}
        for action_id in self._queue.approved_ids(limit):
            buckets[self.dispatch(action_id).outcome].append(action_id)
        return RelayResult(
            sent=tuple(buckets[DispatchOutcome.SENT]),
            failed=tuple(buckets[DispatchOutcome.FAILED] + buckets[DispatchOutcome.NOT_ENABLED]),
            blocked=tuple(buckets[DispatchOutcome.BLOCKED]),
            not_claimed=tuple(buckets[DispatchOutcome.NOT_CLAIMED]),
            deferred=tuple(buckets[DispatchOutcome.DEFERRED]),
        )
