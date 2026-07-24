"""
Relay channel dispatch registry (RELAY-v2.2 sub-task R1).

A plain name -> send-function registry, not an ABC/factory — one real
channel exists today (none; R1 ships only 'noop'). R2 registers the real
Instantly-backed 'email' channel (and 'sms' via sms_compliance) with one
register() call each, without touching the execution engine.
"""
from __future__ import annotations

from typing import Callable

from src.services.relay.queue import QueueItem

# channel name -> callable(item) that performs the real send. Raises on
# failure; the engine catches and records the error. Returns None on success.
DISPATCHERS: dict[str, Callable[[QueueItem], None]] = {}


def register(channel: str, fn: Callable[[QueueItem], None]) -> None:
    DISPATCHERS[channel] = fn


def _noop(item: QueueItem) -> None:
    """R1 test/default channel — marks sent without a real send.
    R2 registers 'email' (Instantly) and 'sms' (Telnyx via sms_compliance)."""
    return None


register("noop", _noop)
