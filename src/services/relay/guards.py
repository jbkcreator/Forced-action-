"""
Relay execution-time guards (RELAY-v2.2 sub-task R3, ceiling reworked for
the PR #179 review fix).

evaluate() is the gate every approved item passes immediately before a claim
is attempted. It answers: may this specific item be sent right now, ignoring
today's channel-wide volume? -- send window (pure arithmetic), then
suppression (one DB query). The daily ceiling is deliberately NOT part of
evaluate() anymore: a per-channel volume cap is shared state across every
concurrent worker, and evaluate()'s old approach (an in-memory dict, one
DB snapshot per batch) could not enforce that -- two overlapping sweeps each
started from the same snapshot and could jointly exceed the ceiling, since
nothing reserved a slot atomically. See reserve_daily_slot()/
release_daily_slot() below, called directly by engine.execute_batch() around
the claim+dispatch sequence, using Redis's atomic INCR as the shared counter.

`now` is injected rather than looked up inside evaluate(), so callers (and
tests) control the clock directly -- no mocking, no freezegun, no DB in the
window-check tests.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.redis_client import rdecr, rincr
from src.services.compliance_gator import validate_outbound
from src.services.email_suppression import is_email_suppressed
from src.services.relay.config import (
    GUARD_ALLOW,
    GUARD_BLOCK,
    GUARD_DEFER,
    REASON_OUTSIDE_SEND_WINDOW,
    REASON_SUPPRESSED,
)
from src.services.relay.queue import QueueItem

ALLOW = GUARD_ALLOW
DEFER = GUARD_DEFER
BLOCK = GUARD_BLOCK

_DAILY_SLOT_KEY_TTL_SECONDS = 2 * 24 * 3600  # outlives the day it counts, then expires


@dataclass(frozen=True)
class Verdict:
    outcome: str          # ALLOW | DEFER | BLOCK
    reason: str = ""       # e.g. "outside_send_window", "suppressed:email_opt_out"


def _within_send_window(now: datetime, settings) -> bool:
    local = now.astimezone(ZoneInfo(settings.relay_send_window_timezone))
    return settings.relay_send_window_start <= local.hour < settings.relay_send_window_end


def _suppression_reason(item: QueueItem) -> str | None:
    """None = clear to send. A string = blocked, and why."""
    with get_db_context() as db:
        if item.channel == "email":
            if is_email_suppressed(db, item.recipient):
                return "email_opt_out"
            return None
        if item.channel in ("sms", "voice"):
            result = validate_outbound(item.recipient, item.channel, db)
            return None if result.allowed else (result.reason or "compliance_blocked")
    return None   # 'noop' and any future non-contact channel


def evaluate(item: QueueItem, *, now: datetime) -> Verdict:
    settings = get_settings()

    if not _within_send_window(now, settings):
        return Verdict(DEFER, REASON_OUTSIDE_SEND_WINDOW)

    cause = _suppression_reason(item)
    if cause is not None:
        return Verdict(BLOCK, f"{REASON_SUPPRESSED}:{cause}")

    return Verdict(ALLOW)


def _daily_slot_key(channel: str, now: datetime, settings) -> str:
    local_date = now.astimezone(ZoneInfo(settings.relay_send_window_timezone)).date()
    return f"relay_daily_sent:{channel}:{local_date.isoformat()}"


def reserve_daily_slot(channel: str, now: datetime, settings) -> bool:
    """Atomically reserve one of today's per-channel send slots (PR #179
    review finding #2). True = go ahead and dispatch; False = at/over the
    ceiling, OR Redis is unreachable.

    Fails CLOSED on a Redis outage rather than falling back to the old
    unsafe local-count behavior: rincr() returns 0 only when it could not
    reach Redis (a real INCR can never return 0), so that case is
    indistinguishable from "ceiling reached" here on purpose -- deferring a
    send is always safe (retried next sweep); silently exceeding the
    configured ceiling because the shared counter was unavailable is not.

    A DENIED attempt immediately undoes its own increment. Without this, a
    deferred item -- which the caller always retries on a later sweep, i.e.
    never actually uses this reservation -- would permanently ratchet the
    counter upward on every retry, since rincr() always increments even
    when the result turns out to be over the cap. Left unfixed, the true
    ceiling would silently shrink over the course of a day as denied
    attempts piled up.
    """
    key = _daily_slot_key(channel, now, settings)
    n = rincr(key, ttl_seconds=_DAILY_SLOT_KEY_TTL_SECONDS)
    if n == 0:
        return False
    if n > settings.relay_daily_ceiling:
        rdecr(key)
        return False
    return True


def release_daily_slot(channel: str, now: datetime, settings) -> None:
    """Give back a reservation that ultimately went unused -- the row's
    claim was lost to a concurrent worker, or the dispatch failed -- so a
    transient failure doesn't permanently eat into today's cap."""
    key = _daily_slot_key(channel, now, settings)
    rdecr(key)
