"""
Relay execution-time guards (RELAY-v2.2 sub-task R3).

evaluate() is the single gate every approved item passes immediately before
dispatch. It answers one question -- may this specific item be sent right
now? -- and is deliberately the ONLY place Relay makes that decision, so a
future channel inherits all three guards by existing rather than by
remembering to call three separate checks.

Check order is cheapest-and-most-global first: send window (pure arithmetic)
-> daily ceiling (dict lookup) -> suppression (one DB query, only reached if
the first two pass). `now` and `sent_today` are injected rather than looked
up inside, so callers (and tests) control the clock and the counter
directly -- no mocking, no freezegun, no DB in the pure-function tests.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.compliance_gator import validate_outbound
from src.services.email_suppression import is_email_suppressed
from src.services.relay.config import (
    GUARD_ALLOW,
    GUARD_BLOCK,
    GUARD_DEFER,
    REASON_DAILY_CEILING_REACHED,
    REASON_OUTSIDE_SEND_WINDOW,
    REASON_SUPPRESSED,
)
from src.services.relay.queue import QueueItem

ALLOW = GUARD_ALLOW
DEFER = GUARD_DEFER
BLOCK = GUARD_BLOCK


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


def evaluate(item: QueueItem, *, now: datetime, sent_today: dict[str, int]) -> Verdict:
    settings = get_settings()

    if not _within_send_window(now, settings):
        return Verdict(DEFER, REASON_OUTSIDE_SEND_WINDOW)

    if sent_today.get(item.channel, 0) >= settings.relay_daily_ceiling:
        return Verdict(DEFER, f"{REASON_DAILY_CEILING_REACHED}:{item.channel}")

    cause = _suppression_reason(item)
    if cause is not None:
        return Verdict(BLOCK, f"{REASON_SUPPRESSED}:{cause}")

    return Verdict(ALLOW)
