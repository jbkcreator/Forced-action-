"""Channel circuit breaker and per-minute rate limiter for every lending send (R1 item 0.4).

One breaker per sending channel (GHL SMS, GHL email, Instantly). The shared senders call:

* ``admit(channel)`` before a send: raises ``ChannelPaused`` while the breaker is open (or its state cannot be read:
  fail closed), and ``RateLimited`` when the channel already sent its per-minute cap.
* ``record_success`` / ``record_failure`` after the send. Five failures in a row (API errors, refusals by the
  provider, timeouts) trip the channel: the breaker opens, every approved card on it is held for review, and Cora's
  inbox and the ops channel are told. A success resets the count.

A tripped channel stays closed to sending until a person reopens it (``reset``): no probe message ever goes to a real
borrower. Reopening also sends the cards held for the breaker back for a fresh approval. The state lives in Postgres
(``lending.channel_breakers``) because the senders run in separate processes (Cora, the call poller, the reminder
worker, lending-api).

Approval-card sends leave ``LIVE_REPLY_RESERVE`` slots of every minute free for live replies (a missed-call text-back),
so a burst of approved cards cannot crowd one out: executors wrap the send in ``card_sends()``.

CLI::

    python -m src.lending.dispatch.circuit_breaker --status
    python -m src.lending.dispatch.circuit_breaker --reset ghl_sms
    python -m src.lending.dispatch.circuit_breaker --list-held
    python -m src.lending.dispatch.circuit_breaker --release 123       # reopen one held card
"""
from __future__ import annotations

import argparse
import contextlib
import contextvars
import getpass
import json
import sys
import logging
from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

from sqlalchemy import text

from config.settings import get_settings
from src.lending.db import lending_session

logger = logging.getLogger(__name__)

CHANNEL_SMS = "ghl_sms"
CHANNEL_EMAIL = "ghl_email"
CHANNEL_INSTANTLY = "instantly"
CHANNELS: tuple[str, ...] = (CHANNEL_SMS, CHANNEL_EMAIL, CHANNEL_INSTANTLY)

# Approval-card channels (lending.pending_actions.channel) that send through each breaker channel.
CARD_CHANNELS: dict[str, tuple[str, ...]] = {
    CHANNEL_SMS: ("ghl_sms",),
    CHANNEL_EMAIL: ("ghl_email",),
    CHANNEL_INSTANTLY: ("instantly_email", "instantly_cold_email"),
}
LABELS = {CHANNEL_SMS: "GHL SMS", CHANNEL_EMAIL: "GHL email", CHANNEL_INSTANTLY: "Instantly"}
RESUME_WORDS = {"sms": CHANNEL_SMS, "ghl_sms": CHANNEL_SMS, "email": CHANNEL_EMAIL, "ghl_email": CHANNEL_EMAIL,
                "instantly": CHANNEL_INSTANTLY}

FAILURE_THRESHOLD = 5
LIVE_REPLY_RESERVE = 10  # per-minute slots approval cards leave free for live replies
HELD_BREAKER_OPEN = "breaker_open"
HELD_SUPPRESSION_UNVERIFIED = "suppression_unverified"  # R1 0.6: an opt-out feed is stale; back for approval once fresh
STATE_CLOSED, STATE_OPEN = "closed", "open"
_PRUNE_AFTER = "1 hour"


class ChannelPaused(RuntimeError):
    """The channel may not send right now (breaker open, or its state could not be read). Nothing was sent.
    ``held_reason`` is what an approval card held for it records."""

    held_reason = HELD_BREAKER_OPEN

    def __init__(self, channel: str, why: str) -> None:
        super().__init__(f"{LABELS.get(channel, channel)} sending is paused: {why}")
        self.channel = channel
        self.why = why


class SuppressionUnverified(ChannelPaused):
    """R1 0.6: an opt-out feed protecting the channel is stale (or the suppression list is unreadable), so nobody can
    be sure the recipient has not opted out. Clears by itself when the feeds are fresh; held cards then come back for
    a fresh approval (``src.lending.dependency_watch``)."""

    held_reason = HELD_SUPPRESSION_UNVERIFIED


class RateLimited(ChannelPaused):
    """The channel already sent its per-minute cap. Nothing was sent; try again after the minute rolls over."""


def per_minute_cap(channel: str) -> int:
    settings = get_settings()
    cap = {CHANNEL_SMS: settings.lending_sms_per_minute, CHANNEL_EMAIL: settings.lending_email_per_minute,
           CHANNEL_INSTANTLY: settings.lending_instantly_per_minute}[channel]
    return max(int(cap), 1)


def should_trip(consecutive_failures: int, state: str) -> bool:
    """True when this failure count opens a closed breaker."""
    return state == STATE_CLOSED and consecutive_failures >= FAILURE_THRESHOLD


_RESERVE: contextvars.ContextVar[int] = contextvars.ContextVar("lending_send_reserve", default=0)


@contextlib.contextmanager
def card_sends() -> Iterator[None]:
    """Mark the sends inside as approval-card sends: they keep ``LIVE_REPLY_RESERVE`` slots of the minute free."""
    token = _RESERVE.set(LIVE_REPLY_RESERVE)
    try:
        yield
    finally:
        _RESERVE.reset(token)


# ── SQL ────────────────────────────────────────────────────────────────────────

_STATE = text("SELECT state, tripped_reason FROM lending.channel_breakers WHERE channel = :c")
_TAKE_SLOT = text("""
    INSERT INTO lending.channel_rate_counters (channel, minute_start, sent)
    VALUES (:c, date_trunc('minute', now()), 1)
    ON CONFLICT (channel, minute_start) DO UPDATE SET sent = lending.channel_rate_counters.sent + 1
     WHERE lending.channel_rate_counters.sent < :limit
    RETURNING sent
""")
_PRUNE = text(f"DELETE FROM lending.channel_rate_counters WHERE minute_start < now() - interval '{_PRUNE_AFTER}'")
_LOCK = text("SELECT state, consecutive_failures FROM lending.channel_breakers WHERE channel = :c FOR UPDATE")
_COUNT_FAILURE = text("""
    UPDATE lending.channel_breakers
       SET consecutive_failures = consecutive_failures + 1, last_error = :err, updated_at = now(),
           state = CASE WHEN :trip THEN 'open' ELSE state END,
           tripped_at = CASE WHEN :trip THEN now() ELSE tripped_at END,
           tripped_reason = CASE WHEN :trip THEN :err ELSE tripped_reason END
     WHERE channel = :c
""")
_COUNT_SUCCESS = text("""
    UPDATE lending.channel_breakers SET consecutive_failures = 0, updated_at = now()
     WHERE channel = :c AND consecutive_failures > 0 AND state = 'closed'
""")
_EVENT = text("""
    INSERT INTO lending.channel_breaker_events (channel, event, actor, detail)
    VALUES (:c, :event, :actor, CAST(:detail AS jsonb))
""")
_HOLD_APPROVED = text("""
    UPDATE lending.pending_actions
       SET status = 'held', held_reason = :reason, error = :detail, updated_at = now()
     WHERE status = 'approved' AND channel = ANY(:channels)
    RETURNING action_id
""")
_REOPEN_HELD = text("""
    UPDATE lending.pending_actions
       SET status = 'pending', held_reason = NULL, error = NULL, card_channel = NULL, card_ts = NULL,
           decided_by = NULL, decided_at = NULL, updated_at = now(),
           expires_at = LEAST(now() + interval '72 hours', COALESCE((payload ->> 'not_after')::timestamptz, 'infinity'))
     WHERE status = 'held' AND held_reason = :reason AND channel = ANY(:channels)
    RETURNING action_id
""")
_RELEASE_ONE = text("""
    UPDATE lending.pending_actions
       SET status = 'pending', held_reason = NULL, error = NULL, card_channel = NULL, card_ts = NULL,
           decided_by = NULL, decided_at = NULL, updated_at = now(),
           expires_at = LEAST(now() + interval '72 hours', COALESCE((payload ->> 'not_after')::timestamptz, 'infinity'))
     WHERE status = 'held' AND action_id = :id
    RETURNING action_id
""")
_OVERVIEW = text("""
    SELECT channel, state, consecutive_failures, tripped_at, tripped_reason, reset_at, reset_by
      FROM lending.channel_breakers ORDER BY channel
""")
_HELD_LIST = text("""
    SELECT action_id, channel, held_reason, summary, updated_at FROM lending.pending_actions
     WHERE status = 'held' ORDER BY action_id LIMIT 200
""")


# ── Before and after a send ────────────────────────────────────────────────────

def admit(channel: str) -> None:
    """Raise ``ChannelPaused`` / ``RateLimited`` when ``channel`` may not send now; take one slot of the minute
    otherwise. An unreadable breaker counts as open: no send without knowing the channel is healthy. First, R1 0.6:
    ``SuppressionUnverified`` while the opt-out feeds protecting the channel are stale (pause switch on)."""
    from src.lending.dependency_health import outbound_allowed

    decision = outbound_allowed(channel)
    if not decision.allowed:
        raise SuppressionUnverified(channel, decision.reason or "opt-outs can't be verified")
    limit = max(per_minute_cap(channel) - _RESERVE.get(), 1)
    paused: Optional[str] = None
    limited = False
    try:
        with lending_session() as db:
            row = db.execute(_STATE, {"c": channel}).first()
            if row is None:
                paused = "breaker state missing"
            elif row.state == STATE_OPEN:
                paused = f"tripped after {FAILURE_THRESHOLD} failures in a row ({row.tripped_reason or 'unknown'}); " \
                         "a person must reopen it"
            else:
                sent = db.execute(_TAKE_SLOT, {"c": channel, "limit": limit}).scalar_one_or_none()
                if sent is None:
                    limited = True
                elif sent == 1:
                    db.execute(_PRUNE)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[channel-breaker] %s state unreadable (%s); send refused", channel, type(exc).__name__)
        paused = "breaker state could not be read"
    if paused is not None:
        raise ChannelPaused(channel, paused)
    if limited:
        raise RateLimited(channel, f"over {limit} a minute; the next minute opens more")


def recheck(channel: str) -> None:
    """Raise ``ChannelPaused`` when the breaker opened since ``admit`` (another worker recorded the fifth failure while
    this send was preparing), or ``SuppressionUnverified`` when an opt-out feed went stale meanwhile (R1 0.6). Takes no
    rate-limit slot. Called right before the final provider request."""
    from src.lending.dependency_health import outbound_allowed

    decision = outbound_allowed(channel)
    if not decision.allowed:
        raise SuppressionUnverified(channel, decision.reason or "opt-outs can't be verified")
    paused: Optional[str] = None
    try:
        with lending_session() as db:
            row = db.execute(_STATE, {"c": channel}).first()
            if row is None or row.state == STATE_OPEN:
                paused = "tripped while this send was being prepared"
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[channel-breaker] %s state unreadable on recheck (%s); send refused", channel, type(exc).__name__)
        paused = "breaker state could not be read"
    if paused is not None:
        raise ChannelPaused(channel, paused)


def record_success(channel: str) -> None:
    """A send went through: the run of failures starts over. Never raises."""
    try:
        with lending_session() as db:
            db.execute(_COUNT_SUCCESS, {"c": channel})
    except Exception as exc:
        logger.error("[channel-breaker] %s success not recorded (%s)", channel, type(exc).__name__)


def record_failure(channel: str, error: str, *, ambiguous: bool = False) -> bool:
    """A send failed (``error`` is the exception class or a short code, never contact details). Returns True when this
    failure tripped the breaker. Never raises."""
    held: list[int] = []
    tripped = False
    try:
        with lending_session() as db:
            row = db.execute(_LOCK, {"c": channel}).first()
            if row is None:
                return False
            tripped = should_trip(row.consecutive_failures + 1, row.state)
            db.execute(_COUNT_FAILURE, {"c": channel, "err": error[:100], "trip": tripped})
            if tripped:
                held = _hold_cards(db, channel)
                db.execute(_EVENT, {"c": channel, "event": "tripped", "actor": "system:breaker",
                                    "detail": json.dumps({"error": error[:100], "ambiguous": ambiguous,
                                                          "held_cards": held})})
    except Exception as exc:
        logger.error("[channel-breaker] %s failure not recorded (%s)", channel, type(exc).__name__)
        return False
    if tripped:
        logger.error("[channel-breaker] %s tripped after %s failures in a row (%s); %s approved sends held",
                     channel, FAILURE_THRESHOLD, error, len(held))
        _announce_trip(channel, error, len(held))
    return tripped


def _hold_cards(db, channel: str) -> list[int]:
    detail = f"{LABELS[channel]} paused after {FAILURE_THRESHOLD} failures in a row"
    return [int(i) for i in db.execute(_HOLD_APPROVED, {"channels": list(CARD_CHANNELS[channel]),
                                                        "reason": HELD_BREAKER_OPEN, "detail": detail}).scalars()]


def _announce_trip(channel: str, error: str, held: int) -> None:
    from src.lending.cora_inbox import add_to_inbox_in_own_session
    from src.lending.ops_alerts import post_ops_alert

    word = next(word for word, ch in RESUME_WORDS.items() if ch == channel and word != channel) \
        if channel != CHANNEL_INSTANTLY else "instantly"
    message = (f"{LABELS[channel]} sending PAUSED after {FAILURE_THRESHOLD} failures in a row (last: {error}). "
               f"{held} approved send(s) held for review. Nothing goes out on this channel until a person "
               f"reopens it: '@Cora resume {word}'.")
    add_to_inbox_in_own_session(event_type="channel_breaker_tripped",
                                dedup_key=f"{channel}:{_epoch_minute()}", summary=message,
                                payload={"channel": channel, "error": error, "held_cards": held})
    post_ops_alert(f":rotating_light: {message}")


def _epoch_minute() -> int:
    import time

    return int(time.time() // 60)


# ── Reopening ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ResetResult:
    channel: str
    was_open: bool
    reopened_cards: int


def reset(channel: str, actor: str) -> ResetResult:
    """A person reopens ``channel``. Cards held for the breaker go back to pending with a fresh card (they need a new
    approval; every send-time check runs again). Returns what was done; a channel that was not open changes nothing."""
    if channel not in CHANNELS:
        raise ValueError(f"unknown channel {channel!r}; one of {', '.join(CHANNELS)}")
    with lending_session() as db:
        was_open = db.execute(text(
            "UPDATE lending.channel_breakers SET state = 'closed', consecutive_failures = 0, reset_at = now(), "
            "reset_by = :actor, updated_at = now() WHERE channel = :c AND state = 'open' RETURNING 1"),
            {"c": channel, "actor": actor}).first() is not None
        reopened = [int(i) for i in db.execute(_REOPEN_HELD, {"channels": list(CARD_CHANNELS[channel]),
                                                              "reason": HELD_BREAKER_OPEN}).scalars()]
        if was_open or reopened:
            db.execute(_EVENT, {"c": channel, "event": "reset", "actor": actor,
                                "detail": json.dumps({"was_open": was_open, "reopened_cards": reopened})})
    if was_open or reopened:
        from src.lending.ops_alerts import post_ops_alert

        post_ops_alert(f":white_check_mark: {LABELS[channel]} sending reopened by {actor}. "
                       f"{len(reopened)} held card(s) are back for approval.")
    return ResetResult(channel, was_open, len(reopened))


def reopen_held(channel: str, reason: str, actor: str) -> int:
    """Send the cards held on ``channel`` for ``reason`` back for a fresh approval (R1 0.6: the opt-out feeds are
    fresh again). Returns how many; the breaker state is not touched."""
    with lending_session() as db:
        reopened = [int(i) for i in db.execute(_REOPEN_HELD, {"channels": list(CARD_CHANNELS[channel]),
                                                              "reason": reason}).scalars()]
        if reopened:
            db.execute(_EVENT, {"c": channel, "event": "reopened", "actor": actor,
                                "detail": json.dumps({"reason": reason, "reopened_cards": reopened})})
    return len(reopened)


def release_held(action_id: int) -> bool:
    """A person reviewed one held card: back to pending with a fresh card. Whatever held it is checked again."""
    with lending_session() as db:
        return db.execute(_RELEASE_ONE, {"id": action_id}).first() is not None


def parse_resume_word(word: str) -> Optional[str]:
    """``sms`` / ``email`` / ``instantly`` (or the channel name) -> the breaker channel, or None."""
    return RESUME_WORDS.get(word.strip().lower().replace("-", "_"))


# ── CLI ────────────────────────────────────────────────────────────────────────

def _out(line: str) -> None:
    sys.stdout.write(line + "\n")


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--status", action="store_true", help="every channel's breaker state")
    group.add_argument("--reset", metavar="CHANNEL", help="reopen a channel: " + ", ".join(CHANNELS))
    group.add_argument("--list-held", action="store_true", help="cards held for review")
    group.add_argument("--release", metavar="ACTION_ID", type=int, help="reopen one held card for a fresh approval")
    args = parser.parse_args(argv)
    if args.status:
        with lending_session() as db:
            for row in db.execute(_OVERVIEW):
                _out(f"{row.channel:10} {row.state:6} failures={row.consecutive_failures} "
                     f"tripped_at={row.tripped_at} ({row.tripped_reason or '-'}) reset_by={row.reset_by or '-'}")
        return 0
    if args.list_held:
        with lending_session() as db:
            for row in db.execute(_HELD_LIST):
                _out(f"#{row.action_id} {row.channel} [{row.held_reason}] {row.summary}")
        return 0
    if args.release:
        _out("reopened" if release_held(args.release) else "not held")
        return 0
    channel = parse_resume_word(args.reset)
    if channel is None:
        _out(f"unknown channel {args.reset!r}; one of {', '.join(CHANNELS)}")
        return 2
    result = reset(channel, f"cli:{getpass.getuser()}")
    _out(f"{channel}: {'reopened' if result.was_open else 'was not open'}; {result.reopened_cards} held card(s) back for approval")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
