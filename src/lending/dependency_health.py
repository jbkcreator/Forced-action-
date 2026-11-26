"""R1 0.6: when each outside dependency last worked, and whether automated outbound may run on a channel.

Feeds and jobs call ``record_success`` / ``record_failure`` (own session, never raise). ``outbound_allowed`` answers
for one channel (``config/lending_fallbacks.py`` channels): paused when lending's suppression list cannot be read, the
health table cannot be read, or a timed opt-out feed protecting the channel is older than its limit. It never touches
the human emergency halt (``LENDING_GLOBAL_HALT``) and clears by itself once the feeds are fresh.

While ``LENDING_FAILCLOSED_PAUSE_ENABLED`` is off, ``outbound_allowed`` allows without reading anything; the
dependency watch (``src.lending.dependency_watch``) still evaluates the same rule and posts "would have paused".
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from sqlalchemy import text

from config.lending_fallbacks import ALL_CHANNELS, FEEDS_BY_KEY, FeedPolicy, feeds_protecting
from config.settings import get_settings

logger = logging.getLogger(__name__)

_READ = text("SELECT dependency, last_success_at, failing_since, detail FROM lending.dependency_health "
             "WHERE dependency = ANY(:deps)")
_SUPPRESSION_PROBE = text("SELECT 1 FROM lending.suppression_list LIMIT 1")
_SUCCESS = text("""
    INSERT INTO lending.dependency_health (dependency, last_success_at, failing_since, detail, updated_at)
    VALUES (:d, :now, NULL, NULL, :now)
    ON CONFLICT (dependency) DO UPDATE SET last_success_at = :now, failing_since = NULL, detail = NULL,
                                           updated_at = :now
""")
_FAILURE = text("""
    INSERT INTO lending.dependency_health (dependency, failing_since, detail, updated_at)
    VALUES (:d, :now, :detail, :now)
    ON CONFLICT (dependency) DO UPDATE
       SET failing_since = COALESCE(lending.dependency_health.failing_since, :now), detail = :detail, updated_at = :now
""")


@dataclass(frozen=True)
class HealthRow:
    dependency: str
    last_success_at: Optional[datetime]
    failing_since: Optional[datetime] = None
    detail: Optional[str] = None


@dataclass(frozen=True)
class StaleFeed:
    feed: str
    label: str
    age: Optional[timedelta]  # None = never recorded a success

    def describe(self) -> str:
        if self.age is None:
            return f"{self.label}: no success recorded"
        minutes = int(self.age.total_seconds() // 60)
        return f"{self.label}: last success {minutes} min ago"


@dataclass(frozen=True)
class Decision:
    channel: str
    allowed: bool
    would_pause: bool = False
    reason: Optional[str] = None


def _default_session():
    from src.lending.db import lending_session

    return lending_session()


# Read at call time so tests swap in a fake and never touch the shared database.
_session_factory: Callable[[], Any] = _default_session


def _utc(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


# ── Pure rule ─────────────────────────────────────────────────────────────────

def stale_feeds(channel: str, rows: Mapping[str, HealthRow], now: datetime) -> list[StaleFeed]:
    """The timed opt-out feeds protecting ``channel`` whose last success is older than their limit (or missing)."""
    stale: list[StaleFeed] = []
    for feed in feeds_protecting(channel):
        row = rows.get(feed.key)
        last = row.last_success_at if row else None
        if last is None:
            stale.append(StaleFeed(feed.key, feed.label, None))
            continue
        age = now - last
        if age > timedelta(seconds=feed.stale_after_s or 0):
            stale.append(StaleFeed(feed.key, feed.label, age))
    return stale


def unverified_reason(channel: str, rows: Optional[Mapping[str, HealthRow]], now: datetime, *,
                      suppression_readable: bool) -> Optional[str]:
    """Why suppression cannot be verified for ``channel``, or None when it can."""
    if channel not in ALL_CHANNELS:
        raise ValueError(f"unknown outbound channel {channel!r}")
    if not suppression_readable:
        return "the suppression list could not be read"
    if rows is None:
        return "the dependency health table could not be read"
    stale = stale_feeds(channel, rows, now)
    if stale:
        return "opt-outs can't be verified (" + "; ".join(s.describe() for s in stale) + ")"
    return None


def decide(channel: str, reason: Optional[str], *, enforce: bool) -> Decision:
    if reason is None:
        return Decision(channel, allowed=True)
    if enforce:
        return Decision(channel, allowed=False, would_pause=True, reason=reason)
    return Decision(channel, allowed=True, would_pause=True, reason=reason)


# ── Database reads and writes ─────────────────────────────────────────────────

def read_rows(db, dependencies) -> dict[str, HealthRow]:
    return {r.dependency: HealthRow(r.dependency, r.last_success_at, r.failing_since, r.detail)
            for r in db.execute(_READ, {"deps": list(dependencies)})}


def evaluate(channel: str, now: Optional[datetime] = None) -> Optional[str]:
    """Read the feeds and the suppression list and return why ``channel`` cannot verify suppression (None = can).
    Any read error counts as unverifiable: fail closed."""
    now = _utc(now)
    feeds = [f.key for f in feeds_protecting(channel)]
    try:
        with _session_factory() as db:
            try:
                db.execute(_SUPPRESSION_PROBE).first()
            except Exception as exc:  # class only: SQL errors embed bound parameters
                logger.error("[dependency-health] suppression list unreadable (%s)", type(exc).__name__)
                return unverified_reason(channel, {}, now, suppression_readable=False)
            rows = read_rows(db, feeds)
    except Exception as exc:
        logger.error("[dependency-health] health unreadable (%s); %s treated as unverified", type(exc).__name__,
                     channel)
        return unverified_reason(channel, None, now, suppression_readable=True)
    return unverified_reason(channel, rows, now, suppression_readable=True)


def pause_enabled() -> bool:
    return bool(get_settings().lending_failclosed_pause_enabled)


def outbound_allowed(channel: str, now: Optional[datetime] = None) -> Decision:
    """May automated outbound run on ``channel`` now? Off switch: always allowed, nothing read (the dependency watch
    reports what would have paused)."""
    if not pause_enabled():
        return Decision(channel, allowed=True)
    decision = decide(channel, evaluate(channel, now), enforce=True)
    if not decision.allowed:
        logger.warning("[dependency-health] %s paused: %s", channel, decision.reason)
    return decision


def outbound_paused(channel: str) -> bool:
    """Shorthand for the gate sites that already check the emergency halt."""
    return not outbound_allowed(channel).allowed


def record_success(dependency: str, now: Optional[datetime] = None) -> None:
    """The feed or job just worked. Own session (committed on exit); never raises."""
    try:
        with _session_factory() as db:
            db.execute(_SUCCESS, {"d": dependency, "now": _utc(now)})
    except Exception as exc:
        logger.error("[dependency-health] %s success not recorded (%s)", dependency, type(exc).__name__)


def record_failure(dependency: str, detail: str, now: Optional[datetime] = None) -> None:
    """The feed or job failed (``detail`` = an error class or short code, never contact details). Never raises."""
    try:
        with _session_factory() as db:
            db.execute(_FAILURE, {"d": dependency, "now": _utc(now), "detail": detail[:200]})
    except Exception as exc:
        logger.error("[dependency-health] %s failure not recorded (%s)", dependency, type(exc).__name__)


def freshness(dependency: str, now: Optional[datetime] = None) -> Optional[timedelta]:
    """How long since ``dependency`` last worked (None = never, or unreadable)."""
    try:
        with _session_factory() as db:
            row = read_rows(db, [dependency]).get(dependency)
    except Exception as exc:
        logger.error("[dependency-health] %s freshness unreadable (%s)", dependency, type(exc).__name__)
        return None
    if row is None or row.last_success_at is None:
        return None
    return _utc(now) - row.last_success_at


def feed(key: str) -> FeedPolicy:
    return FEEDS_BY_KEY[key]
