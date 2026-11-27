"""R1 0.6 dependency watch (cron every 5 minutes): the alerts and housekeeping around the fail-closed pause.

Each pass:

1. Opt-out feeds: for every timed feed (``config/lending_fallbacks.py``) older than its limit, one ops alert when the
   episode starts, naming the feed, its age and the channels it pauses (while ``LENDING_FAILCLOSED_PAUSE_ENABLED`` is
   off: "would have paused"), and one when it clears. An unreadable suppression list or health table is its own
   episode. Approval cards held as ``suppression_unverified`` on a channel that may send again go back for a fresh
   approval (never auto-sent).
2. LendingFlow silence: no LendingFlow delivery for ``LENDINGFLOW_SILENCE_BUSINESS_HOURS`` business hours (Mon-Fri
   9:00-17:00 ET, US federal holidays skipped) raises one alert, cleared by the next delivery. LendingFlow pushes to us, so an outage is silence.
3. Exhausted phones: every phone over the total attempt cap is marked (``src.lending.exhausted``), so the mark exists
   before its attempts age out even when a call was recorded without the after-call hook.

Usage:
    python -m src.lending.dependency_watch [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_fallbacks import (ALL_CHANNELS, BUSINESS_DAY_END_HOUR, BUSINESS_DAY_START_HOUR, BUSINESS_TZ,
                                      FEED_LENDINGFLOW_INBOUND, LENDINGFLOW_SILENCE_BUSINESS_HOURS, OPT_OUT_FEEDS)
from src.lending import dependency_health as dh

logger = logging.getLogger(__name__)

KEY_HEALTH_UNREADABLE = "health_unreadable"
KEY_LENDINGFLOW_SILENCE = "lendingflow_silence"
STALE_PREFIX = "stale:"

_ALERTS = text("SELECT alert_key, active FROM lending.dependency_alerts")
_SET_ALERT = text("""
    INSERT INTO lending.dependency_alerts (alert_key, active, since, detail, updated_at)
    VALUES (:k, :active, :now, :detail, :now)
    ON CONFLICT (alert_key) DO UPDATE SET active = EXCLUDED.active, since = EXCLUDED.since,
                                          detail = EXCLUDED.detail, updated_at = EXCLUDED.updated_at
""")
_LAST_LENDINGFLOW_LEAD = text("SELECT max(received_at) FROM lending.lendingflow_leads")


# ── Pure parts ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AlertPlan:
    starts: dict[str, str]  # key -> detail
    clears: list[str]


def plan_alerts(current: Mapping[str, str], active: Mapping[str, bool]) -> AlertPlan:
    """``current`` = every condition true now (key -> detail); ``active`` = what was alerted before. Each episode
    starts once and clears once."""
    starts = {k: v for k, v in current.items() if not active.get(k)}
    clears = sorted(k for k, on in active.items() if on and k not in current)
    return AlertPlan(starts, clears)


def feed_conditions(rows: Optional[Mapping[str, dh.HealthRow]], now: datetime, *,
                    suppression_readable: bool) -> dict[str, str]:
    """The opt-out conditions true now, keyed for alert episodes."""
    if not suppression_readable:
        return {KEY_HEALTH_UNREADABLE: "the suppression list could not be read"}
    if rows is None:
        return {KEY_HEALTH_UNREADABLE: "the dependency health table could not be read"}
    conditions: dict[str, str] = {}
    for feed in OPT_OUT_FEEDS:
        stale = dh.feed_staleness(feed, rows.get(feed.key), now) if feed.timed else None
        if stale is not None:
            conditions[STALE_PREFIX + feed.key] = stale.describe()
    return conditions


def paused_channels(rows: Optional[Mapping[str, dh.HealthRow]], now: datetime, *,
                    suppression_readable: bool) -> set[str]:
    return {c for c in ALL_CHANNELS
            if dh.unverified_reason(c, rows, now, suppression_readable=suppression_readable) is not None}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th ``weekday`` (Mon=0) of the month; n=-1 is the last one."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    """A fixed-date holiday on a Saturday is observed Friday, on a Sunday the Monday after."""
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def us_federal_holidays(year: int) -> set[date]:
    """The eleven US federal holidays as observed in ``year``."""
    return {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),     # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),     # Washington's Birthday
        _nth_weekday(year, 5, 0, -1),    # Memorial Day
        _observed(date(year, 6, 19)),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),     # Labor Day
        _nth_weekday(year, 10, 0, 2),    # Columbus Day
        _observed(date(year, 11, 11)),
        _nth_weekday(year, 11, 3, 4),    # Thanksgiving
        _observed(date(year, 12, 25)),
    }


def is_business_day(day: date) -> bool:
    return day.weekday() < 5 and day not in us_federal_holidays(day.year)


def business_seconds(start: datetime, end: datetime) -> float:
    """Seconds of Mon-Fri 9:00-17:00 ET between ``start`` and ``end``, US federal holidays skipped."""
    if end <= start:
        return 0.0
    tz = ZoneInfo(BUSINESS_TZ)
    local_start, local_end = start.astimezone(tz), end.astimezone(tz)
    total = 0.0
    day = local_start.date()
    while day <= local_end.date():
        if is_business_day(day):
            open_at = datetime(day.year, day.month, day.day, BUSINESS_DAY_START_HOUR, tzinfo=tz)
            close_at = datetime(day.year, day.month, day.day, BUSINESS_DAY_END_HOUR, tzinfo=tz)
            lo, hi = max(open_at, local_start), min(close_at, local_end)
            if hi > lo:
                total += (hi - lo).total_seconds()
        day += timedelta(days=1)
    return total


def lendingflow_silence(last_delivery: Optional[datetime], now: datetime) -> Optional[str]:
    """The alert detail when LendingFlow has been silent too long, else None. No delivery on record: no baseline."""
    if last_delivery is None:
        return None
    silent = business_seconds(last_delivery, now)
    if silent < LENDINGFLOW_SILENCE_BUSINESS_HOURS * 3600:
        return None
    return (f"no LendingFlow lead for {silent / 3600:.1f} business hours (last at "
            f"{last_delivery.astimezone(ZoneInfo(BUSINESS_TZ)):%a %H:%M} ET)")


SOURCE_LENDINGFLOW = "lendingflow"
SOURCE_PR_MATURITY = "pr_maturity"  # the PropertyRadar pool the bridge stages


def lead_source_offline_reasons(db, now: datetime, *, lendingflow_on: bool) -> dict[str, str]:
    """Why a lead source is offline right now (source -> reason), for 1.6's queue post (``SourceCount``): a source
    listed here is broken, not dry. Read-only."""
    from src.lending.pool_source import stale_message, stale_segments

    reasons: dict[str, str] = {}
    stale = stale_segments(db, now)
    if stale:
        reasons[SOURCE_PR_MATURITY] = stale_message(stale)
    if lendingflow_on:
        row = dh.read_rows(db, [FEED_LENDINGFLOW_INBOUND]).get(FEED_LENDINGFLOW_INBOUND)
        last = row.last_success_at if row else db.execute(_LAST_LENDINGFLOW_LEAD).scalar()
        silence = lendingflow_silence(last, now)
        if silence:
            reasons[SOURCE_LENDINGFLOW] = f"LendingFlow silent: {silence}"
    return reasons


def start_message(key: str, detail: str, channels: set[str], *, enforce: bool) -> str:
    if key == KEY_LENDINGFLOW_SILENCE:
        return (f":warning: LendingFlow may be down: {detail}. Check with LendingFlow; the 2 AM reconcile recovers "
                "leads that reached us but failed.")
    scope = ", ".join(sorted(channels)) or "no channel"
    if enforce:
        return f":rotating_light: Automated sends PAUSED ({scope}): {detail}. They resume on their own when it is fresh."
    return (f":warning: Opt-outs can't be verified ({detail}). Would have paused: {scope} "
            "(LENDING_FAILCLOSED_PAUSE_ENABLED is off).")


def clear_message(key: str) -> str:
    if key == KEY_LENDINGFLOW_SILENCE:
        return ":white_check_mark: LendingFlow leads are arriving again."
    return f":white_check_mark: Opt-out check is fresh again ({key.removeprefix(STALE_PREFIX)})."


# ── One pass ──────────────────────────────────────────────────────────────────

@dataclass
class WatchResult:
    started: list[str]
    cleared: list[str]
    reopened_cards: int = 0
    exhausted_marked: int = 0


def run_watch(db, now: datetime, *, post: Callable[[str], object], reopen: Callable[[str], int],
              lendingflow_on: bool, enforce: bool, dry_run: bool = False) -> WatchResult:
    """One pass on ``db`` (the caller commits). ``post`` sends an ops alert and returns whether Slack took it;
    ``reopen(channel)`` sends the cards held for an unverified opt-out check back for approval and returns how many.
    A database that cannot be reached at all is alerted by the external watchdog (R1 0.3), not here."""
    rows: Optional[dict[str, dh.HealthRow]]
    suppression_readable = True
    try:
        with db.begin_nested():
            db.execute(dh._SUPPRESSION_PROBE).first()
    except Exception as exc:  # class only
        logger.error("[dependency-watch] suppression list unreadable (%s)", type(exc).__name__)
        suppression_readable = False
    try:
        with db.begin_nested():
            rows = dh.read_rows(db, [f.key for f in OPT_OUT_FEEDS] + [FEED_LENDINGFLOW_INBOUND])
    except Exception as exc:
        logger.error("[dependency-watch] health table unreadable (%s)", type(exc).__name__)
        rows = None

    conditions = feed_conditions(rows, now, suppression_readable=suppression_readable)
    paused = paused_channels(rows, now, suppression_readable=suppression_readable)
    if lendingflow_on:
        last = rows.get(FEED_LENDINGFLOW_INBOUND).last_success_at if rows and FEED_LENDINGFLOW_INBOUND in rows else None
        if last is None:
            last = db.execute(_LAST_LENDINGFLOW_LEAD).scalar()
        silence = lendingflow_silence(last, now)
        if silence:
            conditions[KEY_LENDINGFLOW_SILENCE] = silence

    active = {r.alert_key: bool(r.active) for r in db.execute(_ALERTS)}
    plan = plan_alerts(conditions, active)
    result = WatchResult(started=sorted(plan.starts), cleared=plan.clears)
    if dry_run:
        logger.info("[dependency-watch] dry run: start=%s clear=%s paused=%s", result.started, result.cleared,
                    sorted(paused))
        return result
    # The episode changes only once ops has the post: a refused post is tried again on the next pass.
    for key, detail in plan.starts.items():
        if post(start_message(key, detail, paused, enforce=enforce)):
            db.execute(_SET_ALERT, {"k": key, "active": True, "now": now, "detail": detail[:300]})
    for key in plan.clears:
        if post(clear_message(key)):
            db.execute(_SET_ALERT, {"k": key, "active": False, "now": now, "detail": None})

    from src.lending.dispatch.circuit_breaker import CHANNELS as BREAKER_CHANNELS

    for channel in BREAKER_CHANNELS:
        if channel not in paused:
            result.reopened_cards += reopen(channel)

    from src.lending.exhausted import mark_over_cap

    try:
        with db.begin_nested():
            result.exhausted_marked = mark_over_cap(db, now)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[dependency-watch] exhausted marking failed (%s)", type(exc).__name__)
    return result


def _reopen(channel: str) -> int:
    from src.lending.dispatch.circuit_breaker import HELD_SUPPRESSION_UNVERIFIED, reopen_held

    try:
        return reopen_held(channel, HELD_SUPPRESSION_UNVERIFIED, "system:dependency_watch")
    except Exception as exc:  # class only
        logger.error("[dependency-watch] %s held cards not reopened (%s)", channel, type(exc).__name__)
        return 0


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="report what would alert; write and post nothing")
    args = parser.parse_args(argv)
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ops_alerts import post_ops_alert

    settings = get_settings()
    with lending_session() as db:
        result = run_watch(db, datetime.now(timezone.utc), post=post_ops_alert, reopen=_reopen,
                           lendingflow_on=bool(settings.lending_lendingflow_enabled),
                           enforce=dh.pause_enabled(), dry_run=args.dry_run)
        if args.dry_run:
            db.rollback()
    logger.info("[dependency-watch] started=%s cleared=%s reopened_cards=%d exhausted_marked=%d", result.started,
                result.cleared, result.reopened_cards, result.exhausted_marked)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
