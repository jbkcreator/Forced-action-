"""Watchdog collectors: each turns one probe (systemctl, HTTP) into Observations. No secrets or PII in details."""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from typing import Any, Callable, ContextManager, Optional, Sequence

import requests
from sqlalchemy import bindparam, text

from config.lending_reply_agent import REPLY_SLA_BUSINESS_MINUTES
from config.lending_watchdog import DOORS, Door
from src.lending.watchdog.rules import Observation, WatchdogConfig, is_business_time
from src.tasks.lending_reply_sla import ANSWERABLE_KINDS, business_minutes

logger = logging.getLogger(__name__)

_warned_doors_off = False


_SYSTEM_BIN_DIRS = ("/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin")


def _resolve(program: str) -> str:
    """Absolute path of ``program``. The unit's PATH may hold only the virtualenv, so the system dirs are tried too."""
    return shutil.which(program) or shutil.which(program, path=os.pathsep.join(_SYSTEM_BIN_DIRS)) or program


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([_resolve(cmd[0]), *cmd[1:]], capture_output=True, text=True, timeout=10)


def unit_observations(
    units: Sequence[str], run: Callable[[list[str]], subprocess.CompletedProcess] = _run
) -> list[Observation]:
    try:
        lines = run(["systemctl", "is-active", *units]).stdout.splitlines()
    except Exception as exc:
        logger.warning("[watchdog] systemctl failed (%s)", type(exc).__name__)
        return [Observation(f"unit:{u}", False, f"systemctl unavailable: {type(exc).__name__}") for u in units]
    states = [lines[i].strip() if i < len(lines) else "unknown" for i in range(len(units))]
    return [Observation(f"unit:{u}", s == "active", s) for u, s in zip(units, states)]


def health_observation(url: str, get: Callable[..., requests.Response] = requests.get) -> Observation:
    key = "health:lending-api"
    try:
        resp = get(url, timeout=5)
        if resp.status_code != 200:
            return Observation(key, False, f"HTTP {resp.status_code}")
        if resp.json().get("status") != "ok":
            return Observation(key, False, "health body not ok")
        return Observation(key, True, "ok")
    except Exception as exc:
        return Observation(key, False, type(exc).__name__)


def door_observations(
    base_url: str,
    doors: Sequence[Door] = DOORS,
    request: Callable[..., requests.Response] = requests.request,
) -> list[Observation]:
    global _warned_doors_off
    if not base_url:
        if not _warned_doors_off:
            _warned_doors_off = True
            logger.warning("LENDING_WATCHDOG_PUBLIC_BASE_URL unset; door checks off")
        return []
    base = base_url.rstrip("/")
    observations = []
    for door in doors:
        key = f"door:{door.name}"
        try:
            resp = request(door.method, base + door.path, json={}, timeout=10, allow_redirects=False)
            ok = resp.status_code in door.healthy
            observations.append(Observation(key, ok, "ok" if ok else f"HTTP {resp.status_code}"))
        except Exception as exc:
            observations.append(Observation(key, False, type(exc).__name__))
    return observations


_GHL_LOOKBACK = timedelta(days=4)  # covers a Friday evening to Monday morning gap
_REPLY_LOOKBACK = timedelta(days=7)

_GHL_EVENTS = text("""
    SELECT 'call' AS kind, call_started_at AS at FROM lending.call_dispositions
     WHERE call_source = 'ghl' AND direction = 'outbound' AND call_started_at > :since
    UNION ALL
    SELECT 'outcome', received_at FROM lending.ghl_disposition_events WHERE received_at > :since
""")
_PENDING_CARDS = text("SELECT min(created_at), count(*) FROM lending.pending_actions WHERE status = 'pending'")
_UNANSWERED_HANDOFFS = text("""
    SELECT posted_at FROM lending.reply_handoffs
     WHERE posted_at IS NOT NULL AND responded_at IS NULL AND kind IN :kinds AND posted_at > :since
""").bindparams(bindparam("kinds", expanding=True))
_FAILURE_COUNTS = text("""
    SELECT count(*) FILTER (WHERE resolved_at IS NULL AND gave_up_at IS NULL),
           count(*) FILTER (WHERE gave_up_at > :since)
      FROM lending.webhook_failures
""")


def _ghl_decision(
    calls: Sequence[datetime], outcomes: Sequence[datetime], now: datetime, cfg: WatchdogConfig
) -> Optional[Observation]:
    """An outcome in the window proves the webhook works; enough calls with none means it is broken. Fewer calls
    and no outcome proves nothing either way, so it is not observed and an open outage stays open."""
    in_window = lambda moment: business_minutes(moment, now) <= cfg.ghl_window_minutes  # noqa: E731
    n_calls = sum(1 for moment in calls if in_window(moment))
    n_outcomes = sum(1 for moment in outcomes if in_window(moment))
    if n_outcomes:
        return Observation("ghl_outcomes", True, f"{n_calls} GHL calls, {n_outcomes} outcomes")
    if n_calls < cfg.ghl_min_calls:
        return None
    return Observation("ghl_outcomes", False, f"{n_calls} GHL calls, 0 outcomes in {cfg.ghl_window_minutes} business min")


def _card_decision(oldest: Optional[datetime], count: int, now: datetime, cfg: WatchdogConfig) -> Observation:
    if oldest is None:
        return Observation("cards", True, f"{count} waiting")
    age = business_minutes(oldest, now)
    return Observation("cards", age <= cfg.card_age_minutes, f"{count} waiting, oldest {age:.0f} business min")


def _reply_decision(posted: Sequence[datetime], now: datetime, cfg: WatchdogConfig) -> Observation:
    overdue = sum(1 for moment in posted if business_minutes(moment, now) >= REPLY_SLA_BUSINESS_MINUTES)
    return Observation("replies", overdue < cfg.reply_backlog, f"{overdue} handoffs past SLA")


def _failure_decision(open_count: int, gave_up: int, cfg: WatchdogConfig) -> Observation:
    ok = open_count < cfg.open_failures and gave_up == 0
    return Observation("failures", ok, f"{open_count} open, {gave_up} gave up in 24 h")


def ghl_outcome_observation(db, now: datetime, cfg: WatchdogConfig) -> Optional[Observation]:
    if cfg.ghl_min_calls == 0:
        return None
    rows = db.execute(_GHL_EVENTS, {"since": now - _GHL_LOOKBACK}).all()
    calls = [at for kind, at in rows if kind == "call"]
    outcomes = [at for kind, at in rows if kind == "outcome"]
    return _ghl_decision(calls, outcomes, now, cfg)


def card_observation(db, now: datetime, cfg: WatchdogConfig) -> Observation:
    oldest, count = db.execute(_PENDING_CARDS).one()
    return _card_decision(oldest, count, now, cfg)


def reply_observation(db, now: datetime, cfg: WatchdogConfig) -> Observation:
    rows = db.execute(_UNANSWERED_HANDOFFS, {"kinds": list(ANSWERABLE_KINDS), "since": now - _REPLY_LOOKBACK}).all()
    return _reply_decision([posted for (posted,) in rows], now, cfg)


def failure_observation(db, now: datetime, cfg: WatchdogConfig) -> Observation:
    open_count, gave_up = db.execute(_FAILURE_COUNTS, {"since": now - timedelta(hours=24)}).one()
    return _failure_decision(open_count, gave_up, cfg)


DB_CHECKS: tuple[tuple[str, Callable[[Any, datetime, WatchdogConfig], Optional[Observation]]], ...] = (
    ("ghl_outcomes", ghl_outcome_observation),
    ("cards", card_observation),
    ("replies", reply_observation),
    ("failures", failure_observation),
)


def db_observations(
    session_factory: Callable[[], ContextManager], now: datetime, cfg: WatchdogConfig
) -> list[Observation]:
    try:
        with session_factory() as db:
            db.execute(text("SELECT 1"))
            observations = [Observation("database", True, "ok")]
            if is_business_time(now):
                for key, check in DB_CHECKS:
                    try:
                        with db.begin_nested():
                            result = check(db, now, cfg)
                    except Exception as exc:
                        logger.warning("[watchdog] %s check failed (%s)", key, type(exc).__name__)
                        observations.append(Observation(key, False, type(exc).__name__))
                        continue
                    if result is not None:
                        observations.append(result)
            return observations
    except Exception as exc:
        logger.warning("[watchdog] database checks failed (%s)", type(exc).__name__)
        return [Observation("database", False, type(exc).__name__)]
