"""Pure decision core of the external watchdog: observations in, alerts and new state out. No DB, no network."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional, Sequence
from zoneinfo import ZoneInfo

from config.lending_reply_agent import REPLY_HOURS_END, REPLY_HOURS_START, REPLY_WEEKDAYS

logger = logging.getLogger(__name__)

BUSINESS_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Observation:
    key: str
    ok: bool
    detail: str


@dataclass
class CheckState:
    failures: int = 0
    open_since: Optional[datetime] = None
    last_alert_at: Optional[datetime] = None


@dataclass(frozen=True)
class Alert:
    key: str
    kind: Literal["down", "still_down", "recovered"]
    detail: str
    since: datetime


@dataclass(frozen=True)
class WatchdogConfig:
    units: tuple[str, ...]
    health_url: str
    public_base_url: str
    fail_polls: int
    repeat_minutes: int
    ghl_window_minutes: int
    ghl_min_calls: int
    card_age_minutes: int
    reply_backlog: int
    open_failures: int


def is_business_time(now: datetime) -> bool:
    local = now.astimezone(BUSINESS_TIMEZONE)
    if local.weekday() not in REPLY_WEEKDAYS:
        return False
    return REPLY_HOURS_START <= (local.hour, local.minute) < REPLY_HOURS_END


def evaluate(
    observations: Sequence[Observation],
    state: dict[str, CheckState],
    now: datetime,
    *,
    fail_polls: int,
    repeat_minutes: int,
) -> tuple[list[Alert], dict[str, CheckState]]:
    new_state = {key: replace(value) for key, value in state.items()}
    alerts: list[Alert] = []
    repeat = timedelta(minutes=repeat_minutes)
    for obs in observations:
        current = new_state.setdefault(obs.key, CheckState())
        if obs.ok:
            if current.open_since is not None:
                alerts.append(Alert(obs.key, "recovered", obs.detail, current.open_since))
            new_state[obs.key] = CheckState()
            continue
        current.failures += 1
        if current.open_since is None:
            if current.failures >= fail_polls:
                alerts.append(Alert(obs.key, "down", obs.detail, now))
                current.open_since = current.last_alert_at = now
        elif current.last_alert_at is None or now - current.last_alert_at >= repeat:
            alerts.append(Alert(obs.key, "still_down", obs.detail, current.open_since))
            current.last_alert_at = now
    return alerts, new_state


def _encode(state: CheckState) -> dict:
    return {
        "failures": state.failures,
        "open_since": state.open_since.isoformat() if state.open_since else None,
        "last_alert_at": state.last_alert_at.isoformat() if state.last_alert_at else None,
    }


def _decode(raw: dict) -> CheckState:
    failures = raw["failures"]
    if not isinstance(failures, int) or isinstance(failures, bool):
        raise ValueError("failures must be an integer")
    return CheckState(
        failures=failures,
        open_since=datetime.fromisoformat(raw["open_since"]) if raw["open_since"] else None,
        last_alert_at=datetime.fromisoformat(raw["last_alert_at"]) if raw["last_alert_at"] else None,
    )


def load_state(path: Path) -> dict[str, CheckState]:
    try:
        raw = json.loads(Path(path).read_text())
        return {str(key): _decode(value) for key, value in raw.items()}
    except FileNotFoundError:
        logger.warning("watchdog state file missing, starting fresh")
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        logger.warning("watchdog state unreadable, starting fresh: %s", type(exc).__name__)
    return {}


def save_state(path: Path, state: dict[str, CheckState]) -> None:
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({key: _encode(value) for key, value in state.items()}))
    os.replace(tmp, path)
