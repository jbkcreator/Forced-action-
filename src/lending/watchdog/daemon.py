"""R1 0.3 external watchdog: an independent process that texts and posts when the lending system looks down.

    python -m src.lending.watchdog.daemon             # loop (systemd ndl-watchdog)
    python -m src.lending.watchdog.daemon --once      # one cycle and exit
    python -m src.lending.watchdog.daemon --dry-run   # one cycle, log what would be sent, send and save nothing

Off until LENDING_WATCHDOG_ENABLED=true. Runbook: docs/lending/watchdog.md.
"""
from __future__ import annotations

import argparse
import logging
import signal
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import AppSettings, get_settings
from config.lending_watchdog import DOORS, Door
from src.lending.watchdog.collectors import (
    db_observations,
    door_observations,
    health_observation,
    unit_observations,
)
from src.lending.watchdog.notify import deliver
from src.lending.watchdog.rules import (
    Alert,
    CheckState,
    Observation,
    WatchdogConfig,
    evaluate,
    load_state,
    save_state,
)
from src.services.phone_utils import normalize
from src.services.telnyx_sms import send_message

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MS = 5000
# A connection whose traffic is silently dropped is killed by the kernel in about 25 s instead of ~15 min.
TCP_KEEPALIVE_ARGS = {"keepalives": 1, "keepalives_idle": 10, "keepalives_interval": 5, "keepalives_count": 3,
                      "tcp_user_timeout": 15000}
DB_PROBE_DEADLINE_SECONDS = 30


def load_config(settings: AppSettings) -> WatchdogConfig:
    units = tuple(u.strip() for u in settings.lending_watchdog_units.split(",") if u.strip())
    return WatchdogConfig(
        units=units,
        health_url=settings.lending_watchdog_health_url,
        public_base_url=settings.lending_watchdog_public_base_url,
        fail_polls=max(1, settings.lending_watchdog_fail_polls),
        repeat_minutes=max(10, settings.lending_watchdog_repeat_minutes),
        ghl_window_minutes=max(1, settings.lending_watchdog_ghl_window_minutes),
        ghl_min_calls=settings.lending_watchdog_ghl_min_calls,
        card_age_minutes=max(1, settings.lending_watchdog_card_age_minutes),
        reply_backlog=max(1, settings.lending_watchdog_reply_backlog),
        open_failures=max(1, settings.lending_watchdog_open_failures),
    )


def poll_seconds(settings: AppSettings) -> int:
    return max(30, settings.lending_watchdog_poll_seconds)


_warned_doors: set[str] = set()


def selected_doors(settings: AppSettings) -> tuple[Door, ...]:
    names = [n.strip() for n in settings.lending_watchdog_doors.split(",") if n.strip()]
    known = {door.name: door for door in DOORS}
    unknown = [n for n in names if n not in known and n not in _warned_doors]
    if unknown:
        _warned_doors.update(unknown)
        logger.warning("[watchdog] LENDING_WATCHDOG_DOORS has unknown door names, ignored: %s", ", ".join(unknown))
    return tuple(known[n] for n in dict.fromkeys(names) if n in known)


def recipients(settings: AppSettings) -> list[str]:
    numbers = (normalize(settings.lending_alarm_sms_to), normalize(settings.lending_watchdog_backup_sms_to))
    return list(dict.fromkeys(n for n in numbers if n))


def telnyx_route(settings: AppSettings) -> Optional[Callable[[str, str], dict]]:
    """The alerts-only number straight through Telnyx, so a GHL outage cannot silence the watchdog. Deliberately
    never reads TELNYX_SMS_ENABLED: that switch gates subscriber texting, not internal ops alerts."""
    from_number = settings.lending_watchdog_telnyx_from_number
    profile_id = settings.lending_watchdog_telnyx_messaging_profile_id or settings.telnyx_messaging_profile_id
    if not (from_number and settings.telnyx_sms_api_key and profile_id):
        return None
    return lambda to, body: send_message(
        to, body, "transactional", from_number=from_number, messaging_profile_id=profile_id
    )


def _ghl_route() -> Optional[Callable[[str, str], str]]:
    from src.lending.ghl_sms import get_sender

    sender = get_sender(internal_alert=True)
    if sender is None:
        return None
    return lambda to, body: sender(to, body, None)


def _slack(settings: AppSettings) -> Any:
    if not settings.lending_slack_bot_token:
        return None
    from packages.agent_core.config import parse_user_ids
    from src.lending.disposition_delivery import _slack_client
    from src.lending.uncalled_alarm_worker import MentioningSlack

    return MentioningSlack(_slack_client(), parse_user_ids(settings.lending_cora_approver_user_ids))


_session_factory: Optional[sessionmaker] = None


def _factory() -> sessionmaker:
    global _session_factory
    if _session_factory is None:
        engine = create_engine(
            str(get_settings().database_url),
            pool_pre_ping=True,
            connect_args={"connect_timeout": CONNECT_TIMEOUT_SECONDS, "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
                          **TCP_KEEPALIVE_ARGS},
        )
        _session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    return _session_factory


@contextmanager
def _timed_session() -> Generator[Any, None, None]:
    """The watchdog's own connection with connect and statement timeouts, so a hung Postgres fails the check
    quickly instead of blocking the alerts already collected. Commits on exit, rolls back on error."""
    session = _factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _route(name: str, build: Callable[[], Any]) -> Any:
    """A route that cannot be built is skipped, so one bad setting never silences the other routes."""
    try:
        return build()
    except Exception as exc:
        logger.error("[watchdog] %s route unavailable (%s)", name, type(exc).__name__)
        return None


_memory_state: Optional[dict[str, CheckState]] = None  # set only while the last save failed; wins over the file


def setup_problems(settings: AppSettings) -> list[str]:
    problems = []
    if not recipients(settings):
        problems.append("no SMS recipient (LENDING_ALARM_SMS_TO)")
    if _route("GHL", _ghl_route) is None:
        problems.append("no GHL SMS route (LENDING_GHL_* settings)")
    if _route("Telnyx", lambda: telnyx_route(settings)) is None:
        problems.append("no Telnyx fallback route (LENDING_WATCHDOG_TELNYX_FROM_NUMBER, TELNYX_SMS_API_KEY, TELNYX_MESSAGING_PROFILE_ID)")
    if not (settings.lending_slack_bot_token and settings.lending_ops_channel):
        problems.append("no Slack route (LENDING_SLACK_BOT_TOKEN, LENDING_OPS_CHANNEL)")
    if not settings.lending_watchdog_public_base_url:
        problems.append("door checks off (LENDING_WATCHDOG_PUBLIC_BASE_URL)")
    return problems


def log_setup_problems(settings: AppSettings) -> None:
    problems = setup_problems(settings)
    if problems:
        logger.error("[watchdog] setup incomplete: %s", "; ".join(problems))


def _persist(path: Path, state: dict[str, CheckState]) -> None:
    global _memory_state
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        save_state(path, state)
        _memory_state = None
    except OSError as exc:
        _memory_state = state
        logger.error("[watchdog] state not saved (%s)", type(exc).__name__)


def _hold_undelivered(
    alerts: list[Alert], old: dict[str, CheckState], new: dict[str, CheckState]
) -> dict[str, CheckState]:
    """No channel got the message, so keep the failure count but forget that we alerted: the next poll alerts again."""
    held = dict(new)
    for alert in alerts:
        if alert.kind == "down":
            held[alert.key] = replace(new[alert.key], open_since=None, last_alert_at=None)
        elif alert.key in old:
            held[alert.key] = old[alert.key]
    return held


def _db_observations_with_deadline(now: datetime, cfg: WatchdogConfig) -> list[Observation]:
    """The statement timeout runs on the server, so a stalled connection can block here far longer. The probe runs
    in a daemon thread and is abandoned at the deadline: this cycle's other alerts still go out, and the next poll
    starts on time."""
    result: list[list[Observation]] = []
    worker = threading.Thread(
        target=lambda: result.append(db_observations(_timed_session, now, cfg)), name="watchdog-db-probe", daemon=True
    )
    worker.start()
    worker.join(DB_PROBE_DEADLINE_SECONDS)
    if result:
        return result[0]
    detail = "Timeout" if worker.is_alive() else "ProbeError"
    logger.error("[watchdog] database probe gave no answer within %ss (%s)", DB_PROBE_DEADLINE_SECONDS, detail)
    return [Observation("database", False, detail)]


def run_cycle(*, now: Optional[datetime] = None, dry_run: bool = False) -> list[Alert]:
    settings = get_settings()
    if not settings.lending_watchdog_enabled and not dry_run:
        return []
    now = now or datetime.now(timezone.utc)
    cfg = load_config(settings)
    observations = []
    if cfg.units:
        observations.extend(unit_observations(cfg.units))
    health = health_observation(cfg.health_url)
    observations.append(health)
    if health.ok:
        observations.extend(door_observations(cfg.public_base_url, selected_doors(settings)))
    observations.extend(_db_observations_with_deadline(now, cfg))

    path = Path(settings.lending_watchdog_state_path)
    old_state = _memory_state if _memory_state is not None else load_state(path)
    alerts, new_state = evaluate(
        observations, old_state, now,
        fail_polls=cfg.fail_polls, repeat_minutes=cfg.repeat_minutes,
    )
    failing = sum(1 for o in observations if not o.ok)
    logger.info("[watchdog] %d checks, %d failing, %d alerts", len(observations), failing, len(alerts))
    if dry_run:
        for alert in alerts:
            logger.info("[watchdog] dry run, would send %s %s: %s", alert.kind, alert.key, alert.detail)
        return alerts
    delivered = deliver(
        alerts,
        recipients=recipients(settings),
        ghl=_route("GHL", _ghl_route),
        telnyx=_route("Telnyx", lambda: telnyx_route(settings)),
        slack=_route("Slack", lambda: _slack(settings)),
        channel=settings.lending_ops_channel,
    )
    if not delivered:
        logger.error("[watchdog] %d alerts not delivered by any channel; will retry next poll", len(alerts))
        new_state = _hold_undelivered(alerts, old_state, new_state)
    _persist(path, new_state)
    return alerts


_stop_event = threading.Event()


def _on_signal(signum, _frame) -> None:
    logger.info("[watchdog] signal %s received, stopping", signum)
    _stop_event.set()


def _wait(seconds: float) -> None:
    _stop_event.wait(seconds)


def main(argv: Optional[list[str]] = None) -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="NDL external watchdog")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="one cycle; send and save nothing (works while disabled)")
    args = parser.parse_args(argv)
    _stop_event.clear()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    settings = get_settings()
    logger.info("[watchdog] starting (enabled=%s)", settings.lending_watchdog_enabled)
    log_setup_problems(settings)
    while not _stop_event.is_set():
        try:
            run_cycle(dry_run=args.dry_run)
        except Exception as exc:  # class only: details can carry numbers
            logger.error("[watchdog] cycle failed (%s); retrying next interval", type(exc).__name__)
        if args.once or args.dry_run:
            break
        _wait(poll_seconds(settings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
