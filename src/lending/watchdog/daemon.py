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
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator, Optional

from sqlalchemy import text

from config.settings import AppSettings, get_settings
from src.lending.db import lending_session
from src.lending.watchdog.collectors import (
    db_observations,
    door_observations,
    health_observation,
    unit_observations,
)
from src.lending.watchdog.notify import deliver
from src.lending.watchdog.rules import (
    Alert,
    WatchdogConfig,
    evaluate,
    load_state,
    save_state,
)
from src.services.phone_utils import normalize
from src.services.telnyx_sms import send_message

logger = logging.getLogger(__name__)

STATEMENT_TIMEOUT_MS = 5000


def load_config(settings: AppSettings) -> WatchdogConfig:
    units = tuple(u.strip() for u in settings.lending_watchdog_units.split(",") if u.strip())
    return WatchdogConfig(
        units=units,
        health_url=settings.lending_watchdog_health_url,
        public_base_url=settings.lending_watchdog_public_base_url,
        fail_polls=settings.lending_watchdog_fail_polls,
        repeat_minutes=settings.lending_watchdog_repeat_minutes,
        ghl_window_minutes=settings.lending_watchdog_ghl_window_minutes,
        ghl_min_calls=settings.lending_watchdog_ghl_min_calls,
        card_age_minutes=settings.lending_watchdog_card_age_minutes,
        reply_backlog=settings.lending_watchdog_reply_backlog,
        open_failures=settings.lending_watchdog_open_failures,
    )


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


@contextmanager
def _timed_session() -> Generator[Any, None, None]:
    """lending_session with a short statement timeout, so a hung Postgres fails the check instead of the watchdog."""
    with lending_session() as db:
        db.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"))
        yield db


def _persist(path: Path, state: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        save_state(path, state)
    except OSError as exc:
        logger.error("[watchdog] state not saved (%s)", type(exc).__name__)


def run_cycle(*, now: Optional[datetime] = None, dry_run: bool = False) -> list[Alert]:
    settings = get_settings()
    if not settings.lending_watchdog_enabled:
        return []
    now = now or datetime.now(timezone.utc)
    cfg = load_config(settings)
    observations = []
    if cfg.units:
        observations.extend(unit_observations(cfg.units))
    observations.append(health_observation(cfg.health_url))
    observations.extend(door_observations(cfg.public_base_url))
    observations.extend(db_observations(_timed_session, now, cfg))

    path = Path(settings.lending_watchdog_state_path)
    alerts, new_state = evaluate(
        observations, load_state(path), now,
        fail_polls=cfg.fail_polls, repeat_minutes=cfg.repeat_minutes,
    )
    failing = sum(1 for o in observations if not o.ok)
    logger.info("[watchdog] %d checks, %d failing, %d alerts", len(observations), failing, len(alerts))
    if dry_run:
        for alert in alerts:
            logger.info("[watchdog] dry run, would send %s %s: %s", alert.kind, alert.key, alert.detail)
        return alerts
    deliver(
        alerts,
        recipients=recipients(settings),
        ghl=_ghl_route(),
        telnyx=telnyx_route(settings),
        slack=_slack(settings),
        channel=settings.lending_ops_channel,
    )
    _persist(path, new_state)
    return alerts


_running = True


def _stop(signum, _frame) -> None:
    global _running
    logger.info("[watchdog] signal %s received, stopping after this cycle", signum)
    _running = False


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="NDL external watchdog")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="one cycle; send and save nothing")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    settings = get_settings()
    logger.info("[watchdog] starting (enabled=%s)", settings.lending_watchdog_enabled)
    while _running:
        try:
            run_cycle(dry_run=args.dry_run)
        except Exception as exc:  # class only: details can carry numbers
            logger.error("[watchdog] cycle failed (%s); retrying next interval", type(exc).__name__)
        if args.once or args.dry_run:
            break
        time.sleep(settings.lending_watchdog_poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
