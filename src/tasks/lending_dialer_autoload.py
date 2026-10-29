"""Daily automatic dialer load: newest staging run -> BatchDialer, behind guardrails.

Runs from cron once a day. Every run starts with a dry run (gates only, no
Tracerfy, no dialer, rolled back); its report decides what happens next:

- ``LENDING_DIALER_AUTOLOAD_MODE=off`` (default): exit without touching anything.
- ``dry_run``: post the dry-run report and stop (launch week: a person runs the
  live load by hand after reading it).
- ``live``: push when every guardrail passes; a tripped guardrail halts the run.

Guardrails (settings ``LENDING_DIALER_AUTOLOAD_*``), checked on the dry run:
- loadable records above ``MAX_RECORDS``;
- loadable records above ``MAX_GROWTH`` x the contacts already active in the
  dialer (skipped while nothing is active yet, when ``MAX_RECORDS`` bounds it);
- a queue with no dialer campaign tag.

Tracerfy spend: the live scrub covers at most ``MAX_SCRUB_CREDITS`` numbers and never
takes the balance below ``TRACERFY_FLOOR``; the remaining unscrubbed numbers are left
out of this run and scrubbed by the next one. An unreadable balance scrubs nothing.

Instant stop without a deploy: an active ``vendor_cost_pauses`` row for
(vendor ``batchdialer``, target ``lending_dialer_autoload``), managed through the
existing admin pause endpoints. Checked at the start and again right before the push.

The summary goes to ``LENDING_DIAL_TASKS_CHANNEL``; halts, refusals, aborts and
errors also go to the FA Max EXCEPTIONS lane.

The live load refuses numbers outside the 09:00-19:15 ET call window, so the run
happens at 9am ET: cron fires at 13:05 and 14:05 UTC (EDT / EST) and only the one
that is 09:xx ET acts (``--force`` skips that check).

Usage:
    python -m src.tasks.lending_dialer_autoload [--force]
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.settings import AppSettings, get_settings
from src.core.database import get_db_context
from src.lending import dialer_port
from src.lending.compliance import Scrubber, tracerfy_scrub
from src.lending.dialer_load import LoadAborted, LoadRefused, LoadReport, run_dialer_load
from src.lending.pool_source import staged_pool_records
from src.services.relay.slack_post import post_exceptions_alert
from src.services.tracerfy_batch import get_tracerfy_balance
from src.services.vendor_cost_pause_service import get_active_pause
from src.tasks.lending_dialer_load import launch_queue_records

logger = logging.getLogger(__name__)

PAUSE_VENDOR = "batchdialer"
PAUSE_TARGET = "lending_dialer_autoload"
EXCEPTIONS_VENTURE = "fa_max_lending"
RUN_HOUR_ET = 9


class Mode(StrEnum):
    OFF = "off"
    DRY_RUN = "dry_run"
    LIVE = "live"


class Status(StrEnum):
    OFF = "off"
    PAUSED = "paused"
    DRY_RUN = "dry_run"
    HALTED = "halted"
    REFUSED = "refused"
    ABORTED = "aborted"
    ERROR = "error"
    LOADED = "loaded"

    @property
    def alerts(self) -> bool:
        return self in (Status.HALTED, Status.REFUSED, Status.ABORTED, Status.ERROR)

    @property
    def exit_code(self) -> int:
        return 1 if self.alerts else 0


@dataclass
class Outcome:
    status: Status
    report: Optional[LoadReport] = None
    reasons: list[str] = field(default_factory=list)
    scrub_deferred: int = 0


def guardrail_trips(report: LoadReport, active_contacts: int, settings: AppSettings) -> list[str]:
    """Why a live push must not run, from the dry-run report. Empty = safe to push."""
    trips = []
    if report.loadable > settings.lending_dialer_autoload_max_records:
        trips.append(f"loadable {report.loadable} > max {settings.lending_dialer_autoload_max_records}")
    ceiling = active_contacts * settings.lending_dialer_autoload_max_growth
    if active_contacts and report.loadable > ceiling:
        trips.append(f"loadable {report.loadable} > {settings.lending_dialer_autoload_max_growth}x "
                     f"active {active_contacts}")
    if report.unmapped_pools:
        trips.append(f"no dialer campaign for queue(s): {', '.join(report.unmapped_pools)}")
    return trips


def scrub_budget(balance: Optional[int], settings: AppSettings) -> int:
    """Numbers this run may scrub: the per-run cap, kept above the balance floor."""
    if balance is None:
        return 0
    return max(0, min(settings.lending_dialer_autoload_max_scrub_credits,
                      balance - settings.lending_dialer_autoload_tracerfy_floor))


def capped_scrubber(scrubber: Scrubber, budget: int) -> Scrubber:
    """Scrub the first ``budget`` numbers only; the rest come back unscrubbed and wait."""
    def scrub(phones: list[str]) -> list[dict]:
        if len(phones) > budget:
            logger.info("[dialer-autoload] scrubbing %d of %d numbers; the rest wait for the next run",
                        budget, len(phones))
        return scrubber(phones[:budget]) if budget else []
    return scrub


def _tracerfy_balance() -> Optional[int]:
    try:
        return int(get_tracerfy_balance()["balance"])
    except Exception as exc:  # unreadable balance -> scrub nothing this run
        logger.warning("[dialer-autoload] Tracerfy balance unavailable (%s); no scrub this run",
                       type(exc).__name__)
        return None


def _active_contacts(session) -> int:
    return session.execute(
        text("SELECT count(*) FROM lending.dialer_load_records WHERE active")).scalar() or 0


def _paused(session) -> bool:
    return get_active_pause(session, PAUSE_VENDOR, PAUSE_TARGET, use_cache=False) is not None


def run_autoload(
    *,
    settings: Optional[AppSettings] = None,
    session_factory: Callable = get_db_context,
    get_dialer: Callable = dialer_port.get_dialer,
    scrubber: Scrubber = tracerfy_scrub,
    read_balance: Callable[[], Optional[int]] = _tracerfy_balance,
    now: Optional[datetime] = None,
) -> Outcome:
    settings = settings or get_settings()
    try:
        mode = Mode((settings.lending_dialer_autoload_mode or "off").strip().lower())
    except ValueError:
        return Outcome(Status.REFUSED, reasons=[f"unknown LENDING_DIALER_AUTOLOAD_MODE "
                                                f"{settings.lending_dialer_autoload_mode!r}"])
    if mode is Mode.OFF:
        return Outcome(Status.OFF)
    run_id = f"dialer-autoload-{(now or datetime.now(timezone.utc)):%Y%m%dT%H%M%SZ}"

    with session_factory() as session:
        if _paused(session):
            return Outcome(Status.PAUSED, reasons=["autoload pause is active"])
        records = launch_queue_records(staged_pool_records(session))
        active = _active_contacts(session)
        try:
            preview = run_dialer_load(records, session, run_id=run_id + "-dry", dry_run=True, now=now)
        finally:
            session.rollback()
    if mode is Mode.DRY_RUN:
        return Outcome(Status.DRY_RUN, report=preview)
    trips = guardrail_trips(preview, active, settings)
    if trips:
        return Outcome(Status.HALTED, report=preview, reasons=trips)

    dialer = get_dialer()
    if dialer is None:
        return Outcome(Status.REFUSED, report=preview, reasons=["no dialer configured (BATCHDIALER_API_KEY)"])
    missing = dialer.missing_for_load()
    if missing:
        return Outcome(Status.REFUSED, report=preview,
                       reasons=[f"unconfirmed dialer endpoint(s): {', '.join(missing)}"])
    budget = scrub_budget(read_balance(), settings) if preview.needs_scrub else 0
    deferred = max(0, preview.needs_scrub - budget)

    with session_factory() as session:
        if _paused(session):
            return Outcome(Status.PAUSED, report=preview, reasons=["autoload pause set before the push"])
        try:
            report = run_dialer_load(
                records, session, run_id=run_id, dry_run=False,
                scrubber=capped_scrubber(scrubber, budget), dialer=dialer, commit=session.commit, now=now,
                max_consecutive_failures=settings.lending_dialer_autoload_max_consecutive_failures)
        except LoadRefused as exc:
            session.rollback()
            return Outcome(Status.REFUSED, report=preview, reasons=[f"{exc} (nothing pushed)"])
        except LoadAborted as exc:
            return Outcome(Status.ABORTED, report=exc.report, reasons=[str(exc)], scrub_deferred=deferred)
    return Outcome(Status.LOADED, report=report, scrub_deferred=deferred)


def _summary(outcome: Outcome) -> str:
    head = f"Lending dialer autoload: {outcome.status}"
    r = outcome.report
    if r is None:
        return head + ("" if not outcome.reasons else " - " + "; ".join(outcome.reasons))
    by_queue = ", ".join(f"{q} {n}" for q, n in sorted(r.loadable_by_pool.items())) or "none"
    lines = [head, f"Loadable {r.loadable} ({by_queue}); needs scrub {r.needs_scrub}"]
    if not r.dry_run:
        lines.append(f"Loaded {r.loaded} (created {r.created}, updated {r.updated}); "
                     f"failed {len(r.failed)}; opted out mid-run {r.suppressed_mid_run}")
    if outcome.scrub_deferred:
        lines.append(f"Scrub deferred to the next run: {outcome.scrub_deferred}")
    if outcome.reasons:
        lines.append("Reason: " + "; ".join(outcome.reasons))
    return "\n".join(lines)


def _post_summary(settings: AppSettings, message: str) -> None:
    channel = settings.lending_dial_tasks_channel
    token = settings.lending_slack_bot_token
    if not channel or token is None:
        logger.info("[dialer-autoload] Slack not configured; summary logged only")
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)
    except Exception:
        logger.exception("[dialer-autoload] Slack summary post failed")


def notify(outcome: Outcome, settings: AppSettings) -> None:
    message = _summary(outcome)
    logger.info("[dialer-autoload] %s", message.replace("\n", " | "))
    if outcome.status is Status.OFF:
        return
    _post_summary(settings, message)
    if outcome.status.alerts:
        post_exceptions_alert(venture_key=EXCEPTIONS_VENTURE, rule="lending_dialer_autoload", message=message)


def main(argv: list[str] | None = None, now: Optional[datetime] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Daily automatic lending dialer load.")
    parser.add_argument("--force", action="store_true", help="run regardless of the ET hour")
    args = parser.parse_args(argv)
    now_et = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(DEFAULT_TZ))
    if now_et.hour != RUN_HOUR_ET and not args.force:
        logger.info("[dialer-autoload] %02d:xx ET is not the run hour; skipping", now_et.hour)
        return 0
    settings = get_settings()
    try:
        outcome = run_autoload(settings=settings)
    except Exception as exc:  # any unexpected failure still reaches Slack and EXCEPTIONS
        logger.exception("[dialer-autoload] run failed")
        outcome = Outcome(Status.ERROR, reasons=[f"unexpected {type(exc).__name__}; see logs"])
    notify(outcome, settings)
    return outcome.status.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
