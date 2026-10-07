"""Daily automatic dialer load: newest staging run -> BatchDialer, behind guardrails.

Runs from cron once a day before the call window. Every run starts with a dry
run (gates only, no Tracerfy, no dialer, rolled back); its report decides what
happens next:

- ``LENDING_DIALER_AUTOLOAD_MODE=off`` (default): exit without touching anything.
- ``dry_run``: post the dry-run report and stop (launch week: a person runs the
  live load by hand after reading it).
- ``live``: push only when every guardrail passes; a tripped guardrail halts
  the run and alerts.

Guardrails (settings ``LENDING_DIALER_AUTOLOAD_*``):
- loadable records above ``MAX_RECORDS``;
- loadable records above ``MAX_GROWTH`` x the contacts already active in the
  dialer (skipped while nothing is active yet);
- numbers needing a Tracerfy scrub above ``MAX_SCRUB_CREDITS`` (~1 credit each).

Instant stop without a deploy: an active ``vendor_cost_pauses`` row for
(vendor ``batchdialer``, target ``lending_dialer_autoload``), managed through
the existing admin pause endpoints.

Summary goes to ``LENDING_DIAL_TASKS_CHANNEL``; halts and aborts go to
``LENDING_DIALER_ALERT_CHANNEL`` (falls back to the dial tasks channel).

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
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.settings import get_settings
from src.core.database import get_db_context
from src.lending import dialer_port
from src.lending.compliance import tracerfy_scrub
from src.lending.dialer_load import LoadAborted, LoadRefused, LoadReport, run_dialer_load
from src.lending.pool_source import staged_pool_records
from src.tasks.lending_dialer_load import launch_queue_records

logger = logging.getLogger(__name__)

PAUSE_VENDOR = "batchdialer"
PAUSE_TARGET = "lending_dialer_autoload"
MODES = ("off", "dry_run", "live")
RUN_HOUR_ET = 9


@dataclass
class Outcome:
    status: str  # off | paused | dry_run | halted | refused | aborted | loaded
    report: Optional[LoadReport] = None
    reasons: list[str] = field(default_factory=list)


def guardrail_trips(report: LoadReport, active_contacts: int, settings: Any) -> list[str]:
    """Why a live push must not run, from the dry-run report. Empty = safe to push."""
    trips = []
    if report.loadable > settings.lending_dialer_autoload_max_records:
        trips.append(f"loadable {report.loadable} > max {settings.lending_dialer_autoload_max_records}")
    ceiling = active_contacts * settings.lending_dialer_autoload_max_growth
    if active_contacts and report.loadable > ceiling:
        trips.append(f"loadable {report.loadable} > {settings.lending_dialer_autoload_max_growth}x "
                     f"active {active_contacts}")
    if report.needs_scrub > settings.lending_dialer_autoload_max_scrub_credits:
        trips.append(f"needs scrub {report.needs_scrub} > credit cap "
                     f"{settings.lending_dialer_autoload_max_scrub_credits}")
    if report.unmapped_pools:
        trips.append(f"no dialer campaign for queue(s): {', '.join(report.unmapped_pools)}")
    return trips


def _active_contacts(session) -> int:
    return session.execute(
        text("SELECT count(*) FROM lending.dialer_load_records WHERE active")).scalar() or 0


def _paused(session) -> bool:
    from src.services.vendor_cost_pause_service import get_active_pause
    return get_active_pause(session, PAUSE_VENDOR, PAUSE_TARGET, use_cache=False) is not None


def _summary(outcome: Outcome) -> str:
    r = outcome.report
    head = f"Lending dialer autoload: {outcome.status}"
    if r is None:
        return head + ("" if not outcome.reasons else " - " + "; ".join(outcome.reasons))
    by_queue = ", ".join(f"{q} {n}" for q, n in sorted(r.loadable_by_pool.items())) or "none"
    lines = [head, f"Loadable {r.loadable} ({by_queue}); needs scrub {r.needs_scrub}"]
    if not r.dry_run:
        lines.append(f"Loaded {r.loaded} (created {r.created}, updated {r.updated}); "
                     f"failed {len(r.failed)}; opted out mid-run {r.suppressed_mid_run}")
    if outcome.reasons:
        lines.append("Reason: " + "; ".join(outcome.reasons))
    return "\n".join(lines)


def _post(channel: str, message: str) -> None:
    if not channel:
        logger.info("[dialer-autoload] no Slack channel set; message: %s", message)
        return
    try:
        from src.lending.disposition_delivery import _slack_client
        _slack_client().chat_postMessage(channel=channel, text=message)
    except Exception:  # a Slack failure must never hide the run's outcome in the logs
        logger.exception("[dialer-autoload] Slack post failed; message: %s", message)


def notify(outcome: Outcome, settings: Any) -> None:
    message = _summary(outcome)
    logger.info("[dialer-autoload] %s", message.replace("\n", " | "))
    if outcome.status == "off":
        return
    alert = outcome.status in ("halted", "refused", "aborted") or bool(outcome.report and outcome.report.failed)
    channel = (settings.lending_dialer_alert_channel or settings.lending_dial_tasks_channel) if alert \
        else settings.lending_dial_tasks_channel
    _post(channel, message)


def run_autoload(
    *,
    settings: Any = None,
    session_factory: Callable = get_db_context,
    get_dialer: Callable = dialer_port.get_dialer,
    scrubber: Callable = tracerfy_scrub,
    now: Optional[datetime] = None,
) -> Outcome:
    settings = settings or get_settings()
    mode = (settings.lending_dialer_autoload_mode or "off").strip().lower()
    if mode not in MODES:
        return Outcome("refused", reasons=[f"unknown LENDING_DIALER_AUTOLOAD_MODE {mode!r}"])
    if mode == "off":
        return Outcome("off")
    run_id = f"dialer-autoload-{(now or datetime.now(timezone.utc)):%Y%m%dT%H%M%SZ}"

    with session_factory() as session:
        if _paused(session):
            return Outcome("paused", reasons=["autoload pause is active"])
        records = launch_queue_records(staged_pool_records(session))
        active = _active_contacts(session)
        try:
            preview = run_dialer_load(records, session, run_id=run_id + "-dry", dry_run=True, now=now)
        finally:
            session.rollback()
    if mode == "dry_run":
        return Outcome("dry_run", report=preview)
    trips = guardrail_trips(preview, active, settings)
    if trips:
        return Outcome("halted", report=preview, reasons=trips)

    dialer = get_dialer()
    if dialer is None:
        return Outcome("refused", report=preview, reasons=["no dialer configured (BATCHDIALER_API_KEY)"])
    missing = dialer.missing_for_load() if hasattr(dialer, "missing_for_load") else []
    if missing:
        return Outcome("refused", report=preview, reasons=[f"unconfirmed dialer endpoint(s): {', '.join(missing)}"])

    with session_factory() as session:
        try:
            report = run_dialer_load(
                records, session, run_id=run_id, dry_run=False, scrubber=scrubber, dialer=dialer,
                commit=session.commit, now=now,
                max_consecutive_failures=settings.lending_dialer_autoload_max_consecutive_failures)
        except LoadRefused as exc:
            session.rollback()
            return Outcome("refused", report=preview, reasons=[str(exc)])
        except LoadAborted as exc:
            return Outcome("aborted", report=preview, reasons=[str(exc)])
    return Outcome("loaded", report=report)


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
    outcome = run_autoload(settings=settings)
    notify(outcome, settings)
    return {"off": 0, "paused": 0, "dry_run": 0, "loaded": 0}.get(outcome.status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
