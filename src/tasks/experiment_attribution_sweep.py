"""
LEARN-v2.2 L2 — nightly experiment attribution sweep.

Attributes reply.received fleet events to their Agent Lane experiment arm
via the draft-match / last-touch credit model in
src/services/experiment_attribution.py.

Usage:
    python -m src.tasks.experiment_attribution_sweep [--dry-run]

Options:
    --dry-run   Report what would be attributed without writing any rows.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

try:
    from slack_sdk import WebClient as _SlackWebClient
except ImportError:
    _SlackWebClient = None  # type: ignore[assignment,misc]

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.experiment_attribution import AttributionReport, run_attribution
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def _post_slack_digest(report: AttributionReport, dry_run: bool) -> None:
    settings = get_settings()
    bot_token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "lifecycle_incident_slack_channel", None)

    if not bot_token or not channel or _SlackWebClient is None:
        logger.info(
            "[ExperimentAttributionSweep] Slack not configured — skipping digest post."
        )
        return

    prefix = "[DRY-RUN] " if dry_run else ""
    text = (
        f"{prefix}:bar_chart: *Experiment Attribution Sweep* complete\n"
        f"• Scanned: {report.events_scanned}\n"
        f"• Attributed: {report.attributed}\n"
        f"• Already attributed: {report.already_attributed}\n"
        f"• No assignment found: {report.no_assignment}\n"
        f"• Errors: {report.errors}"
    )
    try:
        token = bot_token.get_secret_value() if hasattr(bot_token, "get_secret_value") else bot_token
        client = _SlackWebClient(token=token)
        client.chat_postMessage(channel=channel, text=text)
    except Exception as exc:
        logger.warning("[ExperimentAttributionSweep] Slack digest failed: %s", exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="LEARN-v2.2 L2 experiment attribution sweep")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing rows")
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    logger.info(
        "[ExperimentAttributionSweep] starting dry_run=%s now=%s",
        args.dry_run, now.isoformat(),
    )

    if args.dry_run:
        # In dry-run mode, run inside a savepoint and roll back so nothing persists.
        with get_db_context() as db:
            sp = db.begin_nested()
            try:
                report = run_attribution(db, now=now)
            finally:
                sp.rollback()
    else:
        with get_db_context() as db:
            report = run_attribution(db, now=now)
            db.commit()

    logger.info(
        "[ExperimentAttributionSweep] done scanned=%d attributed=%d already=%d "
        "no_assignment=%d errors=%d",
        report.events_scanned,
        report.attributed,
        report.already_attributed,
        report.no_assignment,
        report.errors,
    )

    if report.error_details:
        for detail in report.error_details:
            logger.warning("[ExperimentAttributionSweep] error: %s", detail)

    _post_slack_digest(report, dry_run=args.dry_run)

    return 1 if report.errors else 0


if __name__ == "__main__":
    sys.exit(main())
