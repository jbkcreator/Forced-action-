"""Nightly ledger export to Google Drive (Cora brief P1.2): one CSV per table plus manifest.json into
``NDL Ledger Exports/{ET date}/``, by 2:00 AM ET. See src/lending/ledger_export.py for what each file holds.

Off until LENDING_LEDGER_EXPORT_ENABLED=true and LENDING_LEDGER_EXPORT_FOLDER_ID is set. Cron runs it at 05:00 UTC
(run.sh retries) and once more with ``--retry`` at 05:55: that run does nothing when the day's manifest is already in
Drive, else exports and posts to #cora if it fails.

    python -m src.tasks.lending_ledger_export [--day YYYY-MM-DD] [--dry-run] [--retry]
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional, Sequence

from src.lending import ledger_export as le

logger = logging.getLogger(__name__)

MANIFEST = "manifest.json"


def manifest(counts: dict[str, int], day: date, now: datetime) -> bytes:
    return json.dumps({"day": day.isoformat(), "generated_at": now.isoformat(), "rows": counts},
                      indent=2).encode("utf-8")


def run(db, *, day: date, folder_id: str, upload: le.Uploader, now: datetime, dry_run: bool = False) -> dict:
    data = le.collect(db, day)
    files = le.build_files(data)
    counts = {f"{name}.csv": len(getattr(data, name)) for name in le.TABLES}
    if not dry_run:
        files[MANIFEST] = manifest(counts, day, now)  # last, so its presence means the day is complete
        le.publish(files, day=day, folder_id=folder_id, upload=upload)
    return counts


def _alert(day: date, error: str) -> None:
    from config.settings import get_settings
    from src.lending.slack_channels import CORA_BOT_TOKEN, report_unset

    settings = get_settings()
    token, channel = settings.lending_cora_slack_bot_token, settings.lending_cora_channel
    if not token or not channel:
        report_unset("ledger export failure", ("LENDING_CORA_CHANNEL", CORA_BOT_TOKEN))
        return
    try:
        from slack_sdk import WebClient

        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel, text=f":warning: Nightly ledger export for {day.isoformat()} failed after every retry ({error}). "
                                  "The agents have no outcome data for that day.")
    except Exception as exc:  # the failure is already logged; a refused alert must not hide it
        logger.error("[ledger-export] alert not posted (%s)", type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.drive_upload import DriveUploader

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Nightly ledger export to Drive (Cora P1.2)")
    parser.add_argument("--day", type=date.fromisoformat, help="ET day to export (default: yesterday ET)")
    parser.add_argument("--dry-run", action="store_true", help="count rows; upload nothing")
    parser.add_argument("--retry", action="store_true", help="second run: skip if done, alert #cora on failure")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    day = args.day or le.export_day(now)
    settings = get_settings()
    folder_id = settings.lending_ledger_export_folder_id
    if not args.dry_run and not (settings.lending_ledger_export_enabled and folder_id):
        logger.info("[ledger-export] off (needs LENDING_LEDGER_EXPORT_ENABLED=true and LENDING_LEDGER_EXPORT_FOLDER_ID)")
        return 0
    uploader = DriveUploader()
    try:
        if args.retry and not args.dry_run and uploader.has_file(folder_id, day.isoformat(), MANIFEST):
            logger.info("[ledger-export] %s already exported", day)
            return 0
        with lending_session() as db:
            counts = run(db, day=day, folder_id=folder_id, upload=uploader, now=now, dry_run=args.dry_run)
    except Exception as exc:  # class only: SQL and Drive errors can embed values
        logger.error("[ledger-export] %s failed (%s)", day, type(exc).__name__)
        if args.retry:
            _alert(day, type(exc).__name__)
            return 0  # alerted: a non-zero exit would make run.sh retry and alert again
        return 1
    logger.info("[ledger-export] %s%s %s", "dry run: " if args.dry_run else "", day,
                " ".join(f"{k}={v}" for k, v in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
