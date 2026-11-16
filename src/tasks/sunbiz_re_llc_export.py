"""P3.2: the daily CSV of newly formed Florida real estate LLCs, to Google Drive for agent outreach.

Each morning: download yesterday's daily corporate filings file from the state's public SFTP server, keep the active
Florida LLCs whose name holds REI / Homes / Investments / Capital (``src/scrappers/sunbiz/daily_filings.py``), and
upload ``sunbiz_re_llcs_{day}.csv`` into the day's subfolder of LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID through the lending Drive
service account (``src/lending/drive_upload.py``; the folder must be shared with it). A re-run replaces the file.

No state file for the day (none on a day without filings, or not posted yet): nothing uploaded, exit 0; the
``--retry`` run also posts an ops notice so a late or missing file is not missed.
A failure (SFTP, parsing, Drive) is logged and exits 1 so run.sh retries. The later ``--retry`` cron run does nothing
when the day's file is already in Drive, else runs again and, if that fails too, posts an ops notice (one alert, not
one per retry). Off until SUNBIZ_RE_LLC_EXPORT_ENABLED=true with the folder id and the SFTP login set.

    python -m src.tasks.sunbiz_re_llc_export [--day YYYY-MM-DD] [--dry-run] [--retry]
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional, Sequence
from zoneinfo import ZoneInfo

from config.sunbiz_re_llc import (
    EXPORT_FILE,
    OFFICER_SLOTS,
    RECORD_LENGTH,
    SFTP_HOST_KEY,
)
from src.scrappers.sunbiz.daily_filings import (
    DailyFileSource,
    Filing,
    parse_file,
    real_estate_llcs,
)

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
# run.sh keeps only lines matching " - (WARNING|ERROR|CRITICAL) - " in the cron log
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
Uploader = Callable[[str, str, str, bytes], None]  # (parent folder id, dated subfolder, file name, bytes)

_BASE_COLUMNS = (
    "document_number", "entity_name", "matched_keyword", "filing_type", "status", "file_date",
    "address_1", "address_2", "city", "state", "zip", "country",
    "mail_address_1", "mail_address_2", "mail_city", "mail_state", "mail_zip", "mail_country",
    "agent_name", "agent_type", "agent_address", "agent_city", "agent_state", "agent_zip",
)
_OFFICER_COLUMNS = ("title", "type", "name", "address", "city", "state", "zip")
COLUMNS = _BASE_COLUMNS + tuple(f"officer_{n}_{c}" for n in range(1, OFFICER_SLOTS + 1) for c in _OFFICER_COLUMNS)


@dataclass
class ExportResult:
    day: date
    state_file: bool = False
    filings: int = 0
    rejected: int = 0
    matched: int = 0
    uploaded: Optional[str] = None


def export_day(now: datetime) -> date:
    """Yesterday in Eastern time: the run is in the morning, after the state has posted the day's file."""
    return now.astimezone(ET).date() - timedelta(days=1)


def _row(filing: Filing, keyword: str) -> list[str]:
    row = [filing.document_number, filing.name, keyword, filing.filing_type, filing.status,
           filing.file_date.isoformat() if filing.file_date else "",
           filing.address_1, filing.address_2, filing.city, filing.state, filing.zip, filing.country,
           filing.mail_address_1, filing.mail_address_2, filing.mail_city, filing.mail_state, filing.mail_zip,
           filing.mail_country, filing.agent_name, filing.agent_type, filing.agent_address, filing.agent_city,
           filing.agent_state, filing.agent_zip]
    for slot in range(OFFICER_SLOTS):
        officer = filing.officers[slot] if slot < len(filing.officers) else None
        row += [getattr(officer, c) if officer else "" for c in _OFFICER_COLUMNS]
    return row


def build_csv(matches: Sequence[tuple[Filing, str]]) -> bytes:
    """Header always present; one row per matched LLC, in the state file's order. The FEI (tax) number is never
    exported."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(COLUMNS)
    for filing, keyword in matches:
        writer.writerow(_row(filing, keyword))
    return out.getvalue().encode("utf-8")


def run(day: date, *, source: DailyFileSource, folder_id: str, upload: Optional[Uploader],
        dry_run: bool = False) -> ExportResult:
    """Download, filter and (unless ``dry_run``) upload one day. Raises on an SFTP, parsing or Drive failure."""
    result = ExportResult(day=day)
    data = source.fetch(day)
    if data is None:
        logger.info("[sunbiz-re-llc] no state file for %s (no filings that day): nothing exported", day)
        return result
    result.state_file = True
    filings, result.rejected = parse_file(data)
    result.filings = len(filings)
    if result.rejected:
        logger.warning("[sunbiz-re-llc] %s: %d line(s) not %d characters, skipped", day, result.rejected, RECORD_LENGTH)
    if filings == [] and result.rejected:
        raise ValueError("no record in the state file could be parsed (layout changed?)")
    matches = list(real_estate_llcs(filings))
    result.matched = len(matches)
    if dry_run:
        return result
    if not folder_id or upload is None:
        raise ValueError("no Drive folder")
    name = EXPORT_FILE.format(day=day.isoformat())
    upload(folder_id, day.isoformat(), name, build_csv(matches))
    result.uploaded = name
    return result


def _notify(message: str) -> None:
    from src.lending.ops_alerts import post_ops_notice

    post_ops_notice(message)


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.lending.drive_upload import DriveUploader
    from src.scrappers.sunbiz.daily_filings import SunbizSftp

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    parser = argparse.ArgumentParser(description="Daily Florida real estate LLC export to Drive (P3.2)")
    parser.add_argument("--day", type=date.fromisoformat, help="state file date (default: yesterday ET)")
    parser.add_argument("--dry-run", action="store_true", help="download and count; upload nothing")
    parser.add_argument("--retry", action="store_true", help="second run: skip if done, post an ops notice on failure")
    args = parser.parse_args(argv)

    day = args.day or export_day(datetime.now(timezone.utc))
    s = get_settings()
    if not args.dry_run and not (s.sunbiz_re_llc_export_enabled and s.lending_sunbiz_llc_export_folder_id):
        logger.info("[sunbiz-re-llc] off (needs SUNBIZ_RE_LLC_EXPORT_ENABLED=true and LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID)")
        return 0
    if not (s.sunbiz_sftp_username and s.sunbiz_sftp_password):
        logger.error("[sunbiz-re-llc] SUNBIZ_SFTP_USERNAME / SUNBIZ_SFTP_PASSWORD not set: %s not exported", day)
        if args.retry:
            _notify(f":warning: Florida real estate LLC file for {day.isoformat()} not exported: "
                    "SUNBIZ_SFTP_USERNAME / SUNBIZ_SFTP_PASSWORD not set.")
            return 0  # alerted: a non-zero exit would make run.sh retry and alert again
        return 1
    source = SunbizSftp(s.sunbiz_sftp_host, s.sunbiz_sftp_username, s.sunbiz_sftp_password.get_secret_value(),
                        s.sunbiz_sftp_host_key or SFTP_HOST_KEY)
    uploader = None if args.dry_run else DriveUploader()
    try:
        if args.retry and uploader is not None and uploader.has_file(
                s.lending_sunbiz_llc_export_folder_id, day.isoformat(), EXPORT_FILE.format(day=day.isoformat())):
            logger.info("[sunbiz-re-llc] %s already exported", day)
            return 0
        result = run(day, source=source, folder_id=s.lending_sunbiz_llc_export_folder_id, upload=uploader,
                     dry_run=args.dry_run)
    except Exception as exc:  # class only: SFTP / Drive errors can embed hosts and ids
        logger.error("[sunbiz-re-llc] %s failed (%s)", day, type(exc).__name__)
        if args.retry:
            _notify(f":warning: Florida real estate LLC file for {day.isoformat()} not exported after the retry "
                    f"({type(exc).__name__}).")
            return 0  # alerted: a non-zero exit would make run.sh retry and alert again
        return 1
    if args.retry and not args.dry_run and not result.state_file:
        logger.warning("[sunbiz-re-llc] still no state file for %s at the retry run", day)
        _notify(f":information_source: No Florida filings file for {day.isoformat()} on the state server yet "
                f"(no filings that day, or posted late). If it appears later: --day {day.isoformat()}.")
    logger.info("[sunbiz-re-llc] %s%s state_file=%s filings=%d rejected=%d matched=%d uploaded=%s",
                "dry run: " if args.dry_run else "", day, result.state_file, result.filings, result.rejected,
                result.matched, result.uploaded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
