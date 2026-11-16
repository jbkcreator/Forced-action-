"""Daily FA export (Cora brief P3.1): yesterday's (ET) PropertyRadar leads as ``fa_export_YYYY-MM-DD.csv`` in
Drive "FA Exports", and as Next Deal Lending GHL contacts tagged ``NDL``. See src/lending/fa_export.py for the rows.

Off until LENDING_FA_EXPORT_ENABLED=true and LENDING_FA_EXPORT_FOLDER_ID is set. The GHL push is a second switch,
LENDING_FA_EXPORT_GHL_ENABLED: only rows with a traced phone that is not suppressed become contacts. A file of the
same name is replaced, and GHL upserts by phone, so a re-run of a day is harmless.

    python -m src.tasks.lending_fa_export [--day YYYY-MM-DD] [--dry-run] [--backfill]
"""
from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import date, datetime, timezone
from typing import Callable, Optional, Sequence

from src.lending import fa_export as fx

logger = logging.getLogger(__name__)

Upload = Callable[[str, str, bytes], object]


def run(session, *, day: date, folder_id: str, upload: Optional[Upload], push: Optional[fx.GhlPush],
        backfill: bool = False) -> dict:
    """Build the day's rows; upload the CSV when ``upload`` is given, push contacts when ``push`` is."""
    rows = fx.collect(session, day, backfill=backfill)
    by_signal = Counter(s for row in rows for s in row["signals"].split(fx.SIGNAL_SEPARATOR))
    summary: dict = {"day": day.isoformat(), "rows": len(rows), "with_phone": sum(1 for r in rows if r["phone"]),
                     **by_signal, "by_state": dict(Counter(str(row["state"]) for row in rows))}
    if upload is not None:
        name = fx.file_name(day, backfill=backfill)
        upload(folder_id, name, fx.to_csv(rows))
        summary["uploaded"] = name
    if push is not None:
        summary["ghl"] = dict(fx.push_to_ghl(rows, push, fx.suppressed_among(session, rows)))
    return summary


def _drive_upload() -> Upload:
    from src.lending.drive_upload import drive_service, upload_bytes

    service = drive_service()
    return lambda folder_id, name, data: upload_bytes(service, folder_id, name, data)


def _ghl_push() -> Optional[fx.GhlPush]:
    from src.lending.wholesaler_lists import ghl_sink

    sink = ghl_sink()
    if sink is None:
        logger.error("[fa-export] GHL push is on but the Next Deal Lending GHL account is not configured")
        return None
    return sink.push


def main(argv: Optional[Sequence[str]] = None) -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.lending.ledger_export import export_day

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Daily FA export to Drive and GHL (Cora P3.1)")
    parser.add_argument("--day", type=date.fromisoformat, help="ET day to export (default: yesterday ET)")
    parser.add_argument("--dry-run", action="store_true", help="count rows; upload and push nothing")
    parser.add_argument("--backfill", action="store_true",
                        help="one-time starter file: every lead that qualifies as of --day (default: today ET), "
                             "as fa_export_backfill_YYYY-MM-DD.csv; never pushes to GHL")
    args = parser.parse_args(argv)

    settings = get_settings()
    folder_id = settings.lending_fa_export_folder_id
    now = datetime.now(timezone.utc)
    day = args.day or (now.astimezone(fx.ET).date() if args.backfill else export_day(now))
    if not args.dry_run and not (settings.lending_fa_export_enabled and folder_id):
        logger.info("[fa-export] off (needs LENDING_FA_EXPORT_ENABLED=true and LENDING_FA_EXPORT_FOLDER_ID)")
        return 0
    try:
        upload = None if args.dry_run else _drive_upload()
        ghl = settings.lending_fa_export_ghl_enabled and not args.dry_run and not args.backfill
        push = _ghl_push() if ghl else None
        with get_db_context() as session:
            summary = run(session, day=day, folder_id=folder_id, upload=upload, push=push, backfill=args.backfill)
    except Exception as exc:  # class only: SQL and Drive errors can embed values
        logger.error("[fa-export] %s failed (%s)", day, type(exc).__name__)
        return 1
    logger.info("[fa-export] %s%s", "dry run: " if args.dry_run else "", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
