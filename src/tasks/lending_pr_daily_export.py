"""Daily calling-list export to Google Drive "FA Exports/{ET day}/": what the night's PropertyRadar pull found and
how far each record got (traced, loaded into BatchDialer, in GHL).

One CSV per signal, then ``manifest.json`` last, so a folder with a manifest is complete (a re-run removes the old
manifest before replacing any CSV):

* ``maturing_loans.csv``: Verified maturity records (estimated maturity about 5.5 months out)
* ``cash_purchases.csv``: all-cash purchases by investors owning 2-99 properties
* ``investor_purchases.csv``: other purchases by the same investors

Rows are the qualifying records first bought that ET day. Property, owner and loan facts are public record; contact
details are never written: no full phone or email, only the last 4 digits of the phone loaded and the GHL contact id
(the same rule as the ledger export).

Off until LENDING_PR_EXPORT_ENABLED=true and LENDING_PR_EXPORT_FOLDER_ID is set (a Shared Drive folder shared with
the lending Drive service account). Cron runs it after the morning load, then once more with ``--retry`` (nothing
when the day's manifest is in Drive, else export and alert on failure).

    python -m src.tasks.lending_pr_daily_export [--day YYYY-MM-DD] [--dry-run] [--retry | --alert-stalled]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from sqlalchemy import text

from config.lending_calling_lists import CASH_BUYER, INVESTOR_PURCHASE, VERIFIED_MATURITY

logger = logging.getLogger(__name__)

MANIFEST = "manifest.json"
FILES: dict[str, str] = {
    VERIFIED_MATURITY: "maturing_loans.csv",
    CASH_BUYER: "cash_purchases.csv",
    INVESTOR_PURCHASE: "investor_purchases.csv",
}
COLUMNS: tuple[str, ...] = (
    "radar_id", "state", "county", "apn", "property_address", "city", "zip", "property_type",
    "owner_name", "ownership_type", "principal_name",
    "lender_name", "loan_type", "loan_amount", "loan_recorded_date", "loan_term_years", "est_maturity_date",
    "match_strength", "last_transfer_date", "last_transfer_value", "down_payment_pct",
    "fa_property_id", "fa_match_method", "traced", "has_phone", "dialer_campaign", "loaded_at", "phone_last4",
    "ghl_contact_id", "ghl_sync_status",
)

_ROWS_SQL = text("""
    SELECT r.list_key, r.radar_id, r.state, r.county, r.apn, r.property_address, r.city, r.zip, r.property_type,
           r.owner_name, r.ownership_type, r.principal_name, r.lender_name, r.loan_type, r.loan_amount,
           r.loan_recorded_date, r.loan_term_years, r.est_maturity_date, r.match_strength, r.last_transfer_date,
           r.last_transfer_value, r.down_payment_pct, r.property_id AS fa_property_id, r.match_method AS fa_match_method,
           t.radar_id IS NOT NULL AS traced, coalesce(cardinality(t.phones), 0) > 0 AS has_phone,
           d.campaign_tag AS dialer_campaign, d.loaded_at, right(d.phone, 4) AS phone_last4, d.ghl_contact_id,
           d.ghl_sync_status
      FROM lending.pr_list_records r
      LEFT JOIN LATERAL (
            SELECT t.radar_id, t.phones FROM lending.pr_list_records r2
              JOIN property_radar_traced_contacts t ON t.radar_id = r2.radar_id
             WHERE r2.list_key = r.list_key AND r2.contact_key = r.contact_key
             ORDER BY cardinality(t.phones) DESC LIMIT 1) t ON true
      LEFT JOIN LATERAL (
            SELECT d.campaign_tag, d.loaded_at, d.phone, d.ghl_contact_id, d.ghl_sync_status
              FROM lending.dialer_load_records d
             WHERE d.source_record_ref = 'pr_list:' || r.list_key || ':' || r.radar_id
             ORDER BY d.active DESC, d.id DESC LIMIT 1) d ON true
     WHERE r.list_key = ANY(:lists) AND r.qualifies AND r.first_seen_at >= :start AND r.first_seen_at < :end
     ORDER BY r.list_key, r.state, r.county, r.id
""")


def day_window(day: date) -> tuple[datetime, datetime]:
    from src.tasks.lending_pr_nightly import night_start

    return night_start(day), night_start(day + timedelta(days=1))


def collect(db, day: date) -> dict[str, list[dict]]:
    start, end = day_window(day)
    rows: dict[str, list[dict]] = {list_key: [] for list_key in FILES}
    for row in db.execute(_ROWS_SQL, {"lists": list(FILES), "start": start, "end": end}).mappings():
        rows[row["list_key"]].append({column: row[column] for column in COLUMNS})
    return rows


def to_csv(rows: Sequence[Mapping[str, Any]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
    return buffer.getvalue().encode("utf-8")


def build_files(rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, bytes]:
    return {FILES[list_key]: to_csv(rows.get(list_key, ())) for list_key in FILES}


def manifest(counts: Mapping[str, int], day: date, now: datetime) -> bytes:
    return json.dumps({"day": day.isoformat(), "generated_at": now.isoformat(), "rows": dict(counts)},
                      indent=2).encode("utf-8")


def run(db, *, day: date, folder_id: str, upload: Callable[[str, str, str, bytes], None], now: datetime,
        dry_run: bool = False, remove: Optional[Callable[[str, str, str], None]] = None) -> dict[str, int]:
    """The manifest marks a complete day: an old one is removed before any CSV is replaced and the new one is
    uploaded last, so a run that fails part-way leaves no manifest (--retry redoes the day)."""
    rows = collect(db, day)
    files = build_files(rows)
    counts = {FILES[list_key]: len(rows.get(list_key, ())) for list_key in FILES}
    if not dry_run:
        if not folder_id:
            raise ValueError("no Drive folder")
        if remove is not None:
            remove(folder_id, day.isoformat(), MANIFEST)
        for name, data in files.items():
            upload(folder_id, day.isoformat(), name, data)
        upload(folder_id, day.isoformat(), MANIFEST, manifest(counts, day, now))
    return counts


def _alert(day: date, problem: str) -> None:
    from src.lending.ops_alerts import post_ops_alert

    post_ops_alert(f":warning: Daily calling-list export to FA Exports for {day.isoformat()}: {problem}.")


def main(argv: Optional[list[str]] = None) -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.tasks.lending_pr_nightly import night_of

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Daily calling-list export to Drive (FA Exports)")
    parser.add_argument("--day", type=date.fromisoformat, help="ET day to export (default: today ET)")
    parser.add_argument("--dry-run", action="store_true", help="count rows; upload nothing")
    parser.add_argument("--retry", action="store_true", help="second run: skip if done, alert on failure")
    parser.add_argument("--alert-stalled", action="store_true",
                        help="cron fallback when --retry could not get the lock: alert unless the day is done")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    day = args.day or night_of(now)
    settings = get_settings()
    folder_id = settings.lending_pr_export_folder_id
    if not args.dry_run and not (settings.lending_pr_export_enabled and folder_id):
        logger.info("[pr-export] off (needs LENDING_PR_EXPORT_ENABLED=true and LENDING_PR_EXPORT_FOLDER_ID)")
        return 0
    if args.dry_run:
        with get_db_context() as db:
            counts = run(db, day=day, folder_id=folder_id, upload=lambda *a: None, now=now, dry_run=True)
            db.rollback()
        logger.info("[pr-export] dry run %s %s", day, json.dumps(counts))
        return 0

    from src.lending.drive_upload import DriveUploader

    uploader = DriveUploader()
    try:
        if (args.retry or args.alert_stalled) and uploader.has_file(folder_id, day.isoformat(), MANIFEST):
            logger.info("[pr-export] %s already exported", day)
            return 0
        if args.alert_stalled:
            _alert(day, "not finished by the last retry (the main run is stalled or died)")
            return 0
        with get_db_context() as db:
            counts = run(db, day=day, folder_id=folder_id, upload=uploader, now=now, remove=uploader.remove)
            db.rollback()  # read only
    except Exception as exc:  # class only: SQL and Drive errors can embed values
        logger.error("[pr-export] %s failed (%s)", day, type(exc).__name__)
        if args.retry:
            _alert(day, f"failed after every retry ({type(exc).__name__})")
            return 0  # alerted: a non-zero exit would make run.sh retry and alert again
        return 1
    logger.info("[pr-export] %s %s", day, json.dumps(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
