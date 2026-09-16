"""Replace the active Backflip campaign-contact snapshot from a CSV export.

Expected columns: email and/or phone (case insensitive). An empty snapshot
requires --allow-empty so a malformed export cannot silently clear protection.
The exact feed mapping remains pending Bailey's response. This importer never
writes permanent opt-out tables.
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.phone_utils import normalize

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def _identifiers(csv_path: Path) -> set[tuple[str, str]]:
    identifiers: set[tuple[str, str]] = set()
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        headers = {name.strip().lower() for name in (reader.fieldnames or [])}
        if not headers.intersection({"email", "phone"}):
            raise ValueError("CSV needs an email or phone column")
        for line_no, raw in enumerate(reader, start=2):
            row = {(key or "").strip().lower(): (value or "").strip()
                   for key, value in raw.items()}
            if row.get("email"):
                identifiers.add(("email", row["email"].lower()))
            if row.get("phone"):
                phone = normalize(row["phone"])
                if not phone:
                    raise ValueError(f"invalid phone on CSV line {line_no}")
                identifiers.add(("phone", phone))
    return identifiers


def run(csv_path: Path, *, dry_run: bool = False, allow_empty: bool = False) -> int:
    identifiers = _identifiers(csv_path)
    if not identifiers and not allow_empty:
        raise ValueError("empty campaign snapshot; pass --allow-empty after verifying the export")
    if dry_run:
        logger.info("DRY RUN: %d identifiers parsed; database unchanged", len(identifiers))
        return len(identifiers)

    # Do not remove rows written by the old importer from permanent opt-outs:
    # an address may also have a genuine unsubscribe or DNC reason.
    with get_db_context() as session:
        session.execute(text("UPDATE fa_max_backflip_campaign_contacts SET active = false WHERE active"))
        for kind, value in identifiers:
            session.execute(
                text("INSERT INTO fa_max_backflip_campaign_contacts "
                     "(identifier_kind, identifier_value, active, imported_at) "
                     "VALUES (:kind, :value, true, now()) "
                     "ON CONFLICT (identifier_kind, identifier_value) DO UPDATE SET "
                     "active = true, imported_at = now()"),
                {"kind": kind, "value": value},
            )
        session.execute(
            text("INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) "
                 "VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET last_success_at = now()")
        )
    logger.info("Backflip campaign snapshot replaced: %d identifiers", len(identifiers))
    return len(identifiers)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, help="Path to suppression CSV")
    parser.add_argument("--dry-run", action="store_true", help="Parse and log but do not write")
    parser.add_argument("--allow-empty", action="store_true", help="Permit a verified empty campaign snapshot")
    args = parser.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    run(csv_path, dry_run=args.dry_run, allow_empty=args.allow_empty)


if __name__ == "__main__":
    main()
