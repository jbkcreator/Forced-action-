"""Import a Backflip active-campaign suppression CSV into email_opt_outs / sms_opt_outs.

IMPORTANT — BLOCKED ON OPEN CLARIFICATION:
    SOT.md clarification #6 (Backflip campaign suppression feed format) is
    unanswered as of 2026-09-15. This script assumes a manual CSV export
    with at least one of: email, phone. Adjust column mapping when the real
    feed format is confirmed.

    Expected CSV columns (case-insensitive):
        email         — email address (optional if phone present)
        phone         — phone number in any format (optional if email present)

    A row with neither email nor phone is skipped with a warning.

Usage:
    PYTHONPATH=. python scripts/import_backflip_suppression_csv.py \
        --file path/to/suppression.csv [--dry-run]

All writes go to email_opt_outs / sms_opt_outs with
source='backflip_campaign_csv'. These tables are the universal enforcement
mechanism; email_suppression.is_email_suppressed() and
compliance_gator.validate_outbound() both read them — so one import here
blocks all channels without any extra plumbing.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

from src.core.database import get_db_context
from src.services.email_suppression import suppress_contact
from src.services.phone_utils import normalize

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE = "backflip_campaign_csv"


def _normalise_phone(raw: str) -> str | None:
    """Return E.164 phone or None if not parseable."""
    try:
        return normalize(raw)
    except Exception:
        return None


def run(csv_path: Path, *, dry_run: bool) -> None:
    skipped = 0
    imported = 0
    errors = 0

    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        # Normalise header keys to lower-case for case-insensitive matching
        rows = [{k.lower().strip(): v.strip() for k, v in row.items()} for row in reader]

    logger.info("Read %d rows from %s", len(rows), csv_path)

    with get_db_context() as session:
        for i, row in enumerate(rows, start=2):  # start=2 to match CSV line numbers
            email = row.get("email") or None
            phone_raw = row.get("phone") or None
            phone = _normalise_phone(phone_raw) if phone_raw else None

            if not email and not phone:
                logger.warning("Row %d: no email or phone — skipped", i)
                skipped += 1
                continue

            if dry_run:
                logger.info("DRY-RUN row %d: email=%r phone=%r", i, email, phone)
                imported += 1
                continue

            try:
                suppress_contact(session, email=email, phone=phone, source=SOURCE)
                imported += 1
            except Exception as exc:
                logger.error("Row %d: suppress_contact failed: %s", i, exc)
                errors += 1

        if not dry_run:
            session.commit()

    logger.info(
        "Done. imported=%d skipped=%d errors=%d dry_run=%s",
        imported, skipped, errors, dry_run,
    )
    if errors:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, help="Path to suppression CSV")
    parser.add_argument("--dry-run", action="store_true", help="Parse and log but do not write")
    args = parser.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    run(csv_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
